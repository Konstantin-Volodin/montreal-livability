import dagster as dg
import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Point

from montreal.defs.assets.h3_layer import (
    _SILVER_META,
    _to_wgs84,
    h3_montreal_addresses,
    h3_montreal_bike_paths,
    h3_montreal_osm_pois_categorized,
    h3_montreal_parks,
    h3_montreal_transit_stops,
)
from montreal.defs.resources.lakehouse import s3_datastore


_AMENITY_CATEGORIES = ("grocery", "school", "health", "transit", "park", "bike")


def _haversine_metres(lat1, lng1, lat2, lng2) -> np.ndarray:
    """Vectorized great-circle distance in metres."""
    lat1_rad = np.radians(np.asarray(lat1, dtype=float))
    lng1_rad = np.radians(np.asarray(lng1, dtype=float))
    lat2_rad = np.radians(np.asarray(lat2, dtype=float))
    lng2_rad = np.radians(np.asarray(lng2, dtype=float))

    dlat = lat2_rad - lat1_rad
    dlng = lng2_rad - lng1_rad
    a = (
        np.sin(dlat / 2.0) ** 2
        + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlng / 2.0) ** 2
    )
    return 6371000.0 * 2.0 * np.arcsin(np.sqrt(a))


def _nearest_distances(addr_df, amenity_df, rings=(2, 5, 10)) -> pd.DataFrame:
    """Nearest amenity distance per address row and category using H3 ring search."""
    dist_columns = [f"dist_{category}" for category in _AMENITY_CATEGORIES]
    distances = pd.DataFrame(index=addr_df.index, columns=dist_columns, dtype=float)

    if addr_df.empty:
        return distances

    required_addr_cols = {"h3_r10", "lat", "lng"}
    required_amenity_cols = {"category", "h3_r10", "lat", "lng"}
    missing_addr = required_addr_cols - set(addr_df.columns)
    missing_amenity = required_amenity_cols - set(amenity_df.columns)
    if missing_addr:
        raise ValueError(f"addr_df missing required columns: {sorted(missing_addr)}")
    if missing_amenity:
        raise ValueError(f"amenity_df missing required columns: {sorted(missing_amenity)}")

    amenity_df = amenity_df.dropna(subset=["category", "h3_r10", "lat", "lng"])
    addr_work = addr_df.dropna(subset=["h3_r10", "lat", "lng"])
    if amenity_df.empty or addr_work.empty:
        return distances

    points_by_category = {}
    for category in _AMENITY_CATEGORIES:
        category_df = amenity_df[amenity_df["category"] == category]
        points_by_category[category] = {
            cell: group[["lat", "lng"]].to_numpy(dtype=float)
            for cell, group in category_df.groupby("h3_r10", sort=False)
        }

    cells = addr_work["h3_r10"].to_numpy()
    coords = addr_work[["lat", "lng"]].to_numpy(dtype=float)
    unique_cells = pd.unique(addr_work["h3_r10"])

    for category, points_by_cell in points_by_category.items():
        candidate_cache = {}
        for addr_cell in unique_cells:
            candidates = []
            for k in rings:
                ring_cells = h3.grid_disk(addr_cell, int(k))
                candidates = [
                    points_by_cell[cell]
                    for cell in ring_cells
                    if cell in points_by_cell
                ]
                if candidates:
                    break
            candidate_cache[addr_cell] = (
                np.vstack(candidates) if candidates else np.empty((0, 2), dtype=float)
            )

        category_distances = np.full(len(addr_work), np.nan, dtype=float)
        for addr_cell, candidates in candidate_cache.items():
            if len(candidates) == 0:
                continue

            positions = np.flatnonzero(cells == addr_cell)
            addr_coords = coords[positions]
            candidate_distances = _haversine_metres(
                addr_coords[:, [0]],
                addr_coords[:, [1]],
                candidates[None, :, 0],
                candidates[None, :, 1],
            )
            category_distances[positions] = np.nanmin(candidate_distances, axis=1)

        distances.loc[addr_work.index, f"dist_{category}"] = category_distances

    return distances


def _points_with_lat_lng(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    points = gdf.geometry.representative_point()
    return gdf.assign(
        geometry=points,
        lat=points.y.to_numpy(dtype=float),
        lng=points.x.to_numpy(dtype=float),
    )


def _amenity_frame(gdf: gpd.GeoDataFrame, category: str) -> gpd.GeoDataFrame:
    points = _points_with_lat_lng(gdf)
    points["category"] = category
    return points[["category", "h3_r7", "h3_r10", "lat", "lng", "geometry"]]


@dg.asset(
    group_name="distance_layer",
    metadata=_SILVER_META,
    deps=[
        h3_montreal_osm_pois_categorized,
        h3_montreal_transit_stops,
        h3_montreal_parks,
        h3_montreal_bike_paths,
    ],
)
def amenity_points(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Long table of amenity candidate points for nearest-distance search."""
    osm_pois = s3_datastore.read_gpq(context, "silver/h3_montreal_osm_pois_categorized.parquet")
    transit = s3_datastore.read_gpq(context, "silver/h3_montreal_transit_stops.parquet")
    parks = s3_datastore.read_gpq(context, "silver/h3_montreal_parks.parquet")
    bike_paths = s3_datastore.read_gpq(context, "silver/h3_montreal_bike_paths.parquet")

    frames = []
    for category in ("grocery", "school", "health"):
        frames.append(_amenity_frame(osm_pois[osm_pois["category"] == category].copy(), category))

    frames.append(_amenity_frame(transit, "transit"))
    frames.append(_amenity_frame(parks, "park"))

    bike_points = bike_paths[["h3_r7", "h3_r10"]].drop_duplicates().copy()
    lat_lng = bike_points["h3_r10"].map(h3.cell_to_latlng)
    bike_points["lat"] = lat_lng.map(lambda coords: coords[0]).astype(float)
    bike_points["lng"] = lat_lng.map(lambda coords: coords[1]).astype(float)
    bike_points["geometry"] = [
        Point(lng, lat) for lat, lng in zip(bike_points["lat"], bike_points["lng"])
    ]
    bike_points["category"] = "bike"
    bike_points = gpd.GeoDataFrame(
        bike_points[["category", "h3_r7", "h3_r10", "lat", "lng", "geometry"]],
        geometry="geometry",
        crs=4326,
    )
    frames.append(bike_points)

    amenities = gpd.GeoDataFrame(
        pd.concat(frames, ignore_index=True),
        geometry="geometry",
        crs=4326,
    )
    amenities = amenities.dropna(subset=["category", "h3_r7", "h3_r10", "lat", "lng"])
    context.log.info(
        f"amenity_points: {len(amenities)} rows across "
        f"{amenities['category'].nunique()} categories"
    )
    s3_datastore.write_gpq(context, amenities)
    return dg.MaterializeResult()


@dg.asset(
    group_name="distance_layer",
    metadata=_SILVER_META,
    deps=[h3_montreal_addresses, amenity_points],
)
def distances_to_amenities(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Nearest amenity distance per Montreal address and livability category."""
    addresses = _to_wgs84(s3_datastore.read_gpq(context, "silver/h3_montreal_addresses.parquet"))
    amenities = s3_datastore.read_gpq(context, "silver/amenity_points.parquet")

    address_points = _points_with_lat_lng(addresses)
    distance_df = _nearest_distances(address_points, amenities)

    out = addresses.copy()
    for column in distance_df.columns:
        out[column] = distance_df[column].to_numpy()

    context.log.info(
        f"distances_to_amenities: {len(out)} address rows with "
        f"{len(distance_df.columns)} distance columns"
    )
    s3_datastore.write_gpq(context, out)
    return dg.MaterializeResult()
