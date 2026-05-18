import dagster as dg
import geopandas as gpd
import h3
import numpy as np
import pandas as pd

from montreal.defs.assets.h3_layer import (
    _to_wgs84,
    h3_montreal_addresses,
    h3_montreal_bike_paths,
    h3_montreal_osm_pois,
    h3_montreal_parks,
    h3_montreal_transit_stops,
    r7_partitions,
)
from montreal.defs.resources.lakehouse import s3_datastore


_AMENITY_CATEGORIES = ("grocery", "school", "health", "transit", "park", "bike")
_SILVER_META = {"layer": "silver","data_category": "geospacial"}
_SILVER_PARTITIONED_META = {**_SILVER_META, "segmentation": "h3_r7"}
_EARTH_RADIUS_METRES = 6371000.0


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
    return _EARTH_RADIUS_METRES * 2.0 * np.arcsin(np.sqrt(a))


def _nearest_distances(addr_df, amenity_df, max_k=10, log=None) -> pd.DataFrame:
    """Nearest amenity distance per address row and category using H3 ring search.

    The algorithm, step by step:
      1. Drop address rows missing an H3 cell or coordinates.
      2. For each amenity category, index its points by the H3 r10 cell they fall in.
      3. For each (category, address cell), step the H3 ring distance outward
         k = 0, 1, 2, ... and stop at the first k whose ring holds amenity
         points (giving up after max_k) -- those become the candidate set for
         every address in that cell.
      4. Haversine-distance every address to its candidates and keep the minimum.
    """
    def _info(msg): 
        if log is not None: log.info(msg)

    dist_columns = [f"dist_{category}" for category in _AMENITY_CATEGORIES]
    distances = pd.DataFrame(index=addr_df.index, columns=dist_columns, dtype=float)

    # Step 1: only addresses with a cell + lat/lng can be matched.
    addr_work = addr_df.dropna(subset=["h3_r10", "lat", "lng"])
    _info(f"_nearest_distances: {len(addr_work)}/{len(addr_df)} addresses usable, {len(amenity_df)} amenity points")

    # Step 2: bucket each category's points by the H3 r10 cell they sit in.
    points_by_category = {}
    for category in _AMENITY_CATEGORIES:
        category_df = amenity_df[amenity_df["category"] == category]
        points_by_category[category] = {
            cell: group[["lat", "lng"]].to_numpy(dtype=float)
            for cell, group in category_df.groupby("h3_r10", sort=False)
        }
        _info(f"  category '{category}': {len(category_df)} points across {len(points_by_category[category])} H3 cells")

    cells = addr_work["h3_r10"].to_numpy()
    coords = addr_work[["lat", "lng"]].to_numpy(dtype=float)
    unique_cells = pd.unique(addr_work["h3_r10"])
    _info(f"  {len(unique_cells)} unique address cells to resolve per category")

    for category, points_by_cell in points_by_category.items():

        # Step 3: step the ring distance out one k at a time, stopping at the first ring that holds an amenity cell.
        candidate_cache = {}
        no_candidate_cells = 0
        for addr_cell in unique_cells:
            found = []

            # k ring search for the nearest amenity cell(s)
            for k in range(max_k + 1):
                grid_ring_cells = h3.grid_ring(addr_cell, k)
                found = [points_by_cell[cell] for cell in grid_ring_cells if cell in points_by_cell]
                if found: break

            if not found: no_candidate_cells += 1
            candidate_cache[addr_cell] = (np.vstack(found) if found else np.empty((0, 2), dtype=float))
        _info(f"  category '{category}': {no_candidate_cells} cells found no amenity within k={max_k}")

        # Step 4: vectorized haversine to candidates, keep the nearest.
        category_distances = np.full(len(addr_work), np.nan, dtype=float)
        for addr_cell, candidates in candidate_cache.items():
            if len(candidates) == 0: continue

            positions = np.flatnonzero(cells == addr_cell)
            addr_coords = coords[positions]
            candidate_distances = _haversine_metres(
                addr_coords[:, [0]],
                addr_coords[:, [1]],
                candidates[None, :, 0],
                candidates[None, :, 1],
            )
            category_distances[positions] = np.nanmin(candidate_distances, axis=1)
        resolved = int(np.count_nonzero(~np.isnan(category_distances)))

        _info(
            f"  category '{category}': {resolved}/{len(addr_work)} addresses got "
            f"a distance (median {np.nanmedian(category_distances):.0f} m)"
            if resolved else f"  category '{category}': 0 addresses got a distance"
        )
        distances.loc[addr_work.index, f"dist_{category}"] = category_distances

    return distances


# (distance_m, score) knots; linearly interpolated between, 0 past the last.


_SCORE_CURVE = ((250.0, 100.0), (750.0, 50.0), (1500.0, 20.0))
def _distance_score(distances) -> np.ndarray:
    """Piecewise-linear distance (m) -> 0-100 livability score.

    100 within 250 m, decaying linearly through the _SCORE_CURVE knots,
    then 0 beyond 1500 m or where the distance is missing.
    """
    knots_m, knot_scores = zip(*_SCORE_CURVE)
    d = np.asarray(distances, dtype=float)
    score = np.interp(d, knots_m, knot_scores)
    score[(d > knots_m[-1]) | np.isnan(d)] = 0.0
    return score


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
    return points[["category", "h3_r10", "lat", "lng", "geometry"]]


@dg.asset(
    group_name="distance_layer",
    metadata=_SILVER_META,
    deps=[
        h3_montreal_osm_pois,
        h3_montreal_transit_stops,
        h3_montreal_parks,
        h3_montreal_bike_paths,
    ],
)
def amenity_points(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """amenity candidate points for nearest-distance search."""
    # read data
    osm_pois = s3_datastore.read_gpq(context, "silver/h3_montreal_osm_pois.parquet")
    transit = s3_datastore.read_gpq(context, "silver/h3_montreal_transit_stops.parquet")
    parks = s3_datastore.read_gpq(context, "silver/h3_montreal_parks.parquet")
    bike_paths = s3_datastore.read_gpq(context, "silver/h3_montreal_bike_paths.parquet")

    # concat POIs
    frames = []
    frames.append(_amenity_frame(osm_pois[osm_pois["category"] == "grocery"].copy(), "grocery"))
    frames.append(_amenity_frame(osm_pois[osm_pois["category"] == "school"].copy(), "school"))
    frames.append(_amenity_frame(osm_pois[osm_pois["category"] == "health"].copy(), "health"))
    frames.append(_amenity_frame(transit, "transit"))
    frames.append(_amenity_frame(parks, "park"))

    # for bike paths, we use the path centroids 
    bike = bike_paths[["h3_r10"]].drop_duplicates().copy()
    latlng = np.array(bike["h3_r10"].map(h3.cell_to_latlng).tolist())
    bike = bike.set_geometry(gpd.points_from_xy(latlng[:, 1], latlng[:, 0]), crs=4326)
    frames.append(_amenity_frame(bike, "bike"))

    # results
    amenities = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry", crs=4326,)
    amenities = amenities.dropna(subset=["category", "h3_r10", "lat", "lng"])
    context.log.info(f"amenity_points: {len(amenities)} rows")
    for category in amenities["category"].unique():
        count = (amenities["category"] == category).sum()
        context.log.info(f"  {category}: {count} rows")
    
    # export
    s3_datastore.write_gpq(context, amenities)
    return dg.MaterializeResult()


@dg.asset(
    group_name="distance_layer",
    metadata=_SILVER_PARTITIONED_META,
    partitions_def=r7_partitions,
    deps=[h3_montreal_addresses, amenity_points],
)
def distances_to_amenities(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Nearest amenity distance per Montreal address and livability category."""
    addresses = _to_wgs84(s3_datastore.read_gpq(context, f"silver/h3_montreal_addresses.parquet/{context.partition_key}.parquet"))
    amenities = s3_datastore.read_gpq(context, "silver/amenity_points.parquet")

    address_points = _points_with_lat_lng(addresses)
    distance_df = _nearest_distances(address_points, amenities, log=context.log)

    out = addresses.copy()
    for column in distance_df.columns:
        out[column] = distance_df[column].to_numpy()

    context.log.info(f"distances_to_amenities: {len(out)} address rows with {len(distance_df.columns)} distance columns")
    s3_datastore.write_gpq(context, out)
    return dg.MaterializeResult()


@dg.asset(
    group_name="distance_layer",
    metadata=_SILVER_PARTITIONED_META,
    partitions_def=r7_partitions,
    deps=[distances_to_amenities],
)
def amenity_scores(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Per-category 0-100 livability score from each address's nearest-amenity distance."""
    distances = s3_datastore.read_gpq(context, f"silver/distances_to_amenities.parquet/{context.partition_key}.parquet")

    out = distances.copy()
    for category in _AMENITY_CATEGORIES:
        dist_col = f"dist_{category}"
        score_col = f"score_{category}"
        scores = _distance_score(distances[dist_col].to_numpy())
        out[score_col] = scores
        resolved = int(np.count_nonzero(~np.isnan(scores)))
        context.log.info(
            f"  category '{category}': {resolved}/{len(out)} scored "
            f"(mean {np.nanmean(scores):.1f})"
            if resolved else f"  category '{category}': 0 addresses scored"
        )

    context.log.info(f"amenity_scores: {len(out)} address rows with {len(_AMENITY_CATEGORIES)} score columns")
    s3_datastore.write_gpq(context, out)
    return dg.MaterializeResult()
