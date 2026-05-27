"""Asset checks for snapshot freshness and derived data quality."""

from typing import Iterable

import dagster as dg

from montreal.defs.assets.bronze.raw import (
    montreal_addresses,
    montreal_bike_paths,
    montreal_municipality_boundaries,
    montreal_parks,
    montreal_pois,
    montreal_transit_stops,
)
from montreal.defs.assets.gold.analytics import livability_score
from montreal.defs.assets.silver.distance import POI_CATEGORIES, amenity_points
from montreal.defs.assets.silver.h3 import (
    h3_montreal_addresses,
    h3_montreal_bike_paths,
    h3_montreal_osm_pois,
    h3_montreal_parks,
    h3_montreal_transit_stops,
    montreal_municipalities,
)
from montreal.defs.resources.lakehouse import location_of, s3_datastore
from montreal.defs.quality import (
    category_coverage_result,
    not_null_result,
    numeric_bounds_result,
    required_columns_result,
    row_count_result,
    snapshot_freshness_result,
    unique_rows_result,
)

_RAW_ASSETS = [
    montreal_addresses,
    montreal_bike_paths,
    montreal_municipality_boundaries,
    montreal_parks,
    montreal_pois,
    montreal_transit_stops,
]
_H3_REQUIRED_COLUMNS = ("geometry", "h3_r10")
_ADDRESS_COLUMNS = (*_H3_REQUIRED_COLUMNS, "h3_r6")
_OSM_POI_COLUMNS = ("geometry", "h3_r10", "name", "category")
_MUNICIPALITY_COLUMNS = ("municipality", "type", "geometry")
_MUNICIPALITY_KEY = ("municipality", "type")
_AMENITY_COLUMNS = ("category", "h3_r10", "lat", "lng", "geometry")
_AMENITY_KEY = ("category", "h3_r10", "lat", "lng")
_LIVABILITY_SCORE_COLUMNS = tuple(f"score_{category}" for category in POI_CATEGORIES)
_LIVABILITY_COLUMNS = (
    "h3_r10",
    "n_addresses",
    "municipality",
    "livability",
    *_LIVABILITY_SCORE_COLUMNS,
)
_LIVABILITY_KEY_COLUMNS = ("h3_r10",)
_LIVABILITY_NOT_NULL_COLUMNS = (
    "h3_r10",
    "n_addresses",
    "municipality",
    "livability",
)


def _snapshot_freshness_check(asset: dg.AssetsDefinition):
    @dg.asset_check(asset=asset, name="snapshot_fresh")
    def _check(s3_datastore: s3_datastore) -> dg.AssetCheckResult:
        ts = s3_datastore.latest_timestamp(location_of(asset))
        return snapshot_freshness_result(ts)

    return _check


def _required_columns_check(
    asset: dg.AssetsDefinition,
    required_columns: Iterable[str],
    *,
    check_name: str = "required_columns",
):
    @dg.asset_check(asset=asset, name=check_name)
    def _check(
        context: dg.AssetCheckExecutionContext,
        s3_datastore: s3_datastore,
    ) -> dg.AssetCheckResult:
        df = s3_datastore.read_gpq(context, location_of(asset))
        return required_columns_result(df, required_columns)

    return _check


def _row_count_check(asset: dg.AssetsDefinition, min_rows: int):
    @dg.asset_check(asset=asset, name="row_count")
    def _check(
        context: dg.AssetCheckExecutionContext,
        s3_datastore: s3_datastore,
    ) -> dg.AssetCheckResult:
        df = s3_datastore.read_gpq(context, location_of(asset))
        return row_count_result(df, min_rows)

    return _check


def _not_null_check(asset: dg.AssetsDefinition, columns: Iterable[str]):
    @dg.asset_check(asset=asset, name="not_null")
    def _check(
        context: dg.AssetCheckExecutionContext,
        s3_datastore: s3_datastore,
    ) -> dg.AssetCheckResult:
        df = s3_datastore.read_gpq(context, location_of(asset))
        return not_null_result(df, columns)

    return _check


def _unique_rows_check(
    asset: dg.AssetsDefinition,
    columns: Iterable[str],
    *,
    check_name: str = "unique_rows",
):
    @dg.asset_check(asset=asset, name=check_name)
    def _check(
        context: dg.AssetCheckExecutionContext,
        s3_datastore: s3_datastore,
    ) -> dg.AssetCheckResult:
        df = s3_datastore.read_gpq(context, location_of(asset))
        return unique_rows_result(df, columns)

    return _check


def _amenity_category_check():
    @dg.asset_check(asset=amenity_points, name="category_coverage")
    def _check(
        context: dg.AssetCheckExecutionContext,
        s3_datastore: s3_datastore,
    ) -> dg.AssetCheckResult:
        df = s3_datastore.read_gpq(context, location_of(amenity_points))
        return category_coverage_result(df, POI_CATEGORIES)

    return _check


def _distance_bounds_check():
    @dg.asset_check(asset=amenity_points, name="coordinate_bounds")
    def _check(
        context: dg.AssetCheckExecutionContext,
        s3_datastore: s3_datastore,
    ) -> dg.AssetCheckResult:
        df = s3_datastore.read_gpq(context, location_of(amenity_points))
        lon = numeric_bounds_result(
            df,
            ("lng",),
            lower=-180.0,
            upper=180.0,
            allow_nulls=False,
        )
        lat = numeric_bounds_result(
            df,
            ("lat",),
            lower=-90.0,
            upper=90.0,
            allow_nulls=False,
        )
        return dg.AssetCheckResult(
            passed=lon.passed and lat.passed,
            metadata={
                "lng_invalid_counts": lon.metadata["invalid_counts"],
                "lat_invalid_counts": lat.metadata["invalid_counts"],
                "lng_null_counts": lon.metadata["null_counts"],
                "lat_null_counts": lat.metadata["null_counts"],
            },
        )

    return _check


def _livability_score_bounds_check():
    @dg.asset_check(asset=livability_score, name="score_bounds")
    def _check(
        context: dg.AssetCheckExecutionContext,
        s3_datastore: s3_datastore,
    ) -> dg.AssetCheckResult:
        df = s3_datastore.read_gpq(context, location_of(livability_score))
        return numeric_bounds_result(
            df,
            ("livability", *_LIVABILITY_SCORE_COLUMNS),
            lower=0.0,
            upper=100.0,
            allow_nulls=False,
        )

    return _check


@dg.definitions
def checks() -> dg.Definitions:
    return dg.Definitions(
        asset_checks=[
            *[_snapshot_freshness_check(asset) for asset in _RAW_ASSETS],
            _row_count_check(h3_montreal_addresses, 1_000),
            _required_columns_check(h3_montreal_addresses, _ADDRESS_COLUMNS),
            _row_count_check(h3_montreal_parks, 10),
            _required_columns_check(h3_montreal_parks, _H3_REQUIRED_COLUMNS),
            _row_count_check(h3_montreal_transit_stops, 100),
            _required_columns_check(h3_montreal_transit_stops, _H3_REQUIRED_COLUMNS),
            _row_count_check(h3_montreal_bike_paths, 10),
            _required_columns_check(h3_montreal_bike_paths, _H3_REQUIRED_COLUMNS),
            _row_count_check(h3_montreal_osm_pois, 10),
            _required_columns_check(h3_montreal_osm_pois, _OSM_POI_COLUMNS),
            _row_count_check(montreal_municipalities, 10),
            _required_columns_check(montreal_municipalities, _MUNICIPALITY_COLUMNS),
            _not_null_check(montreal_municipalities, _MUNICIPALITY_KEY),
            _unique_rows_check(montreal_municipalities, _MUNICIPALITY_KEY),
            _row_count_check(amenity_points, 10),
            _required_columns_check(amenity_points, _AMENITY_COLUMNS),
            _not_null_check(amenity_points, _AMENITY_KEY),
            _unique_rows_check(amenity_points, _AMENITY_KEY),
            _amenity_category_check(),
            _distance_bounds_check(),
            _row_count_check(livability_score, 100),
            _required_columns_check(livability_score, _LIVABILITY_COLUMNS),
            _not_null_check(livability_score, _LIVABILITY_NOT_NULL_COLUMNS),
            _unique_rows_check(
                livability_score,
                _LIVABILITY_KEY_COLUMNS,
                check_name="unique_h3_cells",
            ),
            _livability_score_bounds_check(),
        ],
    )
