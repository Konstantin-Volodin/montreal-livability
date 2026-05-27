"""Freshness checks keyed on the real S3 snapshot age.

A cache-hit materialization resets Dagster's event clock, so the built-in
last-update freshness check would read a stale snapshot as fresh. These checks
read the actual ``_latest`` snapshot timestamp instead, so they only pass when
the underlying data is genuinely recent.
"""

from datetime import datetime, timedelta, timezone

import dagster as dg

from montreal.defs.assets.raw import (
    montreal_addresses,
    montreal_bike_paths,
    montreal_municipality_boundaries,
    montreal_parks,
    montreal_pois,
    montreal_transit_stops,
)
from montreal.defs.resources.lakehouse import location_of, s3_datastore

_RAW_ASSETS = [
    montreal_addresses,
    montreal_bike_paths,
    montreal_municipality_boundaries,
    montreal_parks,
    montreal_pois,
    montreal_transit_stops,
]
_MAX_AGE = timedelta(days=35)


def _snapshot_freshness_check(asset: dg.AssetsDefinition):
    @dg.asset_check(asset=asset, name="snapshot_fresh")
    def _check(s3_datastore: s3_datastore) -> dg.AssetCheckResult:
        ts = s3_datastore.latest_timestamp(location_of(asset))
        age = None if ts is None else datetime.now(timezone.utc) - ts
        return dg.AssetCheckResult(
            passed=age is not None and age <= _MAX_AGE,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "snapshot": dg.MetadataValue.text(str(ts)),
                "age_days": dg.MetadataValue.int(age.days if age else -1),
                "max_age_days": dg.MetadataValue.int(_MAX_AGE.days),
            },
        )

    return _check


@dg.definitions
def checks() -> dg.Definitions:
    return dg.Definitions(
        asset_checks=[_snapshot_freshness_check(asset) for asset in _RAW_ASSETS],
    )
