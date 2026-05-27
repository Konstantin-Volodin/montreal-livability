"""H3-index Montreal addresses, shard the output by r6 cell, and reconcile r6 partitions."""

from dataclasses import asdict

import dagster as dg
import h3

from montreal.defs.assets.bronze import montreal_addresses
from montreal.defs.assets.silver.config import (
    SilverAssetDataContract,
    SilverAssetMetadata,
    h3_index,
    r6_partitions,
)
from montreal.defs.checks.factory import (
    field_completeness_factory,
    row_uniqueness_factory,
    schema_contract_factory,
)
from montreal.defs.resources.lakehouse import location_of, s3_datastore

# metadata
ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="h3_r6",
    description="Montreal addresses with h3_r10 + h3_r6 indices, sharded by r6 cell",
)

# data contract
ASSET_DATA_CONTRACT = SilverAssetDataContract(
    schema={"ID_UEV": "str", "h3_r10": "str", "h3_r6": "str", "geometry": "geometry"},
    uniqueness=("ID_UEV",),
    completeness=("ID_UEV", "h3_r10", "h3_r6", "geometry"),
)

# asset
@dg.asset(group_name="H3_indexed", metadata=asdict(ASSET_META), deps=[montreal_addresses])
def h3_montreal_addresses(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """H3-index addresses, shard the output by r6, and reconcile r6 partitions."""
    gdf = s3_datastore.read_gpq(context, location_of(montreal_addresses))
    gdf = h3_index(gdf)
    gdf["h3_r6"] = gdf["h3_r10"].map(lambda cell: str(h3.cell_to_parent(cell, 6)))
    gdf = gdf[gdf["h3_r6"].notna()]
    context.log.info(f"montreal_addresses: {len(gdf)} rows H3-indexed (r10 + r6)")

    desired = set(gdf["h3_r6"].unique())
    existing = set(context.instance.get_dynamic_partitions(r6_partitions.name))
    context.instance.add_dynamic_partitions(r6_partitions.name, sorted(desired - existing))
    for stale in sorted(existing - desired):
        context.instance.delete_dynamic_partition(r6_partitions.name, stale)
    context.log.info(
        f"r6 partitions: {len(desired)} cells "
        f"(+{len(desired - existing)} / -{len(existing - desired)})"
    )

    stamp = s3_datastore.write_gpq_partitioned(context, gdf, "h3_r6")
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)

# asset checks
addresses_schema = schema_contract_factory(h3_montreal_addresses, ASSET_DATA_CONTRACT.schema)
addresses_uniqueness = row_uniqueness_factory(h3_montreal_addresses, ASSET_DATA_CONTRACT.uniqueness)
addresses_completeness = field_completeness_factory(h3_montreal_addresses, ASSET_DATA_CONTRACT.completeness)
