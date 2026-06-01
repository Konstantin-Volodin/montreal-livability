import dagster as dg

pre_partition = dg.define_asset_job(
    name="pre_partition_job",
    selection=["*amenities", "*montreal_municipalities", "*h3_montreal_addresses"],
)

post_partition = dg.define_asset_job(
    name="post_partition_job",
    selection=["distances_to_amenities"],
)

gold = dg.define_asset_job(
    name="gold_job",
    selection=["livability_score", "livability_map"],
)