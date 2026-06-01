import dagster as dg

# everything upstream of the dynamic r6 partition: one parallel, unpartitioned run.
pre_partition = dg.define_asset_job(
    name="pre_partition_job",
    selection=["*amenities", "*montreal_municipalities", "*h3_montreal_addresses"],
)

# fan-out: the r6 partition run. 
post_partition = dg.define_asset_job(
    name="post_partition_job",
    selection=["distances_to_amenities"],
)

# fan-in: the unpartitioned aggregation + viz. 
gold = dg.define_asset_job(
    name="gold_job",
    selection=["livability_score", "livability_map"],
)