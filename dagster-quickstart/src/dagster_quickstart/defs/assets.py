import dagster as dg
import geopandas as gpd
from datetime import datetime

from dagster_quickstart.defs.resources import ArcGISFeatureServerResource, S3DataStore


# ----- PARTITION DEFINITIONS -----
# Define yearly partitions starting from 2023
yearly_partitions = dg.TimeWindowPartitionsDefinition(
    cron_schedule="@yearly",  # Run once a year
    start=datetime(2023, 1, 1),  # Start from January 1, 2023
    end_offset=1,  # Include the current year
    fmt="%Y"  # Format partition keys as YYYY
)


# ----- ASSETS -----
@dg.asset(
    group_name="raw_data",
    partitions_def=yearly_partitions,
    metadata={
        "layer": "landing", "source": "txdot", "data_category": "vector", "segmentation": "partitions", },
)
def texas_trunk_system(context: dg.AssetExecutionContext, feature_server: ArcGISFeatureServerResource, s3_datastore: S3DataStore) -> dg.MaterializeResult:
    """ Fetches the TXDoT Texas Trunk System containing a network of divided highways intented to become >= 4 lanes."""
    
    # define annual partition key based on execution date
    year = context.partition_key
    context.log.info(f"processing partition for year: {year}")

    # Define query
    url="https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/TxDOT_Texas_Trunk_System/FeatureServer/0/query"
    params = {
        'where': f'EXTRACT(YEAR FROM EXT_DATE) = {year}',
        'outFields': '*',
        'f': 'geojson',
    }
    
    # Fetch data
    gdf = feature_server.fetch_data(url,params,context=context)

    # write to s3
    s3_datastore.write_gpq(context, gdf)


@dg.asset(
    group_name='raw_data',
    metadata={"layer": "landing", "source": "txdot", "data_category": "vector", "segmentation": "full_snapshots"},
)
def texas_county_boundaries(context: dg.AssetExecutionContext, feature_server: ArcGISFeatureServerResource, s3_datastore: S3DataStore) -> dg.MaterializeResult:
    """Fetches the TXDoT polygon layer of the 254 Texas counties"""
    
    # Define query
    url = "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/Texas_County_Boundaries/FeatureServer/0/query?"
    params = {
        'where': '1=1',
        'outFields': '*',
        'f': 'geojson',
    }
    
    # Fetch data
    gdf = feature_server.fetch_data(url=url, params=params, context=context)
    
    # Write the GeoDataFrame to S3
    s3_datastore.write_gpq(context, gdf)
    
    return gdf

@dg.asset(
    group_name='raw_data',
    metadata={"layer": "landing", "source": "census_bureau", "data_category": "vector", "segmentation": "full_snapshots"},
)
def tx_med_household_income(context: dg.AssetExecutionContext, feature_server: ArcGISFeatureServerResource, s3_datastore: S3DataStore) -> dg.MaterializeResult:
    """Fetches American Community Survey (ACS) median household income by census tract in Texas."""
    
    # Define query
    url="https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/ACS_Median_Income_by_Race_and_Age_Selp_Emp_Boundaries/FeatureServer/2/query?"
    params = {
        "where": "State='Texas'",
        "outFields": "*",
        "f": "geojson",
        "returnGeometry": "true", 
    }
    
    # Fetch data
    gdf = feature_server.fetch_data(url=url, params=params, context=context)
    
    # Write the GeoDataFrame to S3
    s3_datastore.write_gpq(context, gdf)
    
    return gdf


@dg.asset(
    group_name='analytics',
    metadata={"layer": "enriched", "source": "analytics", "data_category": "vector", "segmentation": "full_snapshots"},
    deps = [texas_trunk_system, tx_med_household_income, texas_county_boundaries]
)
def trunk_median_income(context: dg.AssetExecutionContext, s3_datastore: S3DataStore) -> dg.MaterializeResult:
    """Joins Texas trunk system to median household income across all Texas counties."""

    # Fetch trunk system
    ts_key_pattern = "landing/txdot/vector/texas_trunk_system/partitions"
    trunk_system = s3_datastore.read_gpq_all_partitions(context, ts_key_pattern)
    
    # Fetch median income tracts (latest snapshot, since the write timestamp changes every run)
    med_income_tracts_pattern = "landing/census_bureau/vector/tx_med_household_income/full_snapshots"
    med_income_tracts = s3_datastore.read_gpq_latest_snapshot(context, med_income_tracts_pattern)
    
    # Fetch counties
    tx_counties_pattern = 'landing/txdot/vector/texas_county_boundaries/full_snapshots'
    tx_counties = s3_datastore.read_gpq_latest_snapshot(context, tx_counties_pattern)
    
    # Join median income tracts to trunk system
    combined_gdf = gpd.sjoin(med_income_tracts, trunk_system, how="inner", predicate="intersects")

    # Clip to the full set of Texas county boundaries (all 254 counties)
    combined_gdf = combined_gdf.clip(tx_counties)
    s3_datastore.write_gpq(context, combined_gdf)

    return combined_gdf


@dg.asset(
    group_name='analytics',
    metadata={"layer": "enriched", "source": "analytics", "data_category": "viz", "segmentation": "full_snapshots"},
    deps=[trunk_median_income],
)
def trunk_median_income_map(context: dg.AssetExecutionContext, s3_datastore: S3DataStore) -> None:
    """Interactive Plotly choropleth of tract median income with the trunk system overlaid."""
    import json
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go

    # fetch the analytics data
    combined_pattern = "enriched/analytics/vector/trunk_median_income/full_snapshots"
    combined = s3_datastore.read_gpq_latest_snapshot(context, combined_pattern)

    # fetch trunk system
    ts_key_pattern = "landing/txdot/vector/texas_trunk_system/partitions"
    trunk = s3_datastore.read_gpq_all_partitions(context, ts_key_pattern)

    if combined.empty:
        raise ValueError("trunk_median_income produced no rows — nothing to map.")

    # Plotly maps need lon/lat (EPSG:4326)
    combined = combined.set_crs(4326, allow_override=True) if combined.crs is None else combined.to_crs(4326)
    combined = combined[combined.geometry.notna() & ~combined.geometry.is_empty].reset_index(drop=True)

    # ACS overall median household income (B19049_001E)
    income_col = "B19049_001E"
    combined[income_col] = pd.to_numeric(combined[income_col], errors="coerce")
    context.log.info(f"Coloring by '{income_col}'")

    geojson = json.loads(combined[[combined.geometry.name]].to_json())
    minx, miny, maxx, maxy = combined.total_bounds

    fig = px.choropleth_map(
        combined,
        geojson=geojson,
        locations=combined.index,
        color=income_col,
        color_continuous_scale="Reds",
        map_style="carto-positron",
        center={"lat": (miny + maxy) / 2, "lon": (minx + maxx) / 2},
        zoom=7,
        opacity=0.7,
        labels={income_col: "Median household income"},
    )

    # Overlay the trunk highway lines in black
    if not trunk.empty:
        trunk = trunk.to_crs(4326)
        lats: list = []
        lons: list = []
        for geom in trunk.geometry:
            if geom is None or geom.is_empty: continue
            parts = geom.geoms if geom.geom_type == "MultiLineString" else [geom]
            for line in parts:
                x, y = line.xy
                lons += list(x) + [None]
                lats += list(y) + [None]
        fig.add_trace(go.Scattermap(
            lat=lats, lon=lons, mode="lines",
            line=dict(width=2, color="black"),
            name="Trunk system", hoverinfo="skip",
        ))

    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), autosize=True)

    # default_width/height make the saved HTML fill the browser window
    html = fig.to_html(
        include_plotlyjs="cdn",
        full_html=True,
        default_width="100%",
        default_height="100vh",
    )
    s3_datastore.write_html(context, html)

    # Log metadata about the map for observability
    context.add_output_metadata({
        "income_column": dg.MetadataValue.text(income_col),
        "tracts_plotted": dg.MetadataValue.int(len(combined)),
    })


