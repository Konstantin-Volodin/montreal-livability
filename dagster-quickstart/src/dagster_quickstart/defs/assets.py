import dagster as dg
import geopandas as gpd

from dagster_quickstart.defs.resources import ArcGISFeatureServerResource, S3DataStore

@dg.asset(
    group_name="raw_data",
    metadata={
        "layer": "landing",
        "source": "txdot",
        "dat_category": "vector",
        "segmentation": "snapshots",
    }
)
def texas_trunk_system(context: dg.AssetExecutionContext, feature_server: ArcGISFeatureServerResource, s3_datastore: S3DataStore) -> dg.MaterializeResult:
    """ Fetches the TXDoT Texas Trunk System containing a network of divided highways intented to become >= 4 lanes."""
    
    # Define query
    url="https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/TxDOT_Texas_Trunk_System/FeatureServer/0/query"
    params = {
        'where': '1=1',
        'outFields': '*',
        'f': 'geojson',
        }
    
    # Fetch data
    gdf = feature_server.fetch_data(
        url, 
        params,
        context=context 
    )

    # write to s3
    s3_datastore.write_gpq(context, gdf)
    

    return gdf