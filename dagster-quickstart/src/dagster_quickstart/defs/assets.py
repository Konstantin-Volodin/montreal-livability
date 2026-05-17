import dagster as dg
import requests
import geopandas as gpd

@dg.asset
def texas_trunk_system(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """ Fetches the TXDoT Texas Trunk System containing a network of divided highways intented to become >= 4 lanes."""
    
    # Define query
    url="https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/TxDOT_Texas_Trunk_System/FeatureServer/0/query"
    params = {
        'where': '1=1',
        'outFields': '*',
        'f': 'geojson',
        }
    
    # Fetch data
    response = requests.get(url, params=params)
    data = response.json()
    
    # Construct geodataframe
    gdf = gpd.GeoDataFrame.from_features(data['features'], crs="EPSG:4326")

    return gdf