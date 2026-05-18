# Montreal Livability Score - Dagster Project

**Goal:** Dagster pipeline that computes a livability score for every address in Montreal.

**Extension:** Where to live based on your actual life" recommender.

**Output:** [Interactive report of Montreal livability score.](https://aws-dagster-example.s3.us-east-1.amazonaws.com/gold/livability_map.html)

## 1. Architecture

- *dagster* for orchestration - matches LL's primary tool. 
- *s3* for parquet storage - standard data lake pattern
- *Folium* for map visualization - Pythonic wrapper around Leaflet, easy to generate interactive HTML maps
- *Geopandas* as needed - for any geometry manipulation that DuckDB can't handle natively
- *H3* for spatial indexing - critical for efficient distance calculations at scale
- (to add) *DuckDB* for SQL querying and distance calculations - fast, single-binary, native spatial extension


## 2. Data Sources
- addresses: donnees.montreal.ca → unités d'évaluation foncière
- points of interest: geofabrik
- transit: stm.info
- bike paths: donnees.montreal.ca → Réseau cyclable
- parks: donnees.montreal.ca → grands parcs, parcs d'arrondissements et espaces publics
- municipality boundaries: donnees.montreal.ca → limites-administratives-agglomeration
- (to add) rent: Centris API or web scrape for average rents by neighborhood


## 3. Data Transformation

### Ingest layer
- save to s3 as parquet

### Index layer
- add columns: `h3_r10` for analysis,  `h3_r6` column for partitioning
- to convert: `addresses`, `pois`, `transit_stops`,`parks`
- bike paths: `h3.polyfill` 

### Categorize layer
- `pois` categorized:
  - `grocery`: Food shops (`supermarket`, `convenience`, `deli`)
  - `school`: Amenity (`school\college`, `university`) Various (`kindergarden`)
  - `health`: Amenity (`clinic`, `hospital`, `pharmacy`)

### Distance + Score layer
- For each address, compute distance to nearest POI of each category. (use h3 kring )
- score distance to 0–100 based on thresholds (e.g., 100m or less = 100 points, 500m = 50 points, etc.)
- final livability score = 0.2 * grocery + 0.2 * transit + 0.2 * park + 0.15 * bike + 0.15 * school + 0.10 * health

### Analytics layer
Folium choropleth: aggregate scores to `h3_r9` cells, color by mean livability, hover for the breakdown.

## 4. Future Extensions
- personalized recommender
- rent overlay
- asset checks + monitoring
- freshness policy + automation schedule (e.g., update monthly or when data sources refresh)

### Personalized Recommender
The base livability layer is reusable. Add these assets on top:

- *user defined anchors*: import locations - e.g., work, climbing gym, etc. list of `{name, lat, lng, weight, mode}`
- *anchor_travel_times*: per H3 cell, time to each anchor using the specified mode (e.g., driving, transit, biking)
- *personalized_score*: base livability + anchor proximity score (e.g., 100 points for under 15 min, 50 points for 30 min, etc.)
- *top_10_neighborhoods*: rank H3 cells by personalized score and explain why 