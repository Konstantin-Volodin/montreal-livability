# Montreal Livability Score — Dagster Project

**Goal:** Dagster pipeline that computes a livability score for every address in Montreal.

**Extension:** Where to live based on your actual life" recommender.


## 1. Architecture

### Stack choices: 
- *dagster* for orchestration - matches LL's primary tool. 
- *s3* for parquet storage - standard data lake pattern
- *Folium* for map visualization - Pythonic wrapper around Leaflet, easy to generate interactive HTML maps
- *Geopandas* as needed - for any geometry manipulation that DuckDB can't handle natively
- *H3* for spatial indexing - critical for efficient distance calculations at scale
- (to add) *DuckDB* for SQL querying and distance calculations - fast, single-binary, native spatial extension

### Asset layering:
1. **Ingest layer**: raw data pulled from sources, stored as Parquet (`raw_addresses`, `raw_osm_pois`, etc.)
2. **Categorize layer**: split OSM POIs into clean categories (`pois_categorized`)
3. **Index layer**: add H3 columns to geographic datasets (`addresses_h3`, `pois_h3`, etc.)
4. **Distance layer**: compute distance from each address to nearest POI in each category (`distances_to_amenities`)
5. **Score layer**: convert distances to scores, combine into overall livability (`livability_score`)
6. **Viz layer**: generate an interactive map of livability scores (`livability_map`)


## 2. Data Sources
- addresses: donnees.montreal.ca → unités d'évaluation foncière
- points of interest: geofabrik
- transit: stm.info
- bike paths: donnees.montreal.ca → Réseau cyclable
- parks: donnees.montreal.ca → grands parcs, parcs d'arrondissements et espaces publics
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

Weights are configurable via a Dagster `Config` resource — this matters for the extension.

### viz layer
Folium choropleth: aggregate scores to `h3_r9` cells, color by mean livability, hover for the breakdown. Output: `livability_montreal.html`.

### MVP vs nice-to-have vs cut

**MVP (must ship):**
- Address ingest + H3 indexing
- POI categorization for grocery + transit + park
- Distance computation for those three
- Weighted livability score
- Static HTML map of one borough (Plateau as the showcase)

**Nice to have (Monday morning, if you're feeling good):**
- Add school + bike layers
- Cover all of Montreal not one borough
- Asset checks + a freshness policy
- Centris rent overlay
- The personalized recommender (#3) — that's a sketch you describe, not build

## 6. Extension to #3 — Personalized Recommender

The base livability layer is reusable. Add these assets on top:

- **`user_anchors`** (Dagster config asset): list of `{name, lat, lng, weight, mode}` — e.g., `{name="work", lat=..., lng=..., weight=2.0, mode="transit"}`, `{name="climbing", lat=..., lng=..., weight=1.0, mode="walk"}`
- **`anchor_travel_times`**: per H3 cell, time to each anchor — OSRM API for proper routing, or haversine × mode-specific multiplier for an MVP
- **`personalized_score`**: weighted combo of base livability + inverse anchor travel times
- **`top_10_neighborhoods`**: rank H3 cells by personalized score, return top 10 with the score breakdown so users can see *why*

**The design payoff:** the livability layer doesn't change. Distances to amenities are computed once, consumed by both the base score asset and the personalized score asset. That's the compositional pattern Dagster's asset-first model encourages, and it's worth naming explicitly in the interview.