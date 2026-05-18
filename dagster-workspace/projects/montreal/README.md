# Montreal Address-Level Livability Scorer

**Goal:** Dagster pipeline that computes a livability score for every address in Montreal.

**Extension:** Where to live based on your actual life" recommender.

## 1. Architecture

### Stack choices: 
- *dagster* for orchestration - matches LL's primary tool. 
- *s3* for parquet storage - standard data lake pattern
- *DuckDB* for SQL querying and distance calculations - fast, single-binary, native spatial extension
- *Folium* for map visualization - Pythonic wrapper around Leaflet, easy to generate interactive HTML maps
- *Geopandas* as needed - for any geometry manipulation that DuckDB can't handle natively
- *H3* for spatial indexing - critical for efficient distance calculations at scale

### Asset layering:
1. **Ingest layer**: raw data pulled from sources, stored as Parquet (`raw_addresses`, `raw_osm_pois`, etc.)
2. **Categorize layer**: split OSM POIs into clean categories (`pois_categorized`)
3. **Index layer**: add H3 columns to geographic datasets (`addresses_h3`, `pois_h3`, etc.)
4. **Distance layer**: compute distance from each address to nearest POI in each category (`distances_to_amenities`)
5. **Score layer**: convert distances to scores, combine into overall livability (`livability_score`)
6. **Viz layer**: generate an interactive map of livability scores (`livability_map`)

## 2. Data Sources
- (done) addresses: donnees.montreal.ca → unités d'évaluation foncière
- points of interest: open street maps
- Transit stops: STM GTFS feed (stm.info developers page)
- Bike paths: data.montreal.ca → pistes cyclables
- (done) parks: donnees.montreal.ca → grands parcs, parcs d'arrondissements et espaces publics
- rent: Centris API or web scrape for average rents by neighborhood

## 3. Assets in Detail

### Ingest layer (raw_*)
Each pulls a remote file → Parquet on disk. No transformation. Cached so re-runs are cheap.

### Index layer (*_h3)
- `addresses_h3` — adds `h3_r9` (≈170m hex, fine-grained) and `h3_r7` (≈5km hex, for partitioning/aggregation) columns
- POIs follow the same indexing scheme
- Bike paths: use `h3.polyfill` after buffering the LineStrings, so you get all H3 cells the path touches

### Categorize layer
- `pois_categorized` — split OSM `amenity` / `shop` / `leisure` tags into clean categories:
  - `grocery` → `shop=supermarket | convenience | greengrocer`
  - `school` → `amenity=school | kindergarten`
  - `park` → `leisure=park | garden` (prefer city's official parks layer if available)
  - Extension-ready: `health`, `restaurant`, `gym`, `library`

### Distance layer — the spatial-join story
For each address, compute distance to nearest POI of each category.

**Naive approach:** address × POI cross-join → haversine → min. O(n × m). Dies on full Montreal.

**Smart approach (what you do):**
1. Both addresses and POIs are H3-indexed at resolution 9
2. For each address H3 cell, look at its `k_ring(2)` neighbors (covers ~500m radius)
3. Compute haversine only for POIs in those cells; take min
4. Fallback: if no POI in `k_ring(2)`, expand to `k_ring(5)` (~1.5km), then `k_ring(10)` if still empty
5. Output: one row per (address, category) with distance

**This is the spatial-join story you tell Tuesday.** Specifically: "I used H3 to spatially bucket both layers, then did localized distance computations on neighboring cells. Brings the join from O(n × m) toward O(n × k) where k is the typical POI density per neighborhood."

### Score layer
Per category, distance → 0–100 score via piecewise function:
- ≤ 300 m → 100
- 300–800 m → linear decay to 50
- 800–1500 m → linear decay to 20
- \> 1500 m → 0

Final score is a weighted combination:
```python
livability = (
    0.25 * grocery_score
  + 0.25 * transit_score
  + 0.20 * park_score
  + 0.15 * bike_score
  + 0.15 * school_score
)
```

Weights are configurable via a Dagster `Config` resource — this matters for the extension.

### Viz layer
Folium choropleth: aggregate scores to `h3_r9` cells, color by mean livability, hover for the breakdown. Output: `livability_montreal.html`.

---

## 4. Partitioning Strategy

**Partition key: `arrondissement` (Montreal has 19 boroughs).** Static partition.


## 5. Build Plan — Sunday, Timeboxed

**Block 1 (3 hr): scaffolding + ingest**
- `pip install dagster dagster-webserver duckdb h3 geopandas folium`
- `dagster project scaffold --name mtl_livability`
- Get the UI running, see the empty asset graph render
- Implement `raw_addresses`, `raw_osm_pois`, `raw_stm_stops`, `raw_bike_paths`
- Confirm Parquet outputs land on disk; click "materialize" in the UI for each

**Block 2 (2 hr): indexing + categorization**
- H3 columns added to all geographic assets via `h3.geo_to_h3(lat, lng, resolution)`
- POI categorization with a small lookup dict
- Add one `asset_check` per category-assignment asset (schema + non-empty)

**Block 3 (2 hr): distance + scoring — the meat**
- `distances_to_amenities` using the H3 k_ring trick
- `livability_score` with the weighted combination
- Sanity-check: known good neighborhoods (Plateau, Mile End) should score high; outer industrial zones should score low. If they don't, fix the weights or distance curves.

**Block 4 (1 hr): viz + cleanup**
- Folium map → HTML output
- README with one screenshot
- Push to GitHub (private is fine, but you can show it from your phone)

**Total: ~8 hr.** Comfortably one day if you start by 10am.

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

**Cut entirely for now:**
- Multi-borough partition definition (single hardcoded borough is fine for MVP)
- The personalized recommender (#3) — that's a sketch you describe, not build

---

## 6. Extension to #3 — Personalized Recommender

The base livability layer is reusable. Add these assets on top:

- **`user_anchors`** (Dagster config asset): list of `{name, lat, lng, weight, mode}` — e.g., `{name="work", lat=..., lng=..., weight=2.0, mode="transit"}`, `{name="climbing", lat=..., lng=..., weight=1.0, mode="walk"}`
- **`anchor_travel_times`**: per H3 cell, time to each anchor — OSRM API for proper routing, or haversine × mode-specific multiplier for an MVP
- **`personalized_score`**: weighted combo of base livability + inverse anchor travel times
- **`top_10_neighborhoods`**: rank H3 cells by personalized score, return top 10 with the score breakdown so users can see *why*

**The design payoff:** the livability layer doesn't change. Distances to amenities are computed once, consumed by both the base score asset and the personalized score asset. That's the compositional pattern Dagster's asset-first model encourages, and it's worth naming explicitly in the interview.

---

## 7. Interview Pitch (30 sec, Tuesday)

> "Over the weekend I was building a Montreal-only version of what you do — address-level livability scoring in Dagster, H3 indexing on top of Parquet, DuckDB for the distance compute. The architecture choice I'd want to talk about is the partitioning — I went borough-level static partitions over H3 partitions, because boroughs match how consumers reason about the data and the asset count stays manageable. I designed the distance layer so a personalized neighborhood recommender slots on top by composing per-amenity distance assets rather than recomputing them. Curious how you handle that compositional pattern at your scale."

That single paragraph does: hands-on Dagster ✓, partition reasoning ✓, H3 vocabulary ✓, lakehouse stack ✓, composition pattern ✓, *and* asks them a real question. It's a wall-to-wall passion signal that costs them nothing to verify.
