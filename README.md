# Lakehouse Geospatial

## Example: Ingesting CSV and ArcGIS GeoJSON data in the Lakehouse and perform Spatial analysis.

__Notes__

* This example was run using DBR 16.0 on a 3 worker cluster (each with 8 CPUs and 61GB RAM);
* Requires a cluster with product `ST_` spatial sql functions enabled for KeplerGL Viz (may require photon cluster + spatial sql flag enabled).
* For the notebook to render well in github, we add screenshots of the map rendering and charts as well as artificially limit tabular results; when you run the notebook in databricks, you can uncomment and remove limits if desired.

---
__Author:__ Mathieu Pelletier <mathieu.pelletier@databricks.com> | _Last Modified:_ 14 APR 2025

## Repo structure

- data
  - UFO Sightings CSV file
- notebooks
  - 00 - Setup: configure catalog, database and volume
  - 01 - Data ingestion and preparation: ingest CSV and ArcGIS data
  - 02 - Spatial analysis: perform transformation and persist to Delta tables
  - 03 - Publish results: visualize data externally using PowerBI and/or Databricks Apps
- src
  - Source code for ArcGIS reader and sample app