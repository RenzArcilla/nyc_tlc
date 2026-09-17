# 🚕 Ride Analytics Lakehouse

A medallion-architecture data pipeline on Databricks (Free Edition) feeding a Streamlit
dashboard. Built as a prototype ride-analytics platform using real NYC TLC trip data.

**Live app:** https://databricks-nyc-tlc-dashboard.streamlit.app/

## Architecture

```
NYC TLC Parquet ──► Bronze (raw_trips) ──► Silver (trips_cleaned) ──► Gold (hourly_demand,
                                                                            zone_summary,
                                                                            tip_by_hour)
                                                                                  │
                                                                                  ▼
                                                                             CSV export ──► Streamlit
```

- **Bronze**: raw NYC Yellow Taxi trip records (150k-row sample), landed unmodified as Delta.
- **Silver**: cleaned and typed: nulls dropped, implausible trips filtered (bad fares,
  0-distance, >3hr or >80mph trips), duration/speed/tip% derived.
- **Gold**: three business-facing aggregates: trip demand by hour, top pickup zones by
  volume, average tip % by hour.
- **Serving**: Gold tables exported to CSV and read directly by Streamlit.

## Why CSV export instead of a live warehouse connection

Databricks Free Edition spins the cluster down when idle and has DBFS root access disabled,
so an always-on external connection isn't available for free. The production-shaped fix is
a serving layer that stays awake (e.g. Supabase Postgres) with Streamlit querying that
directly. This prototype exports Gold tables to CSV to get a working demo running quickly —
swapping the export step for a database write is a same-shape follow-up, not a rebuild.

## Stack

- **Compute:** Databricks Free Edition (PySpark, Delta Lake)
- **Dashboard:** Streamlit + pandas
- **Data:** NYC TLC Yellow Taxi Trip Records

## Running locally

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Requires `hourly_demand.csv`, `zone_summary.csv`, and `tip_by_hour.csv` in the same folder
as `streamlit_app.py` (regenerate them by running the three Databricks notebooks in
`notebooks/` in order).



## Data source

NYC TLC Yellow Taxi Trip Records:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Next steps

- Swap CSV export for a live Supabase Postgres serving layer
- Add OpenWeatherMap enrichment to Silver (surge/demand vs. weather correlation)
- Add OSM zone-to-district mapping for localized geospatial cuts
