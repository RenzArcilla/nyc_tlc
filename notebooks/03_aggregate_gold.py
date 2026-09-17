# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Aggregation
# MAGIC Three focused gold tables: hourly demand, zone summary, tip-by-hour.
# MAGIC Then exports each to CSV in the driver's local filesystem so Streamlit
# MAGIC can read them without needing a live cluster or Supabase set up.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "sandbox"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

silver = spark.table(f"{CATALOG}.silver.trips_cleaned")

# COMMAND ----------

# Gold 1: demand by hour of day
hourly_demand = (
    silver.groupBy("pickup_hour")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance")
    )
    .orderBy("pickup_hour")
)

hourly_demand.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.hourly_demand")

# COMMAND ----------

# Gold 2: zone summary (top pickup zones by volume)
zone_summary = (
    silver.groupBy("pickup_zone_id")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance")
    )
    .orderBy(F.desc("trip_count"))
    .limit(25)
)

zone_summary.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.zone_summary")

# COMMAND ----------

# Gold 3: tip percentage by hour
tip_by_hour = (
    silver.groupBy("pickup_hour")
    .agg(F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"))
    .orderBy("pickup_hour")
)

tip_by_hour.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.tip_by_hour")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export for Streamlit (file-based serving layer)

# COMMAND ----------

print("=== hourly_demand.csv ===")
print(hourly_demand.toPandas().to_csv(index=False))

print("=== zone_summary.csv ===")
print(zone_summary.toPandas().to_csv(index=False))

print("=== tip_by_hour.csv ===")
print(tip_by_hour.toPandas().to_csv(index=False))