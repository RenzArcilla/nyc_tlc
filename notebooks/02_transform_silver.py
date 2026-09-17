# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Transform
# MAGIC Cleans raw trips: drops bad rows, casts types, derives duration/speed.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "sandbox"

bronze = spark.table(f"{CATALOG}.bronze.raw_trips")

# COMMAND ----------

# TLC column names vary slightly by year — this targets the 2024 yellow taxi schema.
silver = (
    bronze
    .withColumnRenamed("tpep_pickup_datetime", "pickup_ts")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_ts")
    .withColumnRenamed("PULocationID", "pickup_zone_id")
    .withColumnRenamed("DOLocationID", "dropoff_zone_id")
    .filter(F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull())
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .withColumn(
        "trip_duration_min",
        (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0
    )
    .filter((F.col("trip_duration_min") > 1) & (F.col("trip_duration_min") < 180))  # drop 0-min and >3hr trips
    .withColumn("speed_mph", F.col("trip_distance") / (F.col("trip_duration_min") / 60.0))
    .filter(F.col("speed_mph") < 80)  # drop physically implausible speeds
    .withColumn("pickup_hour", F.hour("pickup_ts"))
    .withColumn("pickup_date", F.to_date("pickup_ts"))
    .withColumn(
        "tip_pct",
        F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount") * 100).otherwise(0)
    )
)

print(f"Bronze rows: {bronze.count()} -> Silver rows after cleaning: {silver.count()}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")

(
    silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.silver.trips_cleaned")
)

# COMMAND ----------

display(spark.table(f"{CATALOG}.silver.trips_cleaned").limit(10))