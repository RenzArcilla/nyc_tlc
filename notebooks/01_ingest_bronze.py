# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 - Bronze Ingest
# MAGIC Pulls a small sample of NYC TLC Yellow Taxi trip data and lands it as-is in the Bronze layer.
# MAGIC Kept small on purpose (free-tier cluster).

# COMMAND ----------

CATALOG = "sandbox"
BRONZE_SCHEMA = "bronze"

# One month parquet data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

# COMMAND ----------

# Download to local driver storage, then load (avoids needing external storage creds)
import urllib.request
import os

local_path = "/tmp/yellow_tripdata_sample.parquet"
if not os.path.exists(local_path):
    urllib.request.urlretrieve(TLC_URL, local_path)

# COMMAND ----------

# DBTITLE 1,Cell 4
# Read via pandas first (serverless compute doesn't allow /tmp/ access for Spark)
import pandas as pd

SAMPLE_ROWS = 150_000
pdf = pd.read_parquet(local_path)
pdf_sample = pdf.head(SAMPLE_ROWS)

# Convert to Spark DataFrame
df_sample = spark.createDataFrame(pdf_sample)

print(f"Full file rows (approx): {len(pdf)}, sampling {SAMPLE_ROWS} rows")
df_sample.printSchema()

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

(
    df_sample.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.raw_trips")
)

print(f"Wrote {df_sample.count()} rows to {CATALOG}.{BRONZE_SCHEMA}.raw_trips")

# COMMAND ----------

display(spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_trips").limit(10))