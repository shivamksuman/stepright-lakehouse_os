# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
dbutils.widgets.dropdown("data_source", "clickstream", ["clickstream", "inventory"], "Data Source")
dbutils.widgets.dropdown("catalog", "dev", ["dev", "prod"])

data_source = dbutils.widgets.get("data_source")
catalog = dbutils.widgets.get("catalog")

print(f"Selected source: {data_source} and catalog: {catalog}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    DateType, TimestampType, DoubleType, LongType
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 0) Schemas + config dict — same pattern as SCD1/SCD2 notebooks

# COMMAND ----------

silver_clickstream_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("product_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("search_term", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_source_file", StringType(), True),
])

silver_inventory_schema = StructType([
    StructField("snapshot_id", StringType(), True),
    StructField("snapshot_date", DateType(), True),
    StructField("product_id", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("quantity_on_hand", LongType(), True),
    StructField("quantity_reserved", LongType(), True),
    StructField("quantity_available", LongType(), True),
    StructField("reorder_point", LongType(), True),
    StructField("days_of_supply", LongType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_source_file", StringType(), True),
])

# Single source of truth per data_source — same convention as silver_scd1.py /
# silver_scd2.py. Each entry names: the source table, target table, checkpoint
# path, schema, and which column (if any) needs a type cast in this notebook
# (bronze deliberately stays permissive/string-typed; Silver commits to the
# real type, once, here).
DATA_SOURCE_CONFIG = {
    "clickstream": {
        "schema": silver_clickstream_schema,
        "source_table_suffix": "bronze_clickstream_valid",
        "target_table_suffix": "silver_clickstream",
        "checkpoint_suffix": "silver_clickstream",
        "cast_column": "event_timestamp",
        "cast_type": "timestamp",
    },
    "inventory": {
        "schema": silver_inventory_schema,
        "source_table_suffix": "bronze_inventory_valid",
        "target_table_suffix": "silver_inventory",
        "checkpoint_suffix": "silver_inventory",
        "cast_column": "snapshot_date",
        "cast_type": "date",
    },
}

config = DATA_SOURCE_CONFIG[data_source]
schema = config["schema"]
cast_column = config["cast_column"]
cast_type = config["cast_type"]

source_table = f"{catalog}.os_stepright.{config['source_table_suffix']}"
target_table = f"{catalog}.os_stepright.{config['target_table_suffix']}"
checkpoint_loc = f"/Volumes/{catalog}/os_stepright/checkpoints/{config['checkpoint_suffix']}"

print(f"source_table is-> [{source_table}] & target_table is-> [{target_table}]")
print(f"checkpoint is: {checkpoint_loc}")
print(f"casting column [{cast_column}] to [{cast_type}]")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1) Target table existence check — same as every other Silver notebook

# COMMAND ----------

def target_table_existense_check(target_table, schema):

    if not spark.catalog.tableExists(target_table):
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").saveAsTable(target_table)
    else:
        pass

target_table_existense_check(target_table, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Read source as a stream + conform (single type cast)
# MAGIC
# MAGIC No CDC envelope here — clickstream and inventory are both file-based
# MAGIC sources (Auto Loader, not Debezium), so no after.*/op/ts_ms selectExpr
# MAGIC is needed, unlike the SCD2 notebook. Every column passes through as-is
# MAGIC from Bronze except the one column that needs a real type.

# COMMAND ----------

def read_source_table(source_table, cast_column, cast_type):

    source_df = (spark.readStream.table(source_table)
        .withColumn(cast_column, F.col(cast_column).cast(cast_type))
        .drop("is_valid")
    )

    return source_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Write — straight append, no merge/upsert, no quarantine split
# MAGIC
# MAGIC Both sources are naturally append-only event/snapshot logs — every row
# MAGIC is already its own distinct fact by construction (a clickstream event
# MAGIC happened once; an inventory snapshot is a distinct point-in-time
# MAGIC record). No foreachBatch needed either, since there's no custom
# MAGIC per-batch logic (no dedup, no merge) — this is a plain structured
# MAGIC streaming write, same shape as the Bronze ingestion notebooks.

# COMMAND ----------

source_df = read_source_table(source_table, cast_column, cast_type)

(source_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_loc)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(target_table)
)