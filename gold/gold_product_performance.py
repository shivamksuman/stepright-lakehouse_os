# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, TimestampType, DoubleType, LongType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

from gold_product_performance_logic import compute_product_performance_logic

# COMMAND ----------

target_table = "dev.os_stepright.gold_product_performance"

src_silver_order_items = "dev.os_stepright.silver_order_items"

src_silver_products_scd1 = "dev.os_stepright.silver_products_scd1"

src_silver_inventory = "dev.os_stepright.silver_inventory"

checkpoint_loc = "/Volumes/dev/os_stepright/checkpoints/gold_product_performance"

# COMMAND ----------

GOLD_PRODUCT_PERFORMANCE_SCHEMA_DDL = """
    product_id STRING NOT NULL,
    product_name STRING,
    brand STRING,
    units_sold LONG,
    revenue DOUBLE,
    current_stock LONG,
    stockout_risk BOOLEAN,
    CONSTRAINT pk_gold_product_performance PRIMARY KEY (product_id)
"""

if not spark.catalog.tableExists(target_table):
    spark.sql(f"create table {target_table} ({GOLD_PRODUCT_PERFORMANCE_SCHEMA_DDL}) using delta")
else:
    pass

# COMMAND ----------

df_src_silver_order_items = spark.table(src_silver_order_items)
df_src_silver_products_scd1 = spark.table(src_silver_products_scd1)
df_src_silver_inventory = spark.table(src_silver_inventory)

# COMMAND ----------

orders_items_stream_df = spark.readStream.table(src_silver_order_items)

# COMMAND ----------

def process_batch(micro_batch_df, batch_id):

    df_affected_product_id = micro_batch_df.selectExpr("product_id").distinct()

    affected_product_id = [row["product_id"] for row in df_affected_product_id.collect()]

    if not affected_product_id:
        return
    
    scoped_orders_items = df_src_silver_order_items.filter(F.col("product_id").isin(affected_product_id))

    scoped_inventory = df_src_silver_inventory.filter(F.col("product_id").isin(affected_product_id))

    scoped_products = df_src_silver_products_scd1.filter(F.col("product_id").isin(affected_product_id))

    result_df = compute_product_performance_logic(scoped_orders_items, scoped_inventory, scoped_products)

    target_delta = DeltaTable.forName(spark, target_table)
    (
        target_delta.alias("tgt")
        .merge(result_df.alias("src"), "tgt.product_id = src.product_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )



# COMMAND ----------

(
    orders_items_stream_df.writeStream
    .foreachBatch(process_batch)
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_loc + "_order_items")
    .start()
    .awaitTermination()
)

# COMMAND ----------

inventory_stream_df = spark.readStream.table(src_silver_inventory)

# COMMAND ----------

def process_inventory_batch(micro_batch_df, batch_id):
    df_affected_product_id = micro_batch_df.selectExpr("product_id").distinct()
    affected_product_id = [row["product_id"] for row in df_affected_product_id.collect()]

    if not affected_product_id:
        return

    scoped_orders_items = df_src_silver_order_items.filter(F.col("product_id").isin(affected_product_id))
    scoped_inventory = df_src_silver_inventory.filter(F.col("product_id").isin(affected_product_id))
    scoped_products = df_src_silver_products_scd1.filter(F.col("product_id").isin(affected_product_id))

    result_df = compute_product_performance_logic(scoped_orders_items, scoped_inventory, scoped_products)

    target_delta = DeltaTable.forName(spark, target_table)
    (
        target_delta.alias("tgt")
        .merge(result_df.alias("src"), "tgt.product_id = src.product_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

(
    inventory_stream_df.writeStream
    .foreachBatch(process_inventory_batch)
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_loc + "_inventory")
    .start()
    .awaitTermination()
)