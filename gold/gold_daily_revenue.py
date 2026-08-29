# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, TimestampType, DoubleType, LongType

from pyspark.sql.window import Window

from delta.tables import DeltaTable

# COMMAND ----------

from gold_daily_revenue_logic import compute_daily_revenue_logic

# COMMAND ----------

src_orders_scd2 = "dev.os_stepright.silver_orders_scd2"

src_order_items = "dev.os_stepright.silver_order_items"

src_products_scd1 = "dev.os_stepright.silver_products_scd1"

src_categories_scd1 = "dev.os_stepright.silver_categories_scd1"

target_table = "dev.os_stepright.gold_daily_revenue"

# COMMAND ----------

checkpointLocation_loc= "/Volumes/dev/os_stepright/checkpoints/gold_daily_revenue"

# COMMAND ----------


GOLD_DAILY_REVENUE_SCHEMA_DDL = """
    order_date DATE NOT NULL,
    category_id STRING NOT NULL,
    category_name STRING NOT NULL,
    shipping_country STRING NOT NULL,
    shipping_state STRING NOT NULL,
    shipping_city STRING NOT NULL,
    gross_revenue DOUBLE,
    total_discount DOUBLE,
    net_revenue DOUBLE,
    order_count LONG,
    total_units_sold LONG,
    CONSTRAINT pk_gold_daily_revenue PRIMARY KEY (order_date, category_id, shipping_country, shipping_state, shipping_city)
"""


if not spark.catalog.tableExists(target_table):
    spark.sql(f"CREATE TABLE {target_table} ({GOLD_DAILY_REVENUE_SCHEMA_DDL}) USING delta")
else:
    pass

# COMMAND ----------

df_src_orders_scd2 = spark.table(src_orders_scd2)

df_src_order_items = spark.table(src_order_items)

df_src_products_scd1 = spark.table(src_products_scd1)

df_src_categories_scd1 = spark.table(src_categories_scd1)

# COMMAND ----------

orders_stream_df = spark.readStream.table(src_orders_scd2)

# COMMAND ----------

def process_batch(micro_batch_df, batch_id):

    # dedup: if the same order changed twice in this micro-batch, keep only the latest version
    window_spec = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
    micro_batch_df = (
        micro_batch_df
        .withColumn("rank", F.row_number().over(window_spec))
        .filter("rank = 1")
        .drop("rank")
    )

    # step 1: which order_dates changed in this micro-batch?
    affected_dates_df = (
        micro_batch_df.select(F.to_date(F.col("order_date")).alias("order_date")).distinct()
    )
    affected_dates = [row["order_date"] for row in affected_dates_df.collect()]
    if not affected_dates:
        return

    # step 2: batch-read full orders + order_items, scoped to only affected dates
    scoped_orders_df = (
        df_src_orders_scd2
        .filter("year(valid_to)= '9999' and month(valid_to)= '12' and day(valid_to)='31'")
        .filter(F.to_date(F.col("order_date")).isin(affected_dates))
    )

    scoped_order_items_df = (
        df_src_order_items.join(scoped_orders_df.select("order_id"), "order_id", "inner")
    )

    # step 3: run the pure logic function on just the scoped subset
    result_df = compute_daily_revenue_logic(
        scoped_orders_df, scoped_order_items_df, df_src_products_scd1, df_src_categories_scd1
    )

    # step 4: replaceWhere — full, correct replacement of just the affected days
    date_list = ", ".join([f"'{d}'" for d in affected_dates])
    (
        result_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"order_date IN ({date_list})")
        .saveAsTable(target_table)
    )


# COMMAND ----------

(
    orders_stream_df.writeStream
    .foreachBatch(process_batch)
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpointLocation_loc)
    .start()
    .awaitTermination()
)