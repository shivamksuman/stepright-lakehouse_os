# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, TimestampType, DoubleType, LongType

from pyspark.sql.window import Window

from delta.tables import DeltaTable

# COMMAND ----------

from gold_fulfillment_health_logic import compute_fulfillment_health_logic

# COMMAND ----------

src_silver_orders_scd2 = "dev.os_stepright.silver_orders_scd2"
target_table = "dev.os_stepright.gold_fulfillment_health"
checkpoint_loc = "/Volumes/dev/os_stepright/checkpoints/gold_fulfillment_health"

# COMMAND ----------

GOLD_FULFILLMENT_HEALTH_SCHEMA_DDL = """
    order_date DATE NOT NULL,
    region STRING NOT NULL,
    delivered_orders LONG,
    within_sla_orders LONG,
    sla_compliance_rate DOUBLE,
    CONSTRAINT pk_gold_fulfillment_health PRIMARY KEY (order_date, region)
"""

if not spark.catalog.tableExists(target_table):
    spark.sql(f"CREATE TABLE {target_table} ({GOLD_FULFILLMENT_HEALTH_SCHEMA_DDL}) USING DELTA")
else:
    pass

# COMMAND ----------

df_src_silver_orders_scd2 = spark.table(src_silver_orders_scd2)

# COMMAND ----------

df_stream_src_silver_orders_scd2 = spark.readStream.table(src_silver_orders_scd2)

# COMMAND ----------

def process_batch(micro_batch_df, batch_id):

    df_affected_dates = (
        micro_batch_df
        .filter("year(valid_to)='9999' and month(valid_to)='12' and day(valid_to)='31' and order_status='delivered'")
        .select(F.to_date(F.col("order_date")).alias("order_date"))
        .distinct()
    )
    affected_dates = [row["order_date"] for row in df_affected_dates.collect()]

    if not affected_dates:
        return

    scoped_orders = (
        df_src_silver_orders_scd2
        .filter("year(valid_to)='9999' and month(valid_to)='12' and day(valid_to)='31' and order_status='delivered'")
        .filter(F.to_date(F.col("order_date")).isin(affected_dates))
    )

    sla_days = 5
    result_df = compute_fulfillment_health_logic(scoped_orders, sla_days)

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
    df_stream_src_silver_orders_scd2.writeStream
    .foreachBatch(process_batch)
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_loc)
    .start()
    .awaitTermination()
)