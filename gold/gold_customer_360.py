# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
dbutils.widgets.dropdown("catalog", "dev", ["dev", "prod"])
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

from gold_customer_360_logic import compute_customer_360

# COMMAND ----------

# declare source (both fact & dims) and target name variables, and checkpoints

source_tables = {
    "silver_orders": "dev.os_stepright.silver_orders_scd2",
    "silver_customers": "dev.os_stepright.silver_customers_scd2"
}
checkpoint_loc = "/Volumes/dev/os_stepright/checkpoints/gold_customer_360"
target_table = "dev.os_stepright.gold_customer_360"

# COMMAND ----------

# check target table existence, if present then ignore, if not then create dynamically

GOLD_CUSTOMER_360_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("loyalty_tier", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("registration_date", TimestampType(), nullable=True),
    StructField("lifetime_value", DoubleType(), nullable=True),
    StructField("order_count", LongType(), nullable=True),
    StructField("last_order_date", TimestampType(), nullable=True),
    StructField("days_since_last_order", LongType(), nullable=True),
])

def target_table_existence_check(target_table, schema):
    if not spark.catalog.tableExists(target_table):
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").saveAsTable(target_table)
    else:
        pass

target_table_existence_check(target_table, GOLD_CUSTOMER_360_SCHEMA)

# COMMAND ----------

# streaming read on orders — drives the foreachBatch trigger.
# NOTE: customers-side profile-only changes (e.g. loyalty_tier upgrade with no
# new order) are NOT picked up by this design — orders is the sole driver.
# Documented as a known limitation, not an oversight.

orders_stream_df = (
    spark.readStream
    .table(source_tables["silver_orders"])
)

# COMMAND ----------

def process_batch(micro_batch_df, batch_id):

    # step 7: extract distinct customer_ids affected in this micro-batch
    affected_ids_df = micro_batch_df.select("customer_id").distinct()
    affected_ids = [row["customer_id"] for row in affected_ids_df.collect()]

    if not affected_ids:
        return

    # step 8: batch-read full customers/orders, scoped to only affected customers
    scoped_orders_df = (
        spark.read.table(source_tables["silver_orders"])
        .filter(F.col("customer_id").isin(affected_ids))
    )
    scoped_customers_df = (
        spark.read.table(source_tables["silver_customers"])
        .filter(F.col("customer_id").isin(affected_ids))
    )

    # run the pure logic function on the scoped subset only
    as_of_date = date.today()
    result_df = compute_customer_360(scoped_customers_df, scoped_orders_df, as_of_date)

    # MERGE only the recomputed subset into target_table
    target_delta = DeltaTable.forName(spark, target_table)
    (
        target_delta.alias("tgt")
        .merge(result_df.alias("src"), "tgt.customer_id = src.customer_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

(
    orders_stream_df.writeStream
    .foreachBatch(process_batch)
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_loc)
    .start()
    .awaitTermination()
)

# COMMAND ----------

spark.sql(f"select * from {target_table}").display()