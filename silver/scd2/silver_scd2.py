# Databricks notebook source
dbutils.widgets.dropdown("data_source", "customers", ["customers", "orders"], "Data Source")
dbutils.widgets.dropdown("catalog", "dev", ["dev", "prod"])

data_source = dbutils.widgets.get("data_source")
catalog = dbutils.widgets.get("catalog")

print(f"Selected source: {data_source} and catalog: {catalog}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, TimestampType, DoubleType

from pyspark.sql.window import Window

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #### 0) Schemas + single config dict (replaces scattered if data_source==... branches)

# COMMAND ----------

silver_customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("registration_date", TimestampType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("address_line1", StringType(), True),
    StructField("address_line2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_source_file", StringType(), True),
    StructField("valid_from", TimestampType(), True),
    StructField("valid_to", TimestampType(), True),
])

silver_orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("shipping_address_id", StringType(), True),
    StructField("shipping_city", StringType(), True),
    StructField("shipping_state", StringType(), True),
    StructField("shipping_country", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("discount_code", StringType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_source_file", StringType(), True),
    StructField("valid_from", TimestampType(), True),
    StructField("valid_to", TimestampType(), True),
])

customer_tracked_cols = ["email", "first_name", "last_name", "phone", "date_of_birth",
                "gender", "registration_date", "loyalty_tier", "address_line1",
                "address_line2", "city", "state", "zip_code", "country", "is_active"]

orders_tracked_cols = ["customer_id", "order_status", "order_date", "updated_at",
                 "shipping_address_id", "shipping_city", "shipping_state",
                 "shipping_country", "payment_method", "discount_code",
                 "discount_amount", "total_amount"]

# Single source of truth per data_source — replaces every scattered
# "if data_source == 'customers': ... else: ..." block that used to be
# repeated across every function in this notebook.
DATA_SOURCE_CONFIG = {
    "customers": {
        "pk": "customer_id",
        "schema": silver_customers_schema,
        "tracked_cols": customer_tracked_cols,
        "source_table_suffix": "bronze_customers_valid",
        "target_table_suffix": "silver_customers_scd2",
        "checkpoint_suffix": "silver_customers_scd2",
    },
    "orders": {
        "pk": "order_id",
        "schema": silver_orders_schema,
        "tracked_cols": orders_tracked_cols,
        "source_table_suffix": "bronze_orders_valid",
        "target_table_suffix": "silver_orders_scd2",
        "checkpoint_suffix": "silver_orders_scd2",
    },
}

config = DATA_SOURCE_CONFIG[data_source]
pk = config["pk"]
silver_table_schema = config["schema"]
tracked_cols = config["tracked_cols"]

source_table = f"{catalog}.os_stepright.{config['source_table_suffix']}"
target_table = f"{catalog}.os_stepright.{config['target_table_suffix']}"
checkpoint_loc = f"/Volumes/{catalog}/os_stepright/checkpoints/{config['checkpoint_suffix']}"

print(f"source_table is-> [{source_table}] And target_table is-> [{target_table}]")
print(f"checkpoint_loc is-> [{checkpoint_loc}]")

# COMMAND ----------

def target_table_existence_check(target_table, silver_table_schema):

    if not spark.catalog.tableExists(target_table):
        empty_df = spark.createDataFrame([], silver_table_schema)
        empty_df.write.format("delta").saveAsTable(target_table)
    else:
        pass

# COMMAND ----------

# NULL-safe comparison: plain <> / = returns NULL (not true/false) whenever
# either side is NULL, which silently breaks both changed_condition's OR-chain
# and no_changed_condition's AND-chain for any nullable tracked column.
# <=> (null-safe equality) treats NULL <=> NULL as true and NULL <=> 'x' as
# false, so NOT(src.col <=> tgt.col) is only true on a genuine difference.
changed_condition = " OR ".join([f"NOT (src.{c} <=> tgt.{c})" for c in tracked_cols])
print(changed_condition)

# COMMAND ----------

no_changed_condition = " AND ".join([f"src.{c} <=> tgt.{c}" for c in tracked_cols])
print(no_changed_condition)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1) Read source_table as a stream (incremental, checkpoint-tracked)
# MAGIC
# MAGIC Previously this did `spark.table(source_table)` — a full batch read of
# MAGIC the ENTIRE bronze table on every run. Correct by accident (dedup +
# MAGIC compare-against-current-target made it idempotent either way), but
# MAGIC wasteful and defeats the point of Bronze being incrementally ingested.
# MAGIC Switched to readStream + foreachBatch: Spark hands each new micro-batch
# MAGIC to process_batch as a plain static DataFrame, so every function below
# MAGIC (dedup / split / write) is untouched and runs exactly as before — only
# MAGIC the *how it's fed* changed, not the logic itself.

# COMMAND ----------

def read_source_table(source_table):

    source_df = (spark.readStream.table(source_table)
             .selectExpr('after.*', 'op', 'ts_ms', '_source_file', '_ingested_at')
             .withColumn("ts_ms", (F.col("ts_ms") / 1000).cast("timestamp"))
    )

    return source_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) deduplicate incoming source_df

# COMMAND ----------

def dedup_incoming_source_df(source_df, pk):

    window_spec = Window.partitionBy(F.col(pk)).orderBy(F.col("ts_ms").desc())

    dedup_source_df = (source_df.withColumn("rn", F.row_number().over(window_spec))
                   .filter("rn=1")
                   .drop("rn")
    )

    return dedup_source_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) splitting incoming source_df into buckets like deletes_df, changed_rows, new_rows, no_op_changes

# COMMAND ----------

def dedup_incoming_source_split_into_buckets(dedup_source_df, target_table, pk, changed_condition, no_changed_condition):

    src_pk = f"src.{pk}"
    tgt_pk = f"tgt.{pk}"

    tgt_df = spark.table(target_table).filter("valid_to='9999-12-31'")

    deletes_df = dedup_source_df.where("op='d'")

    dedup_source_op_cu_df = dedup_source_df.filter("op in ('c', 'u')")

    changed_rows = (dedup_source_op_cu_df.alias("src")
                .join(tgt_df.alias("tgt"), F.col(src_pk) == F.col(tgt_pk), "inner")
                .where(changed_condition)
    ).selectExpr("src.*")

    new_rows = (dedup_source_op_cu_df.alias("src")
            .join(tgt_df.alias("tgt"), F.col(src_pk) == F.col(tgt_pk), "left_anti")
    ).selectExpr("src.*")

    no_op_changes = (dedup_source_op_cu_df.alias("src")
                 .join(tgt_df.alias("tgt"), F.col(src_pk) == F.col(tgt_pk), "inner")
                .where(no_changed_condition)
    ).selectExpr("src.*")

    return deletes_df, changed_rows, new_rows, no_op_changes

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4) writing to target

# COMMAND ----------

def write_to_target(target_table, pk, deletes_df, changed_rows, new_rows):

    merge_cond = f"t.{pk} = s.{pk} AND t.valid_to = '9999-12-31'"
    s_pk = f"s.{pk}"
    t_pk = f"t.{pk}"

    # step 1: retire — close out old current rows for deletes + changed keys
    target_delta = DeltaTable.forName(spark, target_table)

    retire_source = deletes_df.unionByName(changed_rows)

    (target_delta.alias("t")
        .merge(retire_source.alias("s"), merge_cond)
        .whenMatchedUpdate(set={
            "valid_to": "s.ts_ms"
        })
        .execute()
    )

    # step 2: insert — new current versions for changed keys + brand new keys,
    # guarded by an anti-join on (pk, valid_from) so a retried/replayed batch
    # can never append a duplicate version row.
    rows_to_insert = changed_rows.unionByName(new_rows)

    rows_to_insert_final = (
        rows_to_insert
        .withColumn("valid_from", F.col("ts_ms"))
        .withColumn("valid_to", F.lit("9999-12-31").cast("timestamp"))
        .withColumn("valid_from", F.col("valid_from").cast("timestamp"))
        .drop("op", "ts_ms")
    )

    existing_target = spark.table(target_table).select(pk, "valid_from")

    final_insert_df = (rows_to_insert_final.alias("s")
        .join(
            existing_target.alias("t"),
            (F.col(s_pk) == F.col(t_pk)) &
            (F.col("s.valid_from") == F.col("t.valid_from")),
            "left_anti"
        )
    )

    final_insert_df.write.format("delta").mode("append").saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5) foreachBatch entrypoint + stream start
# MAGIC
# MAGIC process_batch is called once per micro-batch with a plain, static
# MAGIC DataFrame (batch_df) — every function above runs exactly as it did
# MAGIC in the old fully-batch version, just invoked from here instead of
# MAGIC directly at notebook top level.

# COMMAND ----------

target_table_existence_check(target_table, silver_table_schema)

def process_batch(batch_df, batch_id):

    dedup_source_df = dedup_incoming_source_df(batch_df, pk)

    deletes_df, changed_rows, new_rows, no_op_changes = dedup_incoming_source_split_into_buckets(
        dedup_source_df, target_table, pk, changed_condition, no_changed_condition
    )

    write_to_target(target_table, pk, deletes_df, changed_rows, new_rows)

source_stream_df = read_source_table(source_table)

(source_stream_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_loc)
    .trigger(availableNow=True)
    .start()
)
