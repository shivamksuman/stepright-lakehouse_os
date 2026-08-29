# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, TimestampType, DoubleType, LongType

from pyspark.sql.window import Window

from delta.tables import DeltaTable

# COMMAND ----------

from gold_funnel_analysis_logic import compute_funnel_analysis_logic

# COMMAND ----------

src_silver_clickstream= "dev.os_stepright.silver_clickstream"

target_table= "dev.os_stepright.gold_funnel_analysis"

# COMMAND ----------

checkpoint_loc = "/Volumes/dev/os_stepright/checkpoints/gold_funnel_analysis"

# COMMAND ----------

GOLD_FUNNEL_ANALYSIS_SCHEMA_DDL = """
    referrer STRING NOT NULL,
    total_sessions LONG,
    converted_sessions LONG,
    conversion_rate_percentage DOUBLE,
    CONSTRAINT pk_gold_funnel_analysis PRIMARY KEY (referrer)
"""

if not spark.catalog.tableExists(target_table):
    spark.sql(f"CREATE TABLE {target_table} ({GOLD_FUNNEL_ANALYSIS_SCHEMA_DDL}) USING DELTA")
else:
    pass

# COMMAND ----------

df_src_silver_clickstream = spark.table(src_silver_clickstream)

# COMMAND ----------

clickstream_stream_df = spark.readStream.table(src_silver_clickstream)


# COMMAND ----------

def process_batch(micro_batch_df, batch_id):

    df_affected_referrer = (
        micro_batch_df.withColumn("referrer", F.coalesce(F.col("referrer"), F.lit("direct")))
        .select("referrer").distinct()
    )

    affected_referrer= [row['referrer'] for row in df_affected_referrer.collect()]

    if not affected_referrer:
        return
    
    scoped_clickstream = (df_src_silver_clickstream
                          .withColumn("referrer", F.coalesce(F.col("referrer"), F.lit("direct")))
                          .filter(F.col("referrer").isin(affected_referrer))
    )

    result_df = compute_funnel_analysis_logic(scoped_clickstream)

    target_delta = DeltaTable.forName(spark, target_table)

    (
        target_delta.alias("tgt")
        .merge(result_df.alias("src"), "tgt.referrer = src.referrer")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

(
    clickstream_stream_df.writeStream
    .foreachBatch(process_batch)
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_loc)
    .start()
    .awaitTermination()
)

# COMMAND ----------

spark.sql(f"select * from {target_table}").display()

# COMMAND ----------



# COMMAND ----------

df_src_silver_clickstream_coalesce_referrer = (
        df_src_silver_clickstream.withColumn("referrer", F.coalesce(F.col("referrer"), F.lit("direct")))
        .groupBy(F.col("session_id"), F.col("referrer"))
        .agg(
            F.expr("MAX(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END)").alias("converted")
        )
)

df_src_silver_clickstream_coalesce_referrer.display()

# COMMAND ----------

df_groupedBy_referrer = (
        df_src_silver_clickstream_coalesce_referrer.groupBy(F.col("referrer"))
            .agg(
                F.count(F.col("session_id")).alias("total_sessions"), 
                F.sum(F.col("converted")).alias("converted_sessions")
                )
            .withColumn("conversion_rate_percentage", F.when(F.col("total_sessions")>0, F.round((F.col("converted_sessions")/F.col("total_sessions"))*100.0, 2)).otherwise(F.lit(0))
                        )
)

df_groupedBy_referrer.display()

# COMMAND ----------

