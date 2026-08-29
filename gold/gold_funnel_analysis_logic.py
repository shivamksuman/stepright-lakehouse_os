from pyspark.sql import DataFrame, functions as F

def compute_funnel_analysis_logic(df_src_silver_clickstream):

    df_src_silver_clickstream_coalesce_referrer = (
        df_src_silver_clickstream.withColumn("referrer", F.coalesce(F.col("referrer"), F.lit("direct")))
        .groupBy(F.col("session_id"), F.col("referrer"))
        .agg(
            F.expr("MAX(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END)").alias("converted")
        )
    )
    


    df_groupedBy_referrer = (
        df_src_silver_clickstream_coalesce_referrer.groupBy(F.col("referrer"))
            .agg(
                F.count(F.col("session_id")).alias("total_sessions"), 
                F.sum(F.col("converted")).alias("converted_sessions")
                )
            .withColumn("conversion_rate_percentage", F.when(F.col("total_sessions")>0, F.round((F.col("converted_sessions")/F.col("total_sessions"))*100.0, 2)).otherwise(F.lit(0))
                        )
    )

    return df_groupedBy_referrer