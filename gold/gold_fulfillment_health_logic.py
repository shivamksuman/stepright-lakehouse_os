from pyspark.sql import DataFrame, functions as F

def compute_fulfillment_health_logic(df_src_silver_orders_scd2: DataFrame, sla_days: int=5) -> DataFrame:

    df_src_orders_scd2_filtered = (
    df_src_silver_orders_scd2
    .filter("year(valid_to)='9999' and month(valid_to)='12' and day(valid_to)='31' and order_status='delivered'")
    .withColumn("days_to_deliver", F.datediff(F.to_date(F.col("updated_at")), F.to_date(F.col("order_date"))))
    .withColumn("within_sla", F.col("days_to_deliver") <= sla_days)
    )


    result_df = (
    df_src_orders_scd2_filtered
    .groupBy(
        F.to_date(F.col("order_date")).alias("order_date"),
        F.col("shipping_state").alias("region"),
        )
        .agg(
        F.count("order_id").alias("delivered_orders"),
        F.sum(F.when(F.col("within_sla"), 1).otherwise(0)).alias("within_sla_orders"),
        )
    .withColumn(
           "sla_compliance_rate",
        F.when(F.col("delivered_orders") > 0,
               F.round(F.col("within_sla_orders") / F.col("delivered_orders"), 4)
        ).otherwise(F.lit(0.0))
    )
    )


    return result_df


