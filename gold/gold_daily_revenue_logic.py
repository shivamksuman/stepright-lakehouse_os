from datetime import date
from pyspark.sql import DataFrame, functions as F

def compute_daily_revenue_logic(df_src_orders_scd2: DataFrame, df_src_order_items: DataFrame, df_src_products_scd1: DataFrame, df_src_categories_scd1: DataFrame) -> DataFrame:


    order_subtotal_df = (df_src_order_items.groupBy(F.col("order_id"))
        .agg(F.round(F.sum(F.col("line_total")),2).alias("line_total_sum"))
       
    )


    df1_silver_orders_scd2 = (df_src_orders_scd2
                              .filter("year(valid_to)= '9999' and month(valid_to)= '12' and day(valid_to)='31'")
                              )
    

    join1 = (
    df_src_order_items.alias("items")
    .join(order_subtotal_df.alias("sub"), F.col("items.order_id") == F.col("sub.order_id"), "inner")
    .join(df1_silver_orders_scd2.alias("ord"), F.col("items.order_id") == F.col("ord.order_id"), "inner")
    .selectExpr("items.*", "sub.line_total_sum", "ord.discount_amount", "ord.order_date", "ord.shipping_country", "ord.shipping_state", "ord.shipping_city")
    )


    df3 = (
    join1
    .withColumn(
        "allocated_discount",
        F.round(
            F.when(
                F.col("line_total_sum") > 0,
                (F.col("line_total") / F.col("line_total_sum")) * F.coalesce(F.col("discount_amount"), F.lit(0.0))
            ).otherwise(F.lit(0.0)),
            2
        )
    )
    .select(
        "order_item_id", "order_id", "product_id", "sku", "quantity",
        "unit_price", "line_total", "line_total_sum", "discount_amount",
        "allocated_discount", "order_date", "shipping_country",
        "shipping_state", "shipping_city"
    )
    )

    df4 = (
    df3.alias("items")
    .join(df_src_products_scd1.select("product_id", "category_id").alias("prod"),
          "product_id", "inner")
    .join(df_src_categories_scd1.select("category_id", "category_name").alias("cat"),
          "category_id", "inner")
    )


    df5 = df4.withColumn("order_date", F.to_date(F.col("order_date")))


    gold_daily_revenue_df = (
    df5.groupBy(
        "order_date", "category_id", "category_name",
        "shipping_country", "shipping_state", "shipping_city"
    )
    .agg(
        F.round(F.sum("line_total"), 2).alias("gross_revenue"),
        F.round(F.sum("allocated_discount"), 2).alias("total_discount"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("total_units_sold"),
    )
    .withColumn("net_revenue", F.round(F.col("gross_revenue") - F.col("total_discount"), 2))
    )


    return gold_daily_revenue_df



