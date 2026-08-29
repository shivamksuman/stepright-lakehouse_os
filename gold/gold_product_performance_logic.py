from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def compute_product_performance_logic(df_src_silver_order_items, df_src_silver_inventory, df_src_silver_products_scd1):


    df_order_items = (df_src_silver_order_items.groupBy(F.col("product_id"))
                  .agg(
                      F.round(F.sum(F.col("quantity")),2).alias("units_sold"),
                      F.round(F.sum(F.col("line_total")),2).alias("revenue")
                      )
                  )
    

    window_spec= Window.partitionBy(F.col("product_id"), F.col("warehouse_id")).orderBy(F.col("snapshot_date").desc())

    dedup_silver_inventory = (df_src_silver_inventory.withColumn("rn", F.row_number().over(window_spec))
                          .filter("rn=1")
    )

    product_inventory_stock_agg= (dedup_silver_inventory.groupBy(F.col("product_id"))
                             .agg(
                                 F.round(F.sum(F.col("quantity_available")),2).alias("current_stock")
                             )
                             
    )


    df_products_with_sales = (
    df_src_silver_products_scd1.alias("l").join(df_order_items.alias("r"), F.col("l.product_id")==F.col("r.product_id"), "left_outer")
    .selectExpr("l.*", "r.units_sold", "r.revenue")
    )


    df_products_with_sales_with_stock_agg = (
        df_products_with_sales.alias("l").join(product_inventory_stock_agg.alias("r"), F.col("l.product_id")== F.col("r.product_id"), "left_outer")
        .selectExpr("l.*", "r.current_stock")
        ).withColumn("current_stock", F.coalesce(F.col("current_stock"), F.lit(0)))\
        .withColumn("stockout_risk", F.when(F.col("current_stock")<20, F.lit(True)).otherwise(F.lit(False)))\
        .selectExpr("product_id", "product_name", "brand", "units_sold", "revenue", "current_stock", "stockout_risk")



    return df_products_with_sales_with_stock_agg




