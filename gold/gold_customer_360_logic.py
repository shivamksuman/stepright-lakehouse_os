"""
Gold — gold_customer_360: pure transformation logic, no I/O.

Takes raw (unfiltered) customers_df and orders_df as input, filters both to
their current SCD2 row internally, aggregates orders per customer, and left
joins onto customers so zero-order customers are preserved (NULL lifetime_value
/order_count/last_order_date/days_since_last_order), never dropped.

No spark.read, no spark.write, no dbutils, no current_date() — this file only
defines a function. That's what makes it unit-testable in isolation: pass in a
fabricated small DataFrame and a fixed as_of_date, assert on the exact output,
no dependency on real tables or wall-clock time.
"""

from datetime import date
from pyspark.sql import DataFrame, functions as F


def compute_customer_360(customers_df: DataFrame, orders_df: DataFrame, as_of_date: date) -> DataFrame:

    # filter both sources to current row only (SCD2 current-version filter)
    customers_current = customers_df.filter(F.year("valid_to") == 9999)
    orders_current = orders_df.filter(F.year("valid_to") == 9999)

    orders_agg = (
        orders_current
        .groupBy(F.col("customer_id"))
        .agg(
            F.sum(F.col("total_amount")).alias("lifetime_value"),
            F.count(F.col("order_id")).alias("order_count"),
            F.max(F.col("order_date")).alias("last_order_date"),
        )
    )

    customers_selected = customers_current.selectExpr(
        "customer_id", "first_name", "last_name", "email",
        "loyalty_tier", "city", "state", "country", "registration_date"
    )

    joined_df = (
        customers_selected.alias("cust")
        .join(orders_agg.alias("ord"), F.col("cust.customer_id") == F.col("ord.customer_id"), "left")
        .drop(F.col("ord.customer_id")) 
    )

    result_df = joined_df.withColumn(
        "days_since_last_order",
        F.datediff(F.lit(as_of_date), F.col("last_order_date"))
    )

    return result_df