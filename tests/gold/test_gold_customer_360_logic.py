"""
Unit tests for gold_customer_360_logic.compute_customer_360.

This function is pure (no I/O, no dbutils, no current_date()), which is what
makes it possible to test in isolation: we fabricate small DataFrames and a
fixed as_of_date, run the function, and assert on the exact output — no real
tables, no cluster, no dependency on wall-clock time.

Test cases:
1. A customer with multiple current orders aggregates correctly
   (sum, count, max date).
2. A customer with zero matching orders is preserved (left join), with
   NULLs in the order-derived columns rather than being dropped.
3. SCD2 filtering: only rows where valid_to's year is 9999 (current version)
   are used, both for customers and for orders.
4. days_since_last_order is computed correctly relative to as_of_date, and
   is NULL when there's no last_order_date.
5. The join doesn't leave a duplicate customer_id column in the output.
"""

from datetime import date

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType,
)

from gold.gold_customer_360_logic import compute_customer_360


CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("valid_to", DateType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("order_date", DateType(), True),
    StructField("valid_to", DateType(), True),
])

CURRENT = date(9999, 12, 31)  # SCD2 sentinel for "current row"


def _row_by_customer_id(result_df, customer_id):
    rows = result_df.filter(result_df.customer_id == customer_id).collect()
    assert len(rows) == 1, f"expected exactly one row for {customer_id}, got {len(rows)}"
    return rows[0].asDict()


def test_customer_with_orders_aggregates_correctly(spark):
    customers_df = spark.createDataFrame([
        ("C1", "Alice", "Smith", "alice@example.com", "gold",
         "Austin", "TX", "US", date(2023, 1, 1), CURRENT),
    ], CUSTOMER_SCHEMA)

    orders_df = spark.createDataFrame([
        ("C1", "O1", 100.0, date(2024, 1, 10), CURRENT),
        ("C1", "O2", 50.0, date(2024, 2, 15), CURRENT),
    ], ORDER_SCHEMA)

    result_df = compute_customer_360(customers_df, orders_df, as_of_date=date(2024, 3, 1))
    row = _row_by_customer_id(result_df, "C1")

    assert row["lifetime_value"] == 150.0
    assert row["order_count"] == 2
    assert row["last_order_date"] == date(2024, 2, 15)
    assert row["days_since_last_order"] == 15  # Mar 1 - Feb 15


def test_customer_with_zero_orders_is_preserved_with_nulls(spark):
    customers_df = spark.createDataFrame([
        ("C2", "Bob", "Jones", "bob@example.com", "silver",
         "Denver", "CO", "US", date(2023, 5, 1), CURRENT),
    ], CUSTOMER_SCHEMA)

    # No orders at all for C2 (or anyone)
    orders_df = spark.createDataFrame([], ORDER_SCHEMA)

    result_df = compute_customer_360(customers_df, orders_df, as_of_date=date(2024, 3, 1))
    row = _row_by_customer_id(result_df, "C2")

    assert row["lifetime_value"] is None
    assert row["order_count"] is None
    assert row["last_order_date"] is None
    assert row["days_since_last_order"] is None


def test_scd2_filters_out_historical_rows(spark):
    customers_df = spark.createDataFrame([
        # historical version — should be ignored entirely
        ("C3", "Carla", "Old", "carla@old.com", "silver",
         "Miami", "FL", "US", date(2022, 1, 1), date(2023, 6, 1)),
        # current version — should be the only one used
        ("C3", "Carla", "New", "carla@new.com", "gold",
         "Miami", "FL", "US", date(2022, 1, 1), CURRENT),
    ], CUSTOMER_SCHEMA)

    orders_df = spark.createDataFrame([
        # historical order version — should be ignored
        ("C3", "O_old", 999.0, date(2023, 1, 1), date(2023, 5, 1)),
        # current order — should count
        ("C3", "O_new", 75.0, date(2024, 1, 5), CURRENT),
    ], ORDER_SCHEMA)

    result_df = compute_customer_360(customers_df, orders_df, as_of_date=date(2024, 2, 1))

    # only one row for C3 in the output — historical version dropped, not duplicated
    assert result_df.filter(result_df.customer_id == "C3").count() == 1

    row = _row_by_customer_id(result_df, "C3")
    assert row["last_name"] == "New"          # historical "Old" row excluded
    assert row["lifetime_value"] == 75.0      # historical order excluded from sum
    assert row["order_count"] == 1


def test_no_duplicate_customer_id_column_in_output(spark):
    customers_df = spark.createDataFrame([
        ("C4", "Dana", "Lee", "dana@example.com", "silver",
         "Boise", "ID", "US", date(2023, 3, 1), CURRENT),
    ], CUSTOMER_SCHEMA)

    orders_df = spark.createDataFrame([
        ("C4", "O1", 20.0, date(2024, 1, 1), CURRENT),
    ], ORDER_SCHEMA)

    result_df = compute_customer_360(customers_df, orders_df, as_of_date=date(2024, 1, 15))

    customer_id_cols = [c for c in result_df.columns if c == "customer_id"]
    assert len(customer_id_cols) == 1
