from pyspark.sql.functions import countDistinct, sum, round, col

# find repeated customer based of number of orders
# if number of order > 1, customer is counted as a repeat buyer

def finding_repeat_buyers(df_orders, df_customers, df_order_items):
    """
    Joins orders with customers and items, then returns:
    - customer_unique_id
    - number of unique orders
    - total purchase value
    - is_repeat_buyer (1 if >1 order, else 0)
    """

    # Step 1: Join orders with customers to get customer_unique_id
    df_customer_order = df_orders.join(df_customers, on="customer_id", how="inner")

    # Step 2: Join with order_items to get price info
    df_customer_order_items = df_customer_order.join(df_order_items, on="order_id", how="inner")

    # Step 3: Aggregate order count and purchase value
    customer_order_counts = df_customer_order_items.groupBy("customer_unique_id") \
        .agg(
            countDistinct("order_id").alias("num_orders"),
            round(sum("price"), 2).alias("total_purchase_value")
        )

    # Step 4: Add repeat buyer flag
    customer_order_counts = customer_order_counts.withColumn(
        "is_repeat_buyer",
        (col("num_orders") > 1).cast("integer")
    )

    return customer_order_counts

from pyspark.sql.functions import col, lag, datediff, when
from pyspark.sql.window import Window

def add_repeat_order_gaps(df_orders, df_customers, customer_order_counts):
    """
    For repeat buyers, calculate time between current order and previous order.
    Sets null gaps to 1 (for models that only accept integers).

    Parameters:
        df_orders: orders DataFrame with order_id, customer_id, order_purchase_timestamp
        df_customers: customers DataFrame with customer_id and customer_unique_id
        customer_order_counts: output from finding_repeat_buyers()

    Returns:
        DataFrame with:
        - customer_unique_id
        - order_id
        - order_purchase_timestamp
        - prev_order_date
        - days_since_last_order (int, with null → 1)
        - is_first_order (1 if null before, else 0)
    """

    # Step 1: Join orders with customer_unique_id
    df_joined = df_orders.join(df_customers, on="customer_id", how="inner")

    # Step 2: Add repeat buyer flag
    df_repeat_flag = df_joined.join(
        customer_order_counts.select("customer_unique_id", "is_repeat_buyer"),
        on="customer_unique_id", how="left"
    )

    # Step 3: Filter to repeat buyers
    df_repeat_only = df_repeat_flag.filter(col("is_repeat_buyer") == 1)

    # Step 4: Define window for previous order
    window_spec = Window.partitionBy("customer_unique_id").orderBy("order_purchase_timestamp")

    # Step 5: Add previous date and calculate gap
    df_with_gaps = df_repeat_only.withColumn(
        "prev_order_date", lag("order_purchase_timestamp").over(window_spec)
    ).withColumn(
        "raw_days_since_last_order",
        datediff("order_purchase_timestamp", "prev_order_date")
    )

    # Step 6: Handle nulls → 1 and add is_first_order flag
    df_with_flags = df_with_gaps.withColumn(
        "days_since_last_order",
        when(col("raw_days_since_last_order").isNull(), 1).otherwise(col("raw_days_since_last_order"))
    ).withColumn(
        "is_first_order",
        when(col("raw_days_since_last_order").isNull(), 1).otherwise(0)
    )

    df_with_flags.select(
        "customer_unique_id",
        "order_id",
        "order_purchase_timestamp",
        "prev_order_date",
        "days_since_last_order",
        "is_first_order"
    )

    return df_with_flags
