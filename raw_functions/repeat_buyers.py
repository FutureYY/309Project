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