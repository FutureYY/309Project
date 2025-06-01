from pyspark.sql.functions import count

# find repeated customer based of number of orders
# if number of order > 1, customer is counted as a repeat buyer

def finding_repeat_buyers(df_orders, df_customers):
    # Step 1: Join orders with customers to get customer_unique_id
    df_customer_order = df_orders.join(df_customers, on="customer_id", how="inner")

    df_customer_order_items = df_customer_order.join(df_order_items, on="order_id", how="inner")

    # count no. of orders per customer & total value of purchases
    customer_order_counts = df_customer_order_items.groupBy("customer_unique_id") \
        .agg(
            count("order_id").alias("num_orders"),
            round(sum("price"), 2).alias("total_purchase_value")
            )

    # Step 3: Add is_repeat_buyer flag
    customer_order_counts = customer_order_counts.withColumn(
        "is_repeat_buyer",
        (customer_order_counts["num_orders"] > 1).cast("integer")
    )

    return customer_order_counts