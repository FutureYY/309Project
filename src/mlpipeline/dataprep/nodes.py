import pandas as pd
from pyspark.sql.functions import col, round, month, hour, avg, when, radians, sin, cos, sum as spark_sum, atan2, sqrt, lower, trim, min, max, countDistinct, count, sum, lag, datediff, to_timestamp
from pyspark.sql import DataFrame
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql import DataFrame as SparkDataFrame

# Extracting the important features [8] and target [1] needed for my model training. 
def target_dataset(processed_data: SparkDataFrame) -> pd.DataFrame: 
    processed_data = processed_data.toPandas()
    
    target_data = processed_data[["installment_value", 
                                  "high_installment_flag",
                                  "used_voucher",  
                                  "delivered_in_days", 
                                  "delivery_speed_flag",
                                  "delivery_distance_in_km", 
                                  "is_repeat_buyer"]]
    return target_data


# calculate time for each order to be delivered, from 'order_delivered_customer_date' to order_purchase_timestamp'
def time_taken_to_deliver(df_orders):

  # filter for orders that have 'delivered' status
    delivered_orders = df_orders.filter(df_orders.order_status == 'delivered')
    delivered_orders = delivered_orders.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
            .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
           .withColumn("order_approved_at", to_timestamp(col("order_approved_at"))) \
           .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))
    
    df_time = delivered_orders.withColumn('delivered_in_days', round((col('order_delivered_customer_date').cast('long') - col('order_purchase_timestamp').cast('long'))/86400))\
            .withColumn('month_of_purchase', month(col('order_purchase_timestamp')))\
            .withColumn('time_of_purchase', hour(col('order_purchase_timestamp')))
            
    df_time = df_time.withColumn('delivered_in_days', col('delivered_in_days').cast(DoubleType()))
    df_time = df_time.select("order_id" , "order_purchase_timestamp", "order_delivered_customer_date", "time_of_purchase", "month_of_purchase", "delivered_in_days")
    return df_time

#sorts orders into fast, normal, slow delivery
def flag_delivery_speed_relative(df_time, delivery_time_col="delivered_in_days"):
        
    df_time = df_time.withColumn(delivery_time_col, col(delivery_time_col).cast(DoubleType()))    
    # takes data from "delivered_in_days" from delivery_timing
    # calculate the average days take to deliver and stores it in avg_days
    avg_row = df_time.select(avg(col(delivery_time_col)).alias("avg_val")).collect()[0]
    avg_days = df_time.select(avg(col(delivery_time_col)).alias("avg_val")).collect()[0]["avg_val"]

    if avg_days is None:
        raise ValueError(f"The column `{delivery_time_col}` contains only nulls or no data.")

    # creates a new dataframe with delivery_speed_flag column
    # if "delivered_in_days" <= avg_days + 1, delivery_speed_flag == 2, indicating normal delivery
    # if "delivered_in_days" <= avg_days - 1, delivery_speed_flag == 1, indicating fast delivery
    # else delivery_speed_flag == 3, indicating slow delivery

    df_flagged_speed = df_time.withColumn(
            "delivery_speed_flag",
            when(col(delivery_time_col) <= avg_days - 1, 1)      # for fast delivery timing
            .when(col(delivery_time_col) <= avg_days + 1, 2)     # for normal delivery timing
            .otherwise(3)                                       # for slow delivery timing
        ).select("order_id", "delivered_in_days", "delivery_speed_flag")

    return df_flagged_speed

# calcualtes distance of customer from seller based on zip code given (in km)
# distance calculated using the haversine formula
def add_order_delivery_distance(df_orders, df_order_items, df_customers, df_sellers, df_geolocation):
    # Step 1: Aggregate average lat/long by zip code
    df_zip_avg_geo = df_geolocation.groupBy("geolocation_zip_code_prefix").agg(
        avg("geolocation_lat").alias("avg_lat"),
        avg("geolocation_lng").alias("avg_lng")
    )

    # Step 2: Join geolocation to customers
    df_customers_geo = df_customers.join(
        df_zip_avg_geo,
        df_customers["customer_zip_code_prefix"] == df_zip_avg_geo["geolocation_zip_code_prefix"],
        how="left"
    ).select(
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        col("avg_lat").alias("customer_latitude"),
        col("avg_lng").alias("customer_longitude")
    )

    # Step 3: Join geolocation to sellers
    df_sellers_geo = df_sellers.join(
        df_zip_avg_geo,
        df_sellers["seller_zip_code_prefix"] == df_zip_avg_geo["geolocation_zip_code_prefix"],
        how="left"
    ).select(
        "seller_id", "seller_zip_code_prefix",
        col("avg_lat").alias("seller_latitude"),
        col("avg_lng").alias("seller_longitude")
    )

    # Step 4: Combine order_items + orders + customer + seller
    df_full = df_order_items.join(
        df_orders.select("order_id", "customer_id"), on="order_id", how="left"
    ).join(
        df_customers_geo, on="customer_id", how="left"
    ).join(
        df_sellers_geo, on="seller_id", how="left"
    )

    # Step 5: Apply Haversine formula
    df_full = df_full.withColumn("cust_lat_rad", radians(col("customer_latitude"))) \
                     .withColumn("cust_lon_rad", radians(col("customer_longitude"))) \
                     .withColumn("sell_lat_rad", radians(col("seller_latitude"))) \
                     .withColumn("sell_lon_rad", radians(col("seller_longitude")))

    df_full = df_full.withColumn("delta_lat", col("sell_lat_rad") - col("cust_lat_rad")) \
                     .withColumn("delta_lon", col("sell_lon_rad") - col("cust_lon_rad"))

    a = sin(col("delta_lat") / 2) ** 2 + \
        cos(col("cust_lat_rad")) * cos(col("sell_lat_rad")) * sin(col("delta_lon") / 2) ** 2

    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    R_km = 6371.0  # Earth radius in kilometers

    df_full = df_full.withColumn("delivery_distance_in_km", round(R_km * c, 2))
    df_full.drop("cust_lat_rad", "cust_lon_rad", "sell_lat_rad", "sell_lon_rad", "delta_lat", "delta_lon")
    df_full = df_full.select("order_id", "customer_id", "seller_id", "delivery_distance_in_km")

    # Drop intermediate columns for cleanliness
    return df_full


# clip outliers from per installment value and assigns installment value to the max amount 
# flags installments with high value as 1, absed on the average threshold
# adds a column "used_voucher" if voucher is used as opne of the payment type
def add_high_installment_flag(df_order_payment: DataFrame,
                              installment_col="payment_installments",
                              value_col="payment_value",
                              sequential_col="payment_sequential",
                              payment_type_col='payment_type') -> DataFrame:
    """
    Adds:
    - 'installment_value': per-installment cost (with outliers capped)
    - 'high_installment_flag': binary flag based on avg thresholds
    - 'used_voucher': 1 if payment_type == 'voucher'
    """

    # Step 1: Flag voucher payments
    df = df_order_payment.withColumn("used_voucher", when(col(payment_type_col) == "voucher", 1).otherwise(0))

    # Step 2: Calculate raw installment value
    df = df.withColumn(
        "installment_value",
        round(when(col(installment_col) > 0, col(value_col) / col(installment_col)).otherwise(0), 2)
    )

    # Step 3: Compute IQR bounds for capping
    # Using approxQuantile for performance on large data
    iqr_bounds = {}
    for field in ["installment_value", installment_col]:
        q1, q3 = df.approxQuantile(field, [0.25, 0.75], 0.05)
        iqr = q3 - q1
        iqr_bounds[field] = {
            "lower": q1 - 1.5 * iqr,
            "upper": q3 + 1.5 * iqr
        }

    # Step 4: Cap outliers based on IQR
    df = df.withColumn(
        "installment_value_capped",
        when(col("installment_value") < iqr_bounds["installment_value"]["lower"], iqr_bounds["installment_value"]["lower"])
        .when(col("installment_value") > iqr_bounds["installment_value"]["upper"], iqr_bounds["installment_value"]["upper"])
        .otherwise(col("installment_value"))
    ).withColumn(
        "payment_installments_capped",
        when(col(installment_col) < iqr_bounds[installment_col]["lower"], iqr_bounds[installment_col]["lower"])
        .when(col(installment_col) > iqr_bounds[installment_col]["upper"], iqr_bounds[installment_col]["upper"])
        .otherwise(col(installment_col))
    )

    # Step 5: Recalculate averages (exclude vouchers and zeros)
    df_valid = df.filter((col(value_col) > 0) & (col("used_voucher") == 0))
    averages = df_valid.select(
        avg("payment_installments_capped").alias("avg_installments"),
        avg("installment_value_capped").alias("avg_installment_value")
    ).first()

    avg_installments = averages["avg_installments"]
    avg_installment_value = averages["avg_installment_value"]

    # Step 6: Assign high installment flag using capped values
    df_result = df.withColumn(
        "high_installment_flag",
        when(col(sequential_col) == 0, 0)
        .when(
            (col("payment_installments_capped") >= avg_installments) |
            (col("installment_value_capped") <= avg_installment_value),
            1
        ).otherwise(0)
    )

    df_result = df_result.select("order_id", "payment_type", "payment_sequential", "payment_value", "payment_installments", "installment_value", "installment_value_capped", "high_installment_flag", "used_voucher")

    return df_result 


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
    
def build_final_dataset(
    df_order_items, df_products, df_product_category, df_orders,
    df_customers, df_sellers, df_order_payment, df_geolocation, df_order_reviews ):


    df_full = add_order_delivery_distance(df_orders, df_order_items, df_customers, df_sellers, df_geolocation)

    df_time = time_taken_to_deliver(df_orders)
    df_with_speed_flag = flag_delivery_speed_relative(df_time, delivery_time_col="delivered_in_days")

    df_installments = add_high_installment_flag(df_order_payment,
                               installment_col="payment_installments",
                               value_col="payment_value",
                               sequential_col="payment_sequential",
                               payment_type_col="payment_type")
    customer_order_counts = finding_repeat_buyers(df_orders, df_customers, df_order_items)
    df_flagged_speed = flag_delivery_speed_relative(df_time, delivery_time_col="delivered_in_days")
    # --------------------------
    # Customer-level Features
    # --------------------------
    df_base = df_orders.select("order_id", "customer_id") \
        .join(df_customers.select("customer_id", "customer_unique_id"), on="customer_id", how="inner")

    # --------------------------
    # Delivery-related Features
    # --------------------------
    # join delivery_timing (delivered_in_days)
    df_base = df_base.join(df_time.select("order_id", "delivered_in_days", "time_of_purchase", "month_of_purchase"), on="order_id", how="inner")

    # join delivery_speed_flag
    df_base = df_base.join(df_flagged_speed.select("order_id", "delivery_speed_flag"), on="order_id", how="inner")

    # join delivery_distance_km
    df_base = df_base.join(df_full.select("order_id", "delivery_distance_in_km"), on="order_id", how="inner")

    # --------------------------
    # Payment-related Features
    # --------------------------
    df_base = df_base.join(df_installments.select("order_id", "installment_value", "high_installment_flag", "used_voucher"), on="order_id", how="inner")

    # join is_repeat_buyer flag
    df_base = df_base.join(customer_order_counts.select("customer_unique_id", "is_repeat_buyer", "num_orders", "total_purchase_value"), on="customer_unique_id", how="inner")

    # --------------------------
    # Review Score
    # --------------------------
    df_final = df_base.join(df_order_reviews.select("order_id", "review_score"), on="order_id", how="inner")

    return df_final