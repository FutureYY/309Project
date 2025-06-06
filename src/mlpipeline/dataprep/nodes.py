import pandas as pd
from pyspark.sql.functions import col, round, month, hour, avg, when, radians, sin, cos, sum as spark_sum, atan2, sqrt, lower, trim, min, max, countDistinct, count, sum, lag, datediff
from pyspark.sql import DataFrame
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.master", 'local').getOrCreate()

# Extracting the important features [8] and target [1] needed for my model training. 
def target_dataset(processed_data: pd.DataFrame) -> pd.DataFrame: 
    target_data = processed_data[["installment_value", 
                                  "high_installment_flag",
                                  "used_voucher", 
                                  "category_grouped", 
                                  "delivered_in_days", 
                                  "delivery_speed_flag",
                                  "delivery_distance_in_km", 
                                  "purchase_hour", 
                                  "is_repeat_buyer"]]
    return target_data


# calculate time for each order to be delivered, from 'order_delivered_customer_date' to order_purchase_timestamp'
def time_taken_to_deliver(df_orders):

  # filter for orders that have 'delivered' status
  delivered_orders = df_orders.filter(df_orders.order_status == 'delivered')

  df_time = delivered_orders.withColumn('delivered_in_days', round((col('order_delivered_customer_date').cast('long') - col('order_purchase_timestamp').cast('long'))/86400))\
            .withColumn('month_of_purchase', month(col('order_purchase_timestamp')))\
            .withColumn('time_of_purchase', hour(col('order_purchase_timestamp')))

  df_time = df_time.select("order_id" , "order_purchase_timestamp", "order_delivered_customer_date", "time_of_purchase", "month_of_purchase", "delivered_in_days")

  return df_time


# sorts orders into fast, normal, slow delivery
def flag_delivery_speed_relative(df, delivery_time_col="delivered_in_days"):
    

    # takes data from "delivered_in_days" from delivery_timing
    # calculate the average days take to deliver and stores it in avg_days
    avg_days = df.select(avg(col(delivery_time_col)).alias("avg_val")).collect()[0]["avg_val"]

    # creates a new dataframe with delivery_speed_flag column
    # if "delivered_in_days" <= avg_days + 1, delivery_speed_flag == 2, indicating normal delivery
    # if "delivered_in_days" <= avg_days - 1, delivery_speed_flag == 1, indicating fast delivery
    # else delivery_speed_flag == 3, indicating slow delivery

    df_flagged_speed = df.withColumn(
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
def add_high_installment_flag(df_order_payments,
                               installment_col="payment_installments",
                               value_col="payment_value",
                               sequential_col="payment_sequential",
                               payment_type_col='payment_type',
                               header=True, inferSchema=True) -> DataFrame:
    """
    Adds:
    - 'installment_value': per-installment cost (with outliers capped)
    - 'high_installment_flag': binary flag based on avg thresholds
    - 'used_voucher': 1 if payment_type == 'voucher'
    """

    # Step 1: Flag voucher payments
    df = df_order_payments.withColumn("used_voucher", when(col(payment_type_col) == "voucher", 1).otherwise(0))

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


# get product category in english by combining with df_order_items
def get_category_in_english(df_order_items, df_products, df_product_category):

    df_products_clean = df_products.withColumn(
        "product_category_name",
        lower(trim("product_category_name"))
    )
    df_category_clean = df_product_category.withColumn(
        "product_category_name",
        lower(trim("product_category_name"))
    )

    df_products_english = df_products_clean.join(
        df_category_clean,
        on="product_category_name",
        how="left"
    )
    df_category_price = df_order_items.join(
        df_products_english,
        on="product_id",
        how="left"
    )

    df_category_price = df_category_price.select('product_id', 'order_id', 'order_item_id', 'seller_id', 'price', 'product_category_name_english')

    return df_category_price


# group categories that contribute little to the overall percentage sales as 'others'
# do one hot encoding on all the categories so that the model can process it
def group_categories_by_sales_with_ohe(df_category_price, category_col="product_category_name_english", value_col="price", threshold=0.8):

    # Step 1: Calculate total sales per category
    sales_per_category = df_category_price.groupBy(category_col) \
        .agg(spark_sum(value_col).alias("total_sales")) \
        .orderBy("total_sales", ascending=False)

    # Step 2: Convert to Pandas to calculate cumulative sales % and rank
    sales_pd = sales_per_category.toPandas()
    total_sales = sales_pd["total_sales"].sum()

    sales_pd["category_sales_rank"] = range(1, len(sales_pd) + 1)
    sales_pd["category_sales_percent"] = sales_pd["total_sales"] / total_sales
    sales_pd["cumulative_pct"] = sales_pd["category_sales_percent"].cumsum()

    # Step 3: Identify top categories
    top_categories = sales_pd[sales_pd["cumulative_pct"] <= threshold][category_col].tolist()

    # Step 4: Convert back to Spark
    spark =df_category_price.sparkSession
    sales_enriched = spark.createDataFrame(sales_pd)

    # Step 5: Join stats to original
    df_enriched = df_category_price.join(
        sales_enriched.select(
            category_col,
            "total_sales",
            "category_sales_rank",
            "category_sales_percent"
        ),
        on=category_col,
        how="left"
    )

    # Step 6: Add grouped label
    df_labeled = df_enriched.withColumn(
        "category_grouped",
        when(col(category_col).isin(top_categories), col(category_col)).otherwise("other")
    )

    # Step 7: One-hot encode the grouped category
    indexer = StringIndexer(inputCol="category_grouped", outputCol="category_grouped_index", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["category_grouped_index"], outputCols=["category_grouped_ohe"])
    pipeline = Pipeline(stages=[indexer, encoder])
    model = pipeline.fit(df_labeled)
    df_final = model.transform(df_labeled)

    df_final.select("product_id","product_category_name_english", "category_grouped", "category_grouped_ohe")

    return df_final


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
    
# build the final dataset [with the necessary features to catagorize] to be used for model training    
def build_final_dataset(
    df_orders,
    df_customers,
    df_installments,                # from add_high_installment_flag()
    df_category_price,              # from df_with_category_and_price()
    delivery_timing,                # from time_taken_to_deliver()
    df_flagged,                     # from flag_delivery_speed_relative()
    df_full,                        # from add_order_delivery_distance()
    customer_order_counts,           # from finding_repeat_buyers()
    df_order_reviews ) -> pd.DataFrame:

    # 1. combine df to get customer_unique_id and order_id
    df_base = df_orders.select("order_id", "customer_id") \
        .join(df_customers.select("customer_id", "customer_unique_id"), on="customer_id", how="left")

    # join delivery_timing (delivered_in_days)
    processed_data = df_base.join(delivery_timing.select("order_id", "delivered_in_days", "purchase_hour", "month_of_purchase"), on="order_id", how="left") \
                      .join(df_flagged.select("order_id", "delivery_speed_flag"), on="order_id", how="left") \
                      .join(df_full.select("order_id", "delivery_distance_in_km"), on="order_id", how="left") \
                      .join(df_installments.select("order_id", "installment_value", "high_installment_flag", "used_voucher"), on="order_id", how="left") \
                      .join(df_category_price.select("order_id", "category_grouped_ohe", "product_category_name_english", "product_id"), on="order_id", how="left") \
                      .join(customer_order_counts.select("customer_unique_id", "is_repeat_buyer", "num_orders", "total_purchase_value"), on="customer_unique_id", how="left") \
                      .join(df_order_reviews.select("order_id", "review_score"), on="order_id", how="left") 
                      
    return processed_data
