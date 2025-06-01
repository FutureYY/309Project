from pyspark.sql.functions import col, avg, radians, sin, cos, atan2, sqrt, when


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
