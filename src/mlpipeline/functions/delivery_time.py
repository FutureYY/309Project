from cleaned_data import df_orders

from pyspark.sql.functions import col, round, month, hour, avg, when

# calculate time for each order to be delivered, from order purchased to order delivered time
def time_taken_to_deliver(df):
  delivered_orders = df_orders.filter(df_orders.order_status == 'delivered')

  df_time = delivered_orders.withColumn('delivered_in_days', round((col('order_delivered_customer_date').cast('long') - col('order_purchase_timestamp').cast('long'))/86400))\
            .withColumn('month_of_purchase', month(col('order_purchase_timestamp')))\
            .withColumn('time_of_purchase', hour(col('order_purchase_timestamp')))

  df_time = df_time.select("order_id" , "order_purchase_timestamp", "order_delivered_customer_date", "time_of_purchase", "month_of_purchase", "delivered_in_days")

  return df_time

# sortds orders into fast, normal, slow delivery
# fast delivery = 1, normal dlivery = 2, slow delivery = 3
def flag_delivery_speed_flag(df, delivery_time_col="delivered_in_days"):
  
    avg_val = df.select(avg(col(delivery_time_col)).alias("avg_val")).collect()[0]["avg_val"]

    df_flagged = df.withColumn(
            "delivery_speed_flag",
            when(col(delivery_time_col) <= avg_val - 1, 1)      # Fast
            .when(col(delivery_time_col) <= avg_val + 1, 2)     # Normal
            .otherwise(3)                                       # Slow
        ).select("order_id", "delivered_in_days", "delivery_speed_flag")

    return df_flagged