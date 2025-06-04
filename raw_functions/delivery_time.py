from pyspark.sql.functions import col, round, month, hour, avg, when

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
def flag_delivery_speed_relative(df_time, delivery_time_col="delivered_in_days"):
    

    # takes data from "delivered_in_days" from delivery_timing
    # calculate the average days take to deliver and stores it in avg_days
    avg_days = df_time.select(avg(col(delivery_time_col)).alias("avg_val")).collect()[0]["avg_val"]

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