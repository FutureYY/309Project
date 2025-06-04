from raw_functions.distance import time_taken_to_deliver
from raw_functions.delivery_time import time_taken_to_deliver, flag_delivery_speed_relative
from raw_functions.distance import add_order_delivery_distance
from raw_functions.installment_flagging import add_high_installment_flag
from raw_functions.product_category import get_category_in_english, group_categories_by_sales_with_ohe
from raw_functions.repeat_buyers import finding_repeat_buyers, add_repeat_order_gaps

def build_final_dataset(
    df_orders,
    df_customers,
    df_installments,                # from add_high_installment_flag()
    df_category_price,              # from df_with_category_and_price()
    df_ohe,                         # from group_categories_by_sales()
    df_time,                        # from time_taken_to_deliver()
    df_flagged_speed,               # from flag_delivery_speed_relative()
    df_full,                        # from add_order_delivery_distance()
    customer_order_counts,          # from finding_repeat_buyers()
    df_order_reviews                # to get review score
):
    from pyspark.sql.functions import hour

    # --------------------------
    # Customer-level Features
    # --------------------------
    df_base = df_orders.select("order_id", "customer_id") \
        .join(df_customers.select("customer_id", "customer_unique_id"), on="customer_id", how="inner")


    # --------------------------
    # Delivery-related Features
    # --------------------------
    # join delivery_timing (delivered_in_days)
    df_base = df_base.join(df_time.select("order_id", "delivered_in_days", "purchase_hour", "month_of_purchase"), on="order_id", how="inner")

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
    # Product-level Features
    # --------------------------
    df_base = df_base.join(df_ohe.select("order_id", "category_grouped_ohe", "product_id"), on="order_id", how="inner")

    # --------------------------
    # Review Score
    # --------------------------
    df_final = df_base.join(df_order_reviews.select("order_id", "review_score"), on="order_id", how="inner")

    return df_final
