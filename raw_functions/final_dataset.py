from raw_functions.delivery_time import time_taken_to_deliver, flag_delivery_speed_relative
from raw_functions.distance import add_order_delivery_distance
from raw_functions.installment_flagging import add_high_installment_flag
from raw_functions.product_category import get_category_in_english, group_categories_by_sales_with_ohe
from raw_functions.repeat_buyers import finding_repeat_buyers

def build_final_dataset(
    df_order_items, df_products, df_product_category, df_orders,
    df_customers, df_sellers, df_order_payments, df_geolocation, df_order_reviews

):
    df_category_price = get_category_in_english(df_order_items, df_products, df_product_category)
    df_ohe = group_categories_by_sales_with_ohe(
    df_category_price,
    category_col="product_category_name_english",
    value_col="price",
    threshold=0.8
    )


    df_full = add_order_delivery_distance(df_orders, df_ohe, df_customers, df_sellers, df_geolocation, df_order_items)

    df_time = time_taken_to_deliver(df_orders)
    df_with_speed_flag = flag_delivery_speed_relative(df_time, delivery_time_col="delivered_in_days")

    df_installments = add_high_installment_flag(df_order_payments,
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
    # Product-level Features
    # --------------------------
    df_base = df_base.join(df_ohe.select("order_id", "category_grouped_ohe", "product_id"), on="order_id", how="inner")

    # --------------------------
    # Review Score
    # --------------------------
    df_final = df_base.join(df_order_reviews.select("order_id", "review_score"), on="order_id", how="inner")

    return df_final
