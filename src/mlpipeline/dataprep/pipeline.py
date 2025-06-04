from kedro.pipeline import Pipeline, node, pipeline

from .nodes import target_dataset, flag_delivery_speed_flag, time_taken_to_deliver, add_order_delivery_distance, add_high_installment_flag 
from .nodes import get_category_in_english, group_categories_by_sales_with_ohe, finding_repeat_buyers, build_final_dataset

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [   
            node(
                func=time_taken_to_deliver,
                inputs=["df_orders"],
                outputs=["df_time"],
                name="time_taken_to_deliver_node",
            ),
            
            node(
                func=flag_delivery_speed_flag,
                inputs=["df_time", 
                        "params:delivery_time_col"],
                outputs=["df_flagged"],
                name="flag_delivery_speed_flag_node",
            ),
            
            node(
                func=add_order_delivery_distance,
                inputs=["df_orders",
                        "df_order_items", 
                        "df_customers",
                        "df_sellers", 
                        "df_geolocation"],
                outputs=["df_full"],
                name="add_order_delivery_distance_node",
            ),
            
            node(
                func=add_high_installment_flag,
                inputs=["df_order_payments"],
                outputs=["df_result"],
                name="add_high_installment_flag_node",
            ),
            
            node(
                func=get_category_in_english,
                inputs=["df_order_items", 
                        "df_products", 
                        "df_product_category"],
                outputs=["df_category_price"],
                name="get_category_in_english_node",
            ),
            
            node(
                func=group_categories_by_sales_with_ohe,
                inputs=["df_category_price"],
                outputs=["df_final"],
                name="group_categories_by_sales_with_ohe_node",
            ),
            
            node(
                func=finding_repeat_buyers,
                inputs=["df_orders", 
                        "df_customers",
                        "df_order_items"],
                outputs=["customer_order_counts"],
                name="finding_repeat_buyers_node",
            ),
            
            node(
                func=build_final_dataset,
                inputs=["df_orders", 
                        "df_customers", 
                        "df_installments", 
                        "df_category_price", 
                        "delivery_timing", 
                        "df_flagged", 
                        "df_full", 
                        "customer_order_counts", 
                        "df_order_reviews"],
                outputs=["processed_data"],
                name="build_final_dataset_node",
            ),
            
            node(
                func=target_dataset,
                inputs=["processed_data"],
                outputs=["target_data"],
                name="target_dataset_node",
            )
        ]
    )