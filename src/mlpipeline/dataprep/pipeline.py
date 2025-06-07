from kedro.pipeline import Pipeline, node, pipeline
import os
from .nodes import target_dataset, flag_delivery_speed_relative, time_taken_to_deliver, add_high_installment_flag  
from .nodes import add_order_delivery_distance, finding_repeat_buyers, build_final_dataset


from pyspark.sql import SparkSession

def _create_spark_session():
    spark = SparkSession.builder \
        .appName("ML_App") \
        .master("local[*]") \
        .getOrCreate()
    return spark

SPARK_SESSION = _create_spark_session()

def create_pipeline(**kwargs) -> Pipeline:
    spark=SPARK_SESSION
    return pipeline(
        [   
            node(
                func=time_taken_to_deliver,
                inputs=["df_orders"],
                outputs="df_time",
                name="time_taken_to_deliver_node",
            ),
            
            node(
                func=flag_delivery_speed_relative,
                inputs=["df_time", 
                        "params:delivery_time_col"],
                outputs="df_flagged",
                name="flag_delivery_speed_flag_node",
            ),
            
            node(
                func=add_order_delivery_distance,
                inputs=["df_orders",
                        "df_order_items", 
                        "df_customers",
                        "df_sellers", 
                        "df_geolocation"],
                outputs="df_full",
                name="add_order_delivery_distance_node",
            ),
            
            node(
                func=add_high_installment_flag,
                inputs=["df_order_payment"],
                outputs="df_result",
                name="add_high_installment_flag_node",
            ),
            
            node(
                func=finding_repeat_buyers,
                inputs=["df_orders", 
                        "df_customers",
                        "df_order_items"],
                outputs="customer_order_counts",
                name="finding_repeat_buyers_node",
            ),

            node(
                func=build_final_dataset,
                inputs=["df_order_items", 
                        "df_products", 
                        "df_product_category",  
                        "df_orders", 
                        "df_customers", 
                        "df_sellers", 
                        "df_order_payment", 
                        "df_geolocation", 
                        "df_order_reviews"],
                outputs="processed_data",
                name="build_final_dataset_node",
            ),
            
            node(
                func=target_dataset,
                inputs=["processed_data"],
                outputs="target_data",
                name="target_dataset_node",
            )
        ]
    )