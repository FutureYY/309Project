import pandas as pd
from typing import Dict, Any, Tuple


dataset = pd.read_csv("data/processed_data.csv")

def target_dataset_A(): 
    target_A = dataset["installment_value", "High_installment_flag", "used_voucher", "category_grouped", "is_repeat_buyer"]
    target_A.to_csv("data/processed_data_A.csv", index=False)
    

def target_dataset_B():
    target_B = dataset["Delivered_in_days", "Delivery_speed_flag", "Delivery_distance_in_km", "purchase_hour", "is_repeat_buyer"]
    target_B.to_csv("data/processed_data_B.csv", index=False)    
    
def feature_engineering():
    print("hello")
    
    
    
    