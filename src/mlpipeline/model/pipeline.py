from kedro.pipeline import Pipeline, node, pipeline

from .nodes import split_data
from .nodes import model_train_RFC, model_train_BLR, model_train_GBC, report_evaluation

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=split_data,
                inputs=["processed_data", 
                        "parameters:split_data"],
                outputs=["X_train", 
                         "X_test", 
                         "X_val", 
                         "y_train", 
                         "y_test", 
                         "y_val"],
                name="split_data_node",
            ),
            
            node(
                func=model_train_RFC,
                inputs=["X_train", 
                         "X_test", 
                         "y_train", 
                        "parameters:random_forest_params"],
                outputs=["y_pred_rfc",
                         "random_forest_model"], 
                name="model_train_RFC_node",
                
            ),
            node(
                func=model_train_BLR,
                inputs=["X_train", 
                        "X_test", 
                        "y_train_", 
                        "parameters:binary_logistic_params"],
                outputs=["y_pred_blr", 
                         "binary_logistic_model"],
                name="model_train_BLR_node",
            ),
            
            node(
                function=model_train_GBC,
                inputs=["X_train",
                         "X_test", 
                         "y_train", 
                        "parameters:gradient_boosting_params"],
                outputs=["y_pred_gbc", 
                         "gradient_boosting_model"],
                name="model_train_GBC_node",
            ),
            
            node(
                func=report_evaluation,
                inputs=["y_pred_rfc",
                        "y_pred_blr", 
                        "y_pred_gbc", 
                        "y_test", 
                        "X_test",
                        "gradient_boosting_model", 
                        "binary_logistic_model", 
                        "random_forest_model"],
                outputs=None,
                name="report_evaluation_node",
            ), 
        ]
    )

