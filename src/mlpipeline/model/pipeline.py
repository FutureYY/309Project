from kedro.pipeline import Pipeline, node, pipeline
from .nodes import split_data
from .nodes import model_train_RFC, model_train_BLR, report_evaluation

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node( # Node that splits the data into train, test, and validation sets
                func=split_data,
                inputs=["target_data", 
                        "params:split_data"],
                outputs=["X_train", 
                         "X_test", 
                         "X_val", 
                         "y_train", 
                         "y_test", 
                         "y_val"],
                name="split_data_node",
            ),
            
            node( # Node that trains the Random Forest Classifier and returns predictions and model. 
                func=model_train_RFC,
                inputs=["X_train", 
                         "X_test", 
                         "y_train", 
                        "params:random_forest_params"],
                outputs=["y_pred_rfc",
                         "best_model_rfc",
                         "random_forest_model"], 
                name="model_train_RFC_node",
            ),
            
            node( # Node that trains the Binary Classification Regression and returns predictions and model.
                func=model_train_BLR,
                inputs=["X_train", 
                        "X_test", 
                        "y_train", 
                        "params:binary_logistic_params",],
                outputs=["y_pred_blr", 
                         "best_model_blr",
                         "binary_logistic_model"],
                name="model_train_BLR_node",
            ),
            
            node( # Node that evaluates the models, and prints the evaluations [logger].
                func=report_evaluation,
                inputs=["y_pred_rfc",
                        "y_pred_blr", 
                        "y_test", 
                        "X_test",
                        "X_val",
                        "y_val",
                        "best_model_blr",
                        "best_model_rfc"], 
                outputs=None,
                name="report_evaluation_node",
            ), 
        ]
    )

