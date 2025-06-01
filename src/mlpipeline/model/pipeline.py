from kedro.pipeline import Pipeline, node, pipeline

from .nodes import split_data_A, split_data_B 
from .nodes import model_train_RFC, model_train_BLR, model_train_GBC, report_evaluation

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=split_data_A,
                inputs=["processed_data_A", "parameters:split_data"],
                outputs=["X_train", "X_test", "y_train", "y_test"],
                name="split_data_A_node",
            ),
            node(
                func=split_data_B,
                inputs=["processed_data_B", "parameters:split_data"],
                outputs=["X_train", "X_test", "y_train", "y_test"],
                name="split_data_B_node",
            ),
            node(
                func=model_train_RFC,
                inputs=[["X_train_A", "X_test_A", "y_train_A"], "parameters:random_forest_params"],
                outputs="y_pred_rfc", 
                name="model_train_RFC_node",
                
            ),
            node(
                func=model_train_BLR,
                inputs=[["X_train_A", "X_test_A", "y_train_A"], "parameters:binary_logistic_params"],
                outputs="y_pred_blr",
                name="model_train_BLR_node",
            ),
            node(
                function=model_train_GBC,
                inputs=[["X_train_B", "X_test_B", "y_train_B"], "parameters:gradient_boosting_params"],
                outputs="y_pred_gbc",
                name="model_train_GBC_node",
            ),
            node(
                func=report_evaluation,
                inputs=["y_pred_gbc", "y_pred_blr", "y_pred_rfc", "y_test"],
                outputs=None,
                name="report_evaluation_node",
            ), 
        ]
    )

