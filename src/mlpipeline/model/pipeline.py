from kedro.pipeline import Pipeline, node, pipeline

from .nodes import split_data, model_train_RFC, model_train_BLR, model_train_GBC, report_evaluation
from .nodes import classifier_rfc, classifier_blr, classifier_gbc

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=split_data,
                inputs=["________________", "parameters"],
                outputs=["X_train", "X_test", "y_train", "y_test"],
                name="split_data_node",
            ),
            node(
                func=model_train_RFC,
                inputs=["_____________-", "parameters:random_forest_params"],
                outputs="y_pred", 
                name="model_train_RFC_node",
                
            ),
            node(
                func=model_train_BLR,
                inputs=["_____________-", "parameters:binary_logistic_params"],
                outputs="y_pred",
                name="model_train_BLR_node",
            ),
            node(
                function=model_train_GBC,
                inputs=["_____________-", "parameters:gradient_boosting_params"],
                outputs="y_pred",
                name="model_train_GBC_node",
            ),
            node(
                func=report_evaluation,
                inputs=["y_pred", "y_test"],
                outputs=None,
                name="report_evaluation_node",
            ), 
        ]
    )

