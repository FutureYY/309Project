from kedro.pipeline import Pipeline, node, pipeline

from .nodes import split_data, model_train_RFC, model_train_BLR, report_evaluation
from .nodes import classifier_rfc, classifier_blr


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=split_data,
                #inputs=
                #outputs=
                name="split_data_node",
            ),
            node(
                func=model_train_RFC,
                #inputs=
                #outputs= 
                name="model_train_RFC_node",
                
            ),
            node(
                func=model_train_BLR,
                #inputs=
                #outputs= 
                name="model_train_BLR_node",
            ),
            node(
                func=report_evaluation,
                #inputs=["y_pred", "y_test", "X_test"],
                #outputs="evaluation_report",
                name="report_evaluation_node",
            ), 
        ]
    )

