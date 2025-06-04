from kedro.pipeline import Pipeline, node, pipeline

from .nodes import confusion_matrix_rfc, confusion_matrix_blr

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=confusion_matrix_rfc,
                inputs=["target_data"],
                outputs=["X_train"],
                name="confusion_matrix_rfc_node",
            ),
            
            node(
                func=confusion_matrix_blr,
                inputs=["X_train"],
                outputs=["y_pred_rfc"], 
                name="confusion_matrix_blr_node", 
            ), 
        ]
    )

