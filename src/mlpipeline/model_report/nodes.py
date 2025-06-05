import matplotlib.pyplot as plt
import pandas as pd
from sklearn.metrics import confusion_matrix
import seaborn as sns

def confusion_matrix_rfc(y_test: pd.Series, y_pred_rfc: pd.Series) -> tuple[plt.Figure, pd.DataFrame]:
    fig, ax = plt.subplots()
    confusion_rfc = confusion_matrix(y_test, y_pred_rfc)
    labels = ['No', 'Yes']
    cm_df_rfc = pd.DataFrame(confusion_rfc, index=labels, columns=labels)
    sns.heatmap(cm_df_rfc, annot=True, fmt='d', cmap='Blues', ax=ax)
    return fig, cm_df_rfc
    
def confusion_matrix_blr(y_test: pd.Series, y_pred_blr: pd.Series) -> tuple[plt.Figure, pd.DataFrame]:
    
    fig, ax = plt.subplots()
    confusion_blr = confusion_matrix(y_test, y_pred_blr)
    labels = ['No', 'Yes']
    cm_df_blr = pd.DataFrame(confusion_blr, index=labels, columns=labels)
    sns.heatmap(cm_df_blr, annot=True, fmt='d', cmap='Blues', ax=ax)
    return fig, cm_df_blr  


    