import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px  # noqa:  F401
import plotly.graph_objs as go
import seaborn as sn
from .nodes import y_test, y_pred_rfc, y_pred_blr, y_pred_gbc
from sklearn.metrics import confusion_matrix
import seaborn as sns

def confusion_matrix_rfc():
    confusion_matrix_rfc = confusion_matrix(y_test, y_pred_rfc)
    
    sns.set_context('talk')
    ax = sns.heatmap(confusion_matrix, annot=True, fmt='d')
    labels = ['No', 'Yes']
    ax.set_xticklabels(labels)
    ax.set_yticklabels(labels)
    ax.set_ylabel('Actual')
    ax.set_xlabel('Predicted')
    
    return confusion_matrix_rfc
    
def confusion_matrix_blr():
    confusion_matrix_blr = confusion_matrix(y_test, y_pred_blr)    
    
    sns.set_context('talk')
    ax = sns.heatmap(confusion_matrix, annot=True, fmt='d')
    labels = ['No', 'Yes']
    ax.set_xticklabels(labels)
    ax.set_yticklabels(labels)
    ax.set_ylabel('Actual')
    ax.set_xlabel('Predicted')
    
    return confusion_matrix_blr
