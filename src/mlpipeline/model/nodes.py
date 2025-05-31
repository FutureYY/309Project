import logging
import os
from typing import Any, Dict, Tuple
import pandas as pd
from  sklearn.model_selection import train_test_split
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report
import warnings
warnings.filterwarnings("ignore")

# Create a directory to save model
model_dir = "../saved_model"
model_path = os.path.join(model_dir, "best_model.pkl")
os.makedirs(model_dir, exist_ok=True)

#spliting the data into test and train datasets. 
def split_data(data: pd.DataFrame, parameters: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    data_train = data.sample(
        frac=parameters["train_fraction"], random_state=parameters["random_state"])
    
    # return X_train, X_test, y_train, y_test 
    
    
# Function to train and predictions using Random Forest Classifier    
def model_train_RFC(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series) -> pd.series:
    classifier_rfc = RandomForestClassifier()
    classifier_rfc.fit(X_train, y_train)
    y_pred_rfc = classifier_rfc.predict(X_test)
    
    return y_pred_rfc
    
# Function to train and make predictions using Binary Logistic Regression    
def model_train_BLR(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series) -> pd.series:
    classifier_blr = LogisticRegression()
    classifier_blr.fit(X_train, y_train)
    y_pred_blr = classifier_blr.predict(X_test)
    
    return y_pred_blr
    
#Function to train and make predictions using Gradient Boosting Classifier 
def model_train_GBC(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series) -> pd.series:
    classifier_gbc = GradientBoostingClassifier()
    classifier_gbc.fit(X_train, y_train)
    y_pred_gbc = classifier_gbc.predict(X_test)
    
    return y_pred_gbc

# Function to report the evalutation metrics of the models    
def report_evaluation(y_pred_rfc: pd.Series, y_pred_blr: pd.Series, 
                      y_pred_gbc: pd.Series, y_test: pd.Series,
                      X_test: pd.DataFrame):
    
    # Evalutation metrics -> Accuracy
    accuracy_rfc = accuracy_score(y_test, y_pred_rfc)
    accuracy_blr = accuracy_score(y_test, y_pred_blr)
    accuracy_gbc = accuracy_score(y_test, y_pred_gbc)
    
    # Evalutation metrics -> Recall
    recall_rfc = recall_score(y_test, y_pred_rfc)
    recall_blr = recall_score(y_test, y_pred_blr)
    recall_gbc = recall_score(y_test, y_pred_gbc)
    
    # Evalutation metrics -> Precision
    precision_rfc = precision_score(y_test, y_pred_rfc)
    precision_blr = precision_score(y_test, y_pred_blr)
    precision_gbc = precision_score(y_test, y_pred_gbc)
    
    # Evalutation metrics -> F1_score
    f1_score_rfc = f1_score(y_test, y_pred_rfc)
    f1_score_blr = f1_score(y_test, y_pred_blr)
    f1_score_gbc = f1_score(y_test, y_pred_gbc)
    
    # Evalutation metrics -> ROC-AUC
    ROC_AUC_rfc = 0
    ROC_AUC_blr = 0
    ROC_AUC_gbc = 0
     
    # the logger info here is to display the evaluations results of the models
    logger = logging.getlogger(__name__)
    logger.info(f"[RFC] Accuracy={accuracy_rfc:.3f}, Recall={recall_rfc:.3f}, Precision={precision_rfc:.3f}, F1_score={f1_score_rfc:.3f}, and ROC_AUC={ROC_AUC_rfc:.3f}")
    logger.info(f"[BLR] Accuracy={accuracy_blr:.3f}, Recall={recall_blr:.3f}, Precision={precision_blr:.3f}, F1_score={f1_score_blr:.3f}, and ROC_AUC={ROC_AUC_blr:.3f}")
    logger.info(f"[GBC] Accuracy={accuracy_gbc:.3f}, Recall={recall_gbc:.3f}, Precision={precision_gbc:.3f}, F1_score={f1_score_gbc:.3f}, and ROC_AUC={ROC_AUC_gbc:.3f}")
