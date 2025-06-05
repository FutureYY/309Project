import logging
from typing import Any, Dict, Tuple
import pandas as pd
from sklearn.model_selection import StratifiedShuffleSplit, KFold, GridSearchCV
from sklearn.ensemble import RandomForestClassifier 
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
import warnings
warnings.filterwarnings("ignore")

# KF = KFold(n_splits=5, random_state=42, shuffle=True)

# Split dataset into train, test, and validation sets -> Statified Shuffle Split
def split_data(data: pd.DataFrame, parameters: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    target_column = parameters["target_column"]
    X = data.drop(columns=target_column)    
    y = data[target_column]

    # Split data into train and test first
    sss1 = StratifiedShuffleSplit(n_splits=1, train_size=parameters["train_fraction"], random_state=parameters["random_state"])
    for train_idx, test_idx in sss1.split(X, y):
        X_train = X.iloc[train_idx]
        X_test_old = X.iloc[test_idx] #named as X_test_old, to use X_test as final output, [more readable]
        
        y_train = y.iloc[train_idx]
        y_test_old = y.iloc[test_idx]
        
    # Split the 30% of test into test and validation   
    sss2 = StratifiedShuffleSplit(n_splits=1, test_size=0.5, random_state=parameters["random_state"])
    for test_idx, val_idx in sss2.split(X_test_old, y_test_old):
        X_test = X_test_old.iloc[test_idx]
        X_val = X_test_old.iloc[val_idx]
        
        y_test = y_test_old.iloc[test_idx]
        y_val = y_test_old.iloc[val_idx]
        
    return X_train, X_test, X_val, y_train, y_test, y_val
       
        
# Train and Predictions -> Random Forest Classifier    
def model_train_RFC(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series, random_forest_params: dict) -> pd.Series:
    classifier_rfc = GridSearchCV(RandomForestClassifier(**random_forest_params))
    random_forest_model = classifier_rfc.fit(X_train, y_train)
    
    y_pred_rfc = classifier_rfc.predict(X_test)
    return y_pred_rfc, random_forest_model
    
    
# Train and Predictions -> Binary Logistic Regression    
def model_train_BLR(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series, binary_logistic_params: dict) -> pd.Series:
    classifier_blr = GridSearchCV(LogisticRegression(**binary_logistic_params))
    binary_logistic_model = classifier_blr.fit(X_train, y_train)
    
    y_pred_blr = classifier_blr.predict(X_test)
    return y_pred_blr, binary_logistic_model


# Function to report the evalutation metrics of the models    
def report_evaluation(y_pred_rfc: pd.Series, y_pred_blr: pd.Series, 
                      y_test: pd.Series, X_test: pd.DataFrame, 
                      binary_logistic_model, random_forest_model):
    
    # Evalutation metrics -> Random Forest Classifier
    accuracy_rfc = accuracy_score(y_test, y_pred_rfc)
    ROC_AUC_rfc = roc_auc_score(y_test, random_forest_model.predict_proba(X_test)[:, 1])
    classification_report_rfc = classification_report(y_test, y_pred_rfc)

    # Evalutation metrics -> Binary Logistic Regression
    accuracy_blr = accuracy_score(y_test, y_pred_blr)
    ROC_AUC_blr = roc_auc_score(y_test, binary_logistic_model.predict_proba(X_test)[:, 1])
    classification_report_blr = classification_report(y_test, y_pred_blr)
     
    # the logger info here is to display the evaluations results of the models
    logger = logging.getLogger(__name__)
    logger.info(f"[Random Forest Classifier] Accuracy={accuracy_rfc:.3f}, ROC_AUC={ROC_AUC_rfc:.3f} and Classification_Report= {classification_report_rfc}")
    logger.info(f"[Binary Logistic Regression] Accuracy={accuracy_blr:.3f}, ROC_AUC={ROC_AUC_blr:.3f} and Classification_Report= {classification_report_blr}")
