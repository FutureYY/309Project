import logging
from typing import Any, Dict, Tuple
import pandas as pd
from sklearn.model_selection import StratifiedShuffleSplit, StratifiedKFold, GridSearchCV
from sklearn.ensemble import RandomForestClassifier 
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
import warnings
warnings.filterwarnings("ignore")

# KF = KFold(n_splits=5, random_state=42, shuffle=True)

# Split dataset into train, test, and validation sets -> Statified Shuffle Split
def split_data(data: pd.DataFrame, parameters: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    data = data.dropna()
    target_column = parameters["target_column"]
    X = data.drop(columns=[target_column])    
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
    model_params = random_forest_params["model_params_rfc"]
    param_grid = random_forest_params["param_grid_rfc"]
    classifier_rfc = GridSearchCV(
        estimator=RandomForestClassifier(**model_params),
        param_grid=param_grid,
        error_score='raise',
        cv=StratifiedKFold(n_splits=5))
    random_forest_model = classifier_rfc.fit(X_train, y_train)
    
    y_pred_rfc = random_forest_model.predict(X_test)
    best_model_rfc = classifier_rfc.best_estimator_
    return y_pred_rfc, best_model_rfc, random_forest_model
    
    
# Train and Predictions -> Binary Logistic Regression    
def model_train_BLR(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series, binary_logistic_params: dict) -> pd.Series:
    model_params = binary_logistic_params["model_params_blr"]
    param_grid = binary_logistic_params["param_grid_blr"]
    classifier_blr = GridSearchCV(
        estimator=LogisticRegression(**model_params),
        param_grid=param_grid,
        error_score='raise',
        scoring='roc_auc')
    binary_logistic_model = classifier_blr.fit(X_train, y_train)
    
    y_pred_blr = binary_logistic_model.predict(X_test)
    best_model_blr = classifier_blr.best_estimator_
    return y_pred_blr, best_model_blr, binary_logistic_model


# Function to report the evalutation metrics of the models    
def report_evaluation(y_pred_rfc: pd.Series, 
                      y_pred_blr: pd.Series, 
                      y_test: pd.Series, 
                      X_test: pd.DataFrame, 
                      X_val: pd.DataFrame, 
                      y_val: pd.Series,
                      best_model_blr, 
                      best_model_rfc):
    
    # Evalutation metrics -> Random Forest Classifier
    accuracy_rfc = accuracy_score(y_test, y_pred_rfc)
    ROC_AUC_rfc = roc_auc_score(y_test, best_model_rfc.predict_proba(X_test)[:, 1])
    classification_report_rfc = classification_report(y_test, y_pred_rfc)
    y_pred_val_rfc = best_model_rfc.predict(X_val)
    accuracy_val_rfc = accuracy_score(y_val, y_pred_val_rfc)
    
    # Evalutation metrics -> Binary Logistic Regression
    accuracy_blr = accuracy_score(y_test, y_pred_blr)
    ROC_AUC_blr = roc_auc_score(y_test, best_model_blr.predict_proba(X_test)[:, 1])
    classification_report_blr = classification_report(y_test, y_pred_blr)
    
    y_pred_val_blr = best_model_blr.predict(X_val)
    accuracy_val_blr = accuracy_score(y_val, y_pred_val_blr)
        
    # the logger info here is to display the evaluations results of the models
    logger = logging.getLogger(__name__)
    logger.info("=========== Evaluation Results of Random Forest Classifier ===========")
    logger.info(f"[Random Forest Classifier] Accuracy={accuracy_rfc:.3f}, ROC_AUC={ROC_AUC_rfc:.3f} and Classification_Report= {classification_report_rfc}")
    logger.info(f"[Random Forest Classifier] Validation Accuracy={accuracy_val_rfc:.3f}")
    
    logger.info("=========== Evaluation Results of Random Forest Classifier ===========")
    logger.info(f"[Binary Logistic Regression] Accuracy={accuracy_blr:.3f}, ROC_AUC={ROC_AUC_blr:.3f} and Classification_Report= {classification_report_blr}")
    logger.info(f"[Binary Logistic Regression] Validation Accuracy={accuracy_val_blr:.3f}")