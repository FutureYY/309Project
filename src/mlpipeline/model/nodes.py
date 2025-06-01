import logging, os
from typing import Any, Dict, Tuple
import pandas as pd
from  sklearn.model_selection import train_test_split
from sklearn.model_selection import StratifiedShuffleSplit, KFold
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
import warnings
import joblib
warnings.filterwarnings("ignore")

classifier_rfc = RandomForestClassifier()
classifier_blr = LogisticRegression()
classifier_gbc = GradientBoostingClassifier()

# Create a directory to save model
model_dir = "../saved_model"
model_path = os.path.join(model_dir, "best_model.pkl")
os.makedirs(model_dir, exist_ok=True)

KF = KFold(n_splits=5, random_state=42, shuffle=True)

# Split dataset -> First Assumption 
def split_data_A(data: pd.DataFrame, parameters: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    data_train_A = data.sample(
        frac=parameters["train_fraction"], random_state=parameters["random_state"])
    
    data_test_A = data.drop(data_train_A.index)
    X_train_A = data_train_A.drop(columns=parameters["target_column"])
    X_test_A = data_test_A.drop(columns=parameters["target_column"])
    y_train_A = data_train_A[parameters["target_column"]]
    y_test_A = data_test_A[parameters["target_column"]]

    return X_train_A, X_test_A, y_train_A, y_test_A 

# Split dataset -> Second Assumption  
def split_data_B(data: pd.DataFrame, parameters: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    data_train_B = data.sample(
        frac=parameters["train_fraction"], random_state=parameters["random_state"])
    
    data_test_B = data.drop(data_train_B.index)
    X_train_B = data_train_B.drop(columns=parameters["target_column"])
    X_test_B = data_test_B.drop(columns=parameters["target_column"])
    y_train_B = data_train_B[parameters["target_column"]]
    y_test_B = data_test_B[parameters["target_column"]]

    return X_train_B, X_test_B, y_train_B, y_test_B 
    
# Train and Predictions -> Random Forest Classifier    
def model_train_RFC(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series) -> pd.Series:
    classifier_rfc.fit(X_train, y_train)
    
    joblib.dump(classifier_rfc, os.path.join(model_dir, "random_forest.pkl"))    
    
    y_pred_rfc = classifier_rfc.predict(X_test)
    
    return y_pred_rfc
    
# Train and Predictions -> Binary Logistic Regression    
def model_train_BLR(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series) -> pd.Series:
    classifier_blr.fit(X_train, y_train)
    
    joblib.dump(classifier_blr, os.path.join(model_dir, "logistic_regression.pkl"))
    
    y_pred_blr = classifier_blr.predict(X_test)
    
    return y_pred_blr
    
# Train and Predictions -> Gradient Boosting Classifier 
def model_train_GBC(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series) -> pd.Series:
    classifier_gbc.fit(X_train, y_train)
    
    joblib.dump(classifier_gbc, os.path.join(model_dir, "gradient_boosting.pkl"))
    
    y_pred_gbc = classifier_gbc.predict(X_test)
    
    return y_pred_gbc

# Function to report the evalutation metrics of the models    
def report_evaluation(y_pred_rfc: pd.Series, y_pred_blr: pd.Series, 
                      y_pred_gbc: pd.Series, y_test_A: pd.Series, 
                      y_test_B: pd.Series, X_test_A: pd.DataFrame,
                      X_test_B: pd.DataFrame):
    
    # Evalutation metrics -> Random Forest Classifier
    accuracy_rfc = accuracy_score(y_test_A, y_pred_rfc)
    ROC_AUC_rfc = roc_auc_score(y_test_A, classifier_rfc.predict_proba(X_test_A)[:, 1])
    classification_report_rfc = classification_report(y_test_A, y_pred_rfc)

    # Evalutation metrics -> Binary Logistic Regression
    accuracy_blr = accuracy_score(y_test_A, y_pred_blr)
    ROC_AUC_blr = roc_auc_score(y_test_A, classifier_blr.predict_proba(X_test_A)[:, 1])
    classification_report_blr = classification_report(y_test_A, y_pred_blr)

    # Evalutation metrics -> Gradient Boosting Classifier  
    accuracy_gbc = accuracy_score(y_test_B, y_pred_gbc)
    ROC_AUC_gbc = roc_auc_score(y_test_B, classifier_gbc.predict_proba(X_test_B)[:, 1])
    classification_report_gbc = classification_report(y_test_B, y_pred_gbc)
     
    # the logger info here is to display the evaluations results of the models
    logger = logging.getLogger(__name__)
    logger.info(f"[Random Forest Classifier] Accuracy={accuracy_rfc:.3f}, ROC_AUC={ROC_AUC_rfc:.3f} and Classification_Report= {classification_report_rfc}")
    logger.info(f"[Binary Logistic Regression] Accuracy={accuracy_blr:.3f}, ROC_AUC={ROC_AUC_blr:.3f} and Classification_Report= {classification_report_blr}")
    logger.info(f"[Gradient Boosting Classifier] Accuracy={accuracy_gbc:.3f}, ROC_AUC={ROC_AUC_gbc:.3f} and Classification_Report= {classification_report_gbc}")

