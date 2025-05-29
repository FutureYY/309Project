import logging, os
from typing import Any, Dict, Tuple
import joblib
import numpy as np
import pandas as pd
from  sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
import warnings
warnings.filterwarnings("ignore")

# Create a directory to save model
model_dir = "../saved_model"
model_path = os.path.join(model_dir, "best_model.pkl")
os.makedirs(model_dir, exist_ok=True)


# classifier for both binary_logistic regression and random forest clasification
classifier_rfc = RandomForestClassifier()
classifier_blr = LogisticRegression()   


#Function to process the data

#spliting the data into test and train datasets. 
def split_data(data: pd.DataFrame, parameters: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    data_train = data.sample(
        frac=parameters["train_fraction"], random_state=parameters["random_state"])
    
    data_test = data.drop(data_train.index)    
    
    # X_train =
    X_test = 2
    # y_train =
    # y_test = 
    
    # return X_train, X_test, y_train, y_test 
    
    
# Function to make predictions using Random Forest Classifier    
def model_train_RFC(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series) -> pd.series:
    
    # X_train_numpy = X_train.to_numpy()
    X_test_numpy = X_test.to_numpy()
    
    # model_rfc = classifier_rfc.fit(X_train, y_train)
    # model_rfc.save(model_path)
    # print(f"Final model saved at: {model_path}")
    # joblib.dump(classifier_rfc, "classifier_rfc.jonlib")
    
# Function to make predictions using Binary Logistic Regression    
def model_train_BLR(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series) -> pd.series:
    
    # X_train_numpy = X_train.to_numpy()
    X_test_numpy = X_test.to_numpy()
    
    # model_blr = classifier_blr.
    # model_blr.save(model_path)
    # print(f"Final model saved at: {model_path}")
    # joblib.dump(classifier_blr, "classifier_blr.jonlib")
    
    
# Function to report the accuracy of the models    
def report_evaluation(y_pred: pd.Series, y_test: pd.Series, X_test: pd.DataFrame):
    accuracy_rfc = classifier_rfc.score(y_test, y_pred)
    accuracy_blr = classifier_blr.score(X_test, y_test)
    logger = logging.getlogger(__name__)
    # the logger info here is to display the accuracy of the models on the kedro pipeline when run. 
    logger.info("The Random Forest classifer model has accurcy of %.3f on test data", accuracy_rfc)
    logger.info("The Binary Logistic Regression model has accurcy of %.3f on test data", accuracy_blr)