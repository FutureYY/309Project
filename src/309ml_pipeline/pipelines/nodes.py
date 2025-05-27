import logging
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
from  sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
import warnings
warnings.filterwarnings("ignore")

# classifier for both binary_logistic regression and random forest clasification
classifier_1 = RandomForestClassifier()
classifier_2 = LogisticRegression()   

#spliting the data into test and train datasets. 
def split_data(data: pd.DataFrame, parameters: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    data_train = data.sample(
        frac=parameters["train_fraction"], random_state=parameters["random_state"])
    
    data_test = data.drop(data_train.index)    
    
    # X_train =
    # X_test = 
    # y_train =
    # y_test = 
    
    # return X_train, X_test, y_train, y_test 
    
# def make_prediction_RC(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series) -> pd.series:
    
    # X_train_numpy = X_train.to_numpy()
    # X_test_numpy = X_test.to_numpy()
    
    
# def make_prediction_BLR(X_train: pd.DataFrame, X_test: pd.DataFrame, y_train: pd.Series) -> pd.series:
    
    # X_train_numpy = X_train.to_numpy()
    # X_test_numpy = X_test.to_numpy()
    
    
    
def report_accuracy(y_pred: pd.Series, y_test: pd.Series):
    accuracy = (y_pred == y_test).sum() / len(y_test)
    logger = logging.getlogger(__name__)
    # the logger info here is to display the accuracy of the models on the kedro pipeline when run. 
    logger.info("The Random Forest classifer model has accurcy of %.3f on test data", accuracy)
    logger.info("The Binary Logistic Regression model has accurcy of %.3f on test data", accuracy)