import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import euclidean_distances
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import mysql.connector


# Function to calculate engagement and experience scores
def calculate_engagement_experience_scores(user_data, engagement_centroids, experience_centroids):
    # Fill NaNs with median values or remove them
    user_data = user_data[['Activity Duration DL (ms)', 'Activity Duration UL (ms)', 'Total DL (Bytes)', 'Total UL (Bytes)',
                           'Avg RTT DL (ms)', 'Avg RTT UL (ms)', 'Avg Bearer TP DL (kbps)', 'Avg Bearer TP UL (kbps)']].copy()


    # Fill NaNs with median of each column (recommended approach)
    user_data.fillna(user_data.median(), inplace=True)

    # Calculate engagement and experience scores
    engagement_score = euclidean_distances(user_data[['Activity Duration DL (ms)', 'Activity Duration UL (ms)', 'Total DL (Bytes)', 'Total UL (Bytes)']],
                                           [engagement_centroids[0]])
    experience_score = euclidean_distances(user_data[['Avg RTT DL (ms)', 'Avg RTT UL (ms)', 'Avg Bearer TP DL (kbps)', 'Avg Bearer TP UL (kbps)']],
                                           [experience_centroids[0]])

    return engagement_score.flatten(), experience_score.flatten()

# Function to build and train a regression model
def build_regression_model(X_train, y_train):
    # Create an imputer to replace missing values with the median
    imputer = SimpleImputer(strategy='median')
    
    # Impute the missing values in the feature set
    X_train_imputed = imputer.fit_transform(X_train)

    # Build the regression model
    model = LinearRegression()
    model.fit(X_train_imputed, y_train)
    
    return model

# Function to evaluate the regression model
def evaluate_regression_model(model, X_test, y_test):
    # Create an imputer to replace missing values with the median
    imputer = SimpleImputer(strategy='median')
    
    # Impute the missing values in the test set
    X_test_imputed = imputer.fit_transform(X_test)
    
    # Make predictions
    y_pred = model.predict(X_test_imputed)
    
    # Calculate RMSE
    mse = mean_squared_error(y_test, y_pred)
    return np.sqrt(mse)

# Function to perform K-means clustering
def kmeans_clustering(data, n_clusters):
    from sklearn.cluster import KMeans

    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    data['cluster'] = kmeans.fit_predict(data[['engagement_score', 'experience_score']])
    return data


