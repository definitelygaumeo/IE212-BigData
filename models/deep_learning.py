# CVD_Prediction_System/models/deep_learning.py
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from sklearn.model_selection import train_test_split
import numpy as np
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score

def create_deep_model(input_shape):
    """
    Create Deep Learning model using Keras
    """
    model = keras.Sequential([
        layers.Dense(64, activation='relu', input_shape=(input_shape,)),
        layers.Dense(32, activation='relu'),
        layers.Dense(1, activation='sigmoid')
    ])
    
    model.compile(optimizer='adam', 
                 loss='binary_crossentropy', 
                 metrics=['accuracy'])
    
    return model

def train_deep_learning(X, y, test_size=0.2, random_state=42, epochs=10, batch_size=32):
    """
    Train Deep Learning model
    """
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)
    
    model = create_deep_model(X_train.shape[1])
    
    history = model.fit(
        X_train, y_train,
        epochs=epochs,
        batch_size=batch_size,
        validation_split=0.1
    )
    
    loss, accuracy = model.evaluate(X_test, y_test)
    y_pred_prob = model.predict(X_test).ravel()
    y_pred = (y_pred_prob > 0.5).astype(int)
    auc = roc_auc_score(y_test, y_pred_prob)
    f1 = f1_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    metrics = {
        "loss": loss,
        "accuracy": accuracy,
        "auc": auc,
        "f1": f1,
        "precision": precision,
        "recall": recall
    }
    
    return model, history, metrics, (X_test, y_test)