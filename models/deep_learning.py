# CVD_Prediction_System/models/deep_learning.py
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from sklearn.model_selection import train_test_split
import numpy as np

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
    
    metrics = {
        "loss": loss,
        "accuracy": accuracy
    }
    
    return model, history, metrics, (X_test, y_test)