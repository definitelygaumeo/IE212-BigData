# CVD_Prediction_System/models/random_forest.py
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import pandas as pd

def train_random_forest(train_data, test_data, feature_col="scaledFeatures", label_col="label"):
    """
    Train Random Forest model
    """
    rf = RandomForestClassifier(featuresCol=feature_col, labelCol=label_col, numTrees=100)
    model = rf.fit(train_data)
    predictions = model.transform(test_data)
    
    # Evaluate
    evaluator = BinaryClassificationEvaluator(labelCol=label_col)
    auc = evaluator.evaluate(predictions)
    
    evaluator_acc = MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator_acc.evaluate(predictions)
    
    metrics = {
        "auc": auc,
        "accuracy": accuracy
    }
    
    # Feature importance
    importances = model.featureImportances.toArray()
    
    return model, predictions, metrics, importances