# CVD_Prediction_System/models/logistic_regression.py
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

def train_logistic_regression(train_data, test_data, feature_col="scaledFeatures", label_col="label"):
    """
    Train Logistic Regression model
    """
    lr = LogisticRegression(featuresCol=feature_col, labelCol=label_col, maxIter=20)
    model = lr.fit(train_data)
    predictions = model.transform(test_data)
    
    # Evaluate
    evaluator = BinaryClassificationEvaluator(labelCol=label_col)
    auc = evaluator.evaluate(predictions)
    
    evaluator_acc = MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator_acc.evaluate(predictions)
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="f1")
    f1 = evaluator_f1.evaluate(predictions)
    evaluator_precision = MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="weightedPrecision")
    precision = evaluator_precision.evaluate(predictions)
    evaluator_recall = MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction", metricName="weightedRecall")
    recall = evaluator_recall.evaluate(predictions)
    
    metrics = {
        "auc": auc,
        "accuracy": accuracy,
        "f1": f1,
        "precision": precision,
        "recall": recall
    }
    
    return model, predictions, metrics