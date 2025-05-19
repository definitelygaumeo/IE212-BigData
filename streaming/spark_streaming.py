# CVD_Prediction_System/streaming/spark_streaming.py
import os
import sys

# Thêm thư mục cha vào sys.path để import từ modules khác
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)
sys.path.insert(0, project_dir)

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
import pyspark.sql.functions as F

from utils.data_processing import clean_data, feature_engineering
from streaming.kafka_consumer import consume_from_kafka

def create_spark_session(app_name="CVD_Streaming"):
    """
    Tạo Spark Session cho streaming
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()
    
    return spark

def process_streaming_data(streaming_df, pipeline_path, model_path):
    """
    Xử lý dữ liệu streaming và dự đoán với mô hình đã lưu
    """
    # Load pipeline và model
    pipeline = PipelineModel.load(pipeline_path)
    model = RandomForestClassificationModel.load(model_path)
    
    # Define processing function
    def process_batch(batch_df, batch_id):
        if batch_df.count() > 0:
            # Clean data
            cleaned_df = clean_data(batch_df)
            
            # Apply feature engineering
            featured_df = feature_engineering(cleaned_df)
            
            # Transform with pipeline
            transformed_df = pipeline.transform(featured_df)
            
            # Make predictions
            predictions = model.transform(transformed_df)
            
            # Save results
            predictions_to_save = predictions.select(
                "*", 
                F.col("prediction").alias("Predicted_Heart_Disease")
            )
            
            # Save to MongoDB (example)
            predictions_to_save.write \
                .format("mongo") \
                .mode("append") \
                .option("uri", "mongodb://localhost:27017/cvd_db.predictions") \
                .save()
            
            # Show results
            predictions_to_save.select("General_Health", "Age_Category", "BMI", 
                               "Heart_Disease", "Predicted_Heart_Disease").show(10)
    
    # Apply processing to streaming data
    streaming_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .start()

if __name__ == "__main__":
    # Tạo Spark Session
    spark = create_spark_session()
    
    # Path đến pipeline và model
    pipeline_path = os.path.join(project_dir, "models", "pipeline_model")
    model_path = os.path.join(project_dir, "models", "random_forest_model")
    
    # Đọc dữ liệu từ Kafka
    topic = "cvd_topic"
    streaming_df = consume_from_kafka(spark, topic)
    
    # Xử lý dữ liệu streaming
    process_streaming_data(streaming_df, pipeline_path, model_path)