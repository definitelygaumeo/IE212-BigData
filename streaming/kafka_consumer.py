# CVD_Prediction_System/streaming/kafka_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def create_spark_session(app_name="CVD_Consumer"):
    """
    Tạo Spark Session với các cấu hình cần thiết cho Kafka
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()
    
    return spark

def consume_from_kafka(spark, topic, bootstrap_servers="localhost:9092"):
    """
    Đọc dữ liệu từ Kafka bằng Spark Structured Streaming
    """
    # Định nghĩa schema cho dữ liệu
    schema = StructType([
        StructField("General_Health", StringType(), True),
        StructField("Checkup", StringType(), True),
        StructField("Exercise", StringType(), True),
        StructField("Heart_Disease", StringType(), True),
        StructField("Skin_Cancer", StringType(), True),
        StructField("Other_Cancer", StringType(), True),
        StructField("Depression", StringType(), True),
        StructField("Diabetes", StringType(), True),
        StructField("Arthritis", StringType(), True),
        StructField("Sex", StringType(), True),
        StructField("Age_Category", StringType(), True),
        StructField("Height_(cm)", DoubleType(), True),
        StructField("Weight_(kg)", DoubleType(), True),
        StructField("BMI", DoubleType(), True),
        StructField("Smoking_History", StringType(), True),
        StructField("Alcohol_Consumption", DoubleType(), True),
        StructField("Fruit_Consumption", DoubleType(), True),
        StructField("Green_Vegetables_Consumption", DoubleType(), True),
        StructField("FriedPotato_Consumption", DoubleType(), True)
    ])
    
    # Đọc từ Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()
    
    # Parse JSON từ Kafka
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    return parsed_df

if __name__ == "__main__":
    # Tạo Spark Session
    spark = create_spark_session()
    
    # Đọc dữ liệu từ Kafka
    topic = "cvd_topic"
    streaming_df = consume_from_kafka(spark, topic)
    
    # Hiển thị dữ liệu nhận được
    query = streaming_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    query.awaitTermination()