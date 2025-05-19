# CVD_Prediction_System/streaming/kafka_producer.py
from kafka import KafkaProducer
import json
import pandas as pd
import time
import os

def create_kafka_producer(bootstrap_servers=["localhost:9092"]):
    """
    Tạo Kafka producer
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def send_data_to_kafka(producer, topic, data_path, batch_size=10, delay=0.5):
    """
    Gửi dữ liệu từ file CSV đến Kafka topic
    """
    # Đọc dữ liệu
    df = pd.read_csv(data_path)
    records = df.to_dict('records')
    
    # Gửi từng batch records đến Kafka
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        for record in batch:
            producer.send(topic, record)
        producer.flush()
        print(f"Sent batch {i//batch_size + 1}/{(len(records)//batch_size) + 1} with {len(batch)} records")
        time.sleep(delay)  # Đợi một chút giữa các batches
    
    return len(records)

if __name__ == "__main__":
    # Thư mục hiện tại
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(current_dir)
    
    # Path đến dữ liệu
    data_path = os.path.join(project_dir, "data_raw", "CVD_cleaned.csv")
    
    # Tạo producer
    producer = create_kafka_producer()
    
    # Gửi dữ liệu
    topic = "cvd_topic"
    total_sent = send_data_to_kafka(producer, topic, data_path)
    
    print(f"Total sent: {total_sent} records to topic '{topic}'")