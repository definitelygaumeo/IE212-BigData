# Phân tích Dữ liệu Y tế để Dự đoán Nguy cơ Bệnh Tim Mạch

![Banner](https://img.shields.io/badge/Big%20Data-CVD%20Prediction-blue)

## Giới thiệu

Đồ án **Phân tích Dữ liệu Y tế để Dự đoán Nguy cơ Bệnh Tim Mạch** là một hệ thống phân tích dữ liệu y tế quy mô lớn để dự đoán nguy cơ mắc bệnh tim mạch của bệnh nhân. Dự án áp dụng các công nghệ Big Data hiện đại và mô hình học máy để xử lý, phân tích và trực quan hóa dữ liệu.

## Mục tiêu

- Xây dựng pipeline xử lý dữ liệu y tế phân tán với Apache Spark
- Áp dụng mô hình học máy để dự đoán nguy cơ bệnh tim mạch
- Xây dựng hệ thống streaming data với Kafka
- Trực quan hóa kết quả phân tích với Dash/Plotly
- Phân tích các yếu tố nguy cơ và mối tương quan giữa các chỉ số sức khỏe

## Công nghệ sử dụng

- **Apache Spark**: Xử lý dữ liệu phân tán
- **Spark MLlib**: Xây dựng mô hình học máy
- **Spark Streaming**: Xử lý dữ liệu theo thời gian thực
- **Apache Kafka**: Hệ thống message broker cho streaming
- **Python**: Ngôn ngữ lập trình chính
- **Dash/Plotly**: Framework trực quan hóa dữ liệu
- **Pandas/NumPy**: Xử lý và phân tích dữ liệu

## Cấu trúc dự án

```
CVD_Prediction_System/
├── data_raw/           # Dữ liệu gốc
├── data_result/        # Kết quả sau xử lý
├── models/             # Mô hình đã huấn luyện
│   └── random_forest_model/
├── notebooks/          # Jupyter notebooks
├── streaming/          # Mã nguồn streaming 
│   ├── producer.py     # Kafka producer
│   └── consumer.py     # Kafka consumer
├── utils/              # Các hàm tiện ích
└── visualization_app/  # Ứng dụng trực quan hóa
    └── app.py         
```

## Yêu cầu hệ thống

- Python 3.8+
- Apache Spark 3.3+
- Apache Kafka 3.0+
- Libraries: pyspark, kafka-python, dash, plotly, pandas, numpy

## Cài đặt

1. Clone repository:
```
git clone https://github.com/username/CVD_Prediction_System.git
cd CVD_Prediction_System
```

2. Cài đặt dependencies:
```
pip install -r requirements.txt
```

3. Cài đặt và khởi động Kafka:
```
# Khởi động Kafka server
cd path/to/kafka
bin/windows/kafka-server-start.bat config/server.properties
```

4. Tạo Kafka topic:
```
bin/windows/kafka-topics.bat --create --topic cvd_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Hướng dẫn sử dụng

### Phân tích dữ liệu và huấn luyện mô hình

Chạy các notebooks trong thư mục `notebooks/` theo thứ tự:
1. `data_exploration.ipynb` - EDA và tiền xử lý dữ liệu
2. `feature_engineering.ipynb` - Kỹ thuật đặc trưng
3. `model_training.ipynb` - Huấn luyện mô hình ML

### Streaming data

1. Khởi động Kafka server
2. Chạy producer để gửi dữ liệu vào Kafka:
```
python CVD_Prediction_System/streaming/producer.py
```

3. (Tùy chọn) Theo dõi dữ liệu từ consumer:
```
python CVD_Prediction_System/streaming/consumer.py
```

### Trực quan hóa

Khởi động ứng dụng Dash:
```
python CVD_Prediction_System/visualization_app/app.py
```
Truy cập dashboard tại http://127.0.0.1:8050

## Kết quả đạt được

- Mô hình Random Forest đạt độ chính xác 85% trong dự đoán bệnh tim mạch
- Xác định các yếu tố nguy cơ chính ảnh hưởng đến bệnh tim mạch
- Trực quan hóa thành công mối quan hệ giữa BMI, độ tuổi, giới tính và nguy cơ bệnh tim mạch
- Xây dựng được hệ thống streaming để xử lý dữ liệu theo thời gian thực

## Tài liệu tham khảo

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Dash Documentation](https://dash.plotly.com/)

## Tác giả



---

