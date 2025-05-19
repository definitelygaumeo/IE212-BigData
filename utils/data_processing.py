# CVD_Prediction_System/utils/data_processing.py
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def clean_data(df: DataFrame) -> DataFrame:
    """
    Làm sạch dữ liệu: xử lý missing values, outliers, etc.
    """
    # Loại bỏ missing values
    df = df.dropna()
    
    # Handle outliers (ví dụ: giới hạn BMI trong khoảng hợp lý)
    df = df.filter((F.col("BMI") > 10) & (F.col("BMI") < 60))
    
    return df

def feature_engineering(df: DataFrame) -> DataFrame:
    """
    Tạo các đặc trưng mới từ dữ liệu
    """
    # Ví dụ: tạo đặc trưng mới từ BMI và Age_Category
    df = df.withColumn("BMI_Age_Ratio", 
                     F.col("BMI") / F.when(F.col("Age_Category").contains("18-24"), 21)
                                    .when(F.col("Age_Category").contains("25-29"), 27)
                                    .when(F.col("Age_Category").contains("30-34"), 32)
                                    .when(F.col("Age_Category").contains("35-39"), 37)
                                    .when(F.col("Age_Category").contains("40-44"), 42)
                                    .when(F.col("Age_Category").contains("45-49"), 47)
                                    .when(F.col("Age_Category").contains("50-54"), 52)
                                    .when(F.col("Age_Category").contains("55-59"), 57)
                                    .when(F.col("Age_Category").contains("60-64"), 62)
                                    .when(F.col("Age_Category").contains("65-69"), 67)
                                    .when(F.col("Age_Category").contains("70-74"), 72)
                                    .when(F.col("Age_Category").contains("75-79"), 77)
                                    .otherwise(80))
    
    # Chuyển đổi categorical thành numerical (nếu cần)
    return df

def save_to_hdfs(df: DataFrame, path: str) -> None:
    """
    Lưu DataFrame vào HDFS
    """
    df.write.mode("overwrite").parquet(path)

def save_to_hive(df: DataFrame, database: str, table: str) -> None:
    """
    Lưu DataFrame vào Hive
    """
    df.write.mode("overwrite").saveAsTable(f"{database}.{table}")