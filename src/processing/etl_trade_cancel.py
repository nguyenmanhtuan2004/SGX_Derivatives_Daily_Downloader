import sys
import os
import argparse
import configparser
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, trim, to_date, year, month

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from processing.schema_parser import SchemaParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("etl_trade_cancel")

def init_spark_session(config):
    endpoint = config.get("minio", "endpoint")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    
    return SparkSession.builder \
        .appName("SGX_Trade_Cancel_ETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def run_etl(date_str, config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    bucket = config.get("minio", "bucket")
    endpoint = config.get("minio", "endpoint")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    
    date_normalized = date_str.replace("-", "")
    
    # 1. Parse Schema từ file .dat
    parser = SchemaParser(endpoint, access_key, secret_key, bucket)
    schema_key = f"raw/{date_normalized}/TC_structure.dat"
    columns_schema = parser.parse_schema(schema_key, fallback_type="tc")
    
    # 2. Khởi tạo Spark
    spark = init_spark_session(config)
    
    # 3. Đọc trực tiếp file Text thô từ MinIO Raw Zone
    tc_path = f"s3a://{bucket}/raw/{date_normalized}/TC_{date_normalized}.txt"
    logger.info(f"Spark đang đọc file text thô từ: {tc_path}")
    
    try:
        # Đọc dữ liệu text
        df_raw = spark.read.text(tc_path)
        
        # 4. Parse các trường dữ liệu theo Schema
        df_parsed = df_raw
        for col_info in columns_schema:
            name = col_info["name"]
            start = col_info["start"]
            length = col_info["length"]
            
            df_parsed = df_parsed.withColumn(name, trim(substring(col("value"), start, length)))
            
        df_parsed = df_parsed.drop("value")
        
        # 5. Làm sạch ngày tháng, ép kiểu số và tạo cột phân vùng
        date_col = "TradeDate" if "TradeDate" in df_parsed.columns else "Date"
        
        df_cleaned = df_parsed \
            .withColumn("TradeDateParsed", to_date(col(date_col), "yyyyMMdd")) \
            .withColumn("year", year(col("TradeDateParsed"))) \
            .withColumn("month", month(col("TradeDateParsed")))
            
        # Ép kiểu dữ liệu số
        if "Price" in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn("Price", col("Price").cast("decimal(18,4)"))
        if "Volume" in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn("Volume", col("Volume").cast("int"))
            
        # 6. Ghi xuống Delta Table
        output_path = f"s3a://{bucket}/processed/trade_cancellations"
        logger.info(f"Đang ghi dữ liệu vào Delta Table: {output_path}")
        
        df_cleaned.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("year", "month") \
            .save(output_path)
            
        logger.info("✓ Hoàn thành ETL Trade Cancellation thành công!")
        
    except Exception as e:
        logger.error(f"✗ Lỗi trong quá trình chạy Spark ETL: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark ETL Trade Cancellation")
    parser.add_argument("--date", required=True, help="Ngày xử lý dữ liệu YYYY-MM-DD")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file cấu hình config.ini")
    args = parser.parse_args()
    
    run_etl(args.date, args.config)