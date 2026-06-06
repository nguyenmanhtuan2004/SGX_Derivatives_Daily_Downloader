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
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0,org.apache.hadoop:hadoop-aws:3.4.2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.connection.idle.time", "60000") \
        .config("spark.hadoop.fs.s3a.connection.ttl", "60000") \
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.acquisition.timeout", "60000") \
        .getOrCreate()

def run_etl(date_str, config_path, spark=None):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    bucket = config.get("minio", "bucket")
    endpoint = config.get("minio", "endpoint")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    
    date_normalized = date_str.replace("-", "")
    
    # 2. Khởi tạo Spark nếu chưa truyền vào
    should_stop_spark = False
    if spark is None:
        spark = init_spark_session(config)
        should_stop_spark = True
    
    # 3. Đọc trực tiếp file Text thô từ MinIO Raw Zone (file TSV phân tách bằng tab)
    if "*" in date_normalized or "[" in date_normalized or "?" in date_normalized:
        tc_path = f"s3a://{bucket}/raw/{date_normalized}/TC_*.txt"
    else:
        tc_path = f"s3a://{bucket}/raw/{date_normalized}/TC_{date_normalized}.txt"
        
    logger.info(f"Spark đang đọc file text thô từ: {tc_path}")
    
    try:
        # Đọc dữ liệu TSV
        df_parsed = spark.read.option("delimiter", "\t").csv(tc_path, header=True, inferSchema=False)
        
        # Loại bỏ các dòng header bị trùng lặp do ghép nhiều file
        df_parsed = df_parsed.filter(col("Commodity") != "Commodity")
        
        # 5. Làm sạch ngày tháng, ép kiểu số và tạo cột phân vùng
        df_cleaned = df_parsed \
            .withColumn("Symbol", trim(col("Commodity"))) \
            .withColumn("ContractType", trim(col("Contract_Type"))) \
            .withColumn("DeliveryMonth", trim(col("Delivery_Month"))) \
            .withColumn("DeliveryYear", trim(col("Delivery_Year"))) \
            .withColumn("StrikePrice", trim(col("Strike_Price"))) \
            .withColumn("TradeDate", trim(col("Business_Date"))) \
            .withColumn("CancelTime", trim(col("Match_Time"))) \
            .withColumn("PriceIndicator", trim(col("Price_Indicator"))) \
            .withColumn("MessageCode", trim(col("Message_Code"))) \
            .withColumn("AmendCode", trim(col("Amend_Code"))) \
            .withColumn("TradeDateParsed", to_date(col("Business_Date"), "yyyyMMdd")) \
            .withColumn("Price", col("Price").cast("decimal(18,4)") / 100.0) \
            .withColumn("Volume", col("Volume").cast("int")) \
            .withColumn("year", year(col("TradeDateParsed"))) \
            .withColumn("month", month(col("TradeDateParsed"))) \
            .select("Symbol", "ContractType", "DeliveryMonth", "DeliveryYear", "StrikePrice",
                    "TradeDate", "CancelTime", "PriceIndicator", "MessageCode", "AmendCode",
                    "TradeDateParsed", "Price", "Volume", "year", "month")
            
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
        if should_stop_spark:
            spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark ETL Trade Cancellation")
    parser.add_argument("--date", required=True, help="Ngày xử lý dữ liệu YYYY-MM-DD")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file cấu hình config.ini")
    args = parser.parse_args()
    
    run_etl(args.date, args.config)