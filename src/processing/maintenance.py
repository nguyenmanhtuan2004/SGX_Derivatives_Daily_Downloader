import sys
import os
import argparse
import configparser
import logging
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("maintenance")

def init_spark_session(config):
    endpoint = config.get("minio", "endpoint")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    
    return SparkSession.builder \
        .appName("SGX_Delta_Maintenance") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def run_maintenance(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    bucket = config.get("minio", "bucket")
    spark = init_spark_session(config)
    
    tables = [
        ("ticks", f"s3a://{bucket}/processed/ticks"),
        ("trade_cancellations", f"s3a://{bucket}/processed/trade_cancellations")
    ]
    
    try:
        for table_name, table_path in tables:
            logger.info(f"================ Bắt đầu bảo trì bảng: {table_name} ================")
            
            # 1. OPTIMIZE: Gộp các file nhỏ thành file lớn để tăng tốc độ truy vấn cột
            logger.info(f"Đang chạy OPTIMIZE trên {table_path}...")
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
            logger.info(f"✓ Hoàn thành OPTIMIZE bảng {table_name}!")
            
            # 2. VACUUM: Xóa các file lịch sử cũ đã bị loại bỏ vượt quá thời hạn lưu trữ
            # Lưu ý: Cấu hình spark.databricks.delta.vacuum.parallelDelete.enabled tăng tốc độ xóa
            logger.info(f"Đang chạy VACUUM trên {table_path}...")
            spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS") # Giữ lịch sử 7 ngày (168 giờ)
            logger.info(f"✓ Hoàn thành VACUUM bảng {table_name}!")
            
        logger.info("✓ Hoàn thành toàn bộ quy trình bảo trì Delta Lake Store!")
        
    except Exception as e:
        logger.error(f"✗ Gặp lỗi khi bảo trì Data Store: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delta Lake Data Store Maintenance Job")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file cấu hình config.ini")
    args = parser.parse_args()
    
    run_maintenance(args.config)