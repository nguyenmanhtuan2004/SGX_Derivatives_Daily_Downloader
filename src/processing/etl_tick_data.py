import sys
import os
import argparse
import configparser
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, trim, to_date, year, month

# Thêm src vào path để nhận diện module local
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from processing.schema_parser import SchemaParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("etl_tick_data")

def init_spark_session(config):
    """Khởi tạo Spark Session cấu hình sẵn sàng kết nối MinIO và Delta Lake"""
    endpoint = config.get("minio", "endpoint")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    
    return SparkSession.builder \
        .appName("SGX_Tick_Data_ETL") \
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

def unzip_rdd_stream(binary_file_tuple):
    """Giải nén file ZIP trực tiếp trong bộ nhớ RAM của Spark Executor"""
    import zipfile
    import io
    file_path, binary_content = binary_file_tuple
    lines = []
    with zipfile.ZipFile(io.BytesIO(binary_content)) as z:
        for name in z.namelist():
            with z.open(name) as f:
                lines.extend(f.read().decode('utf-8').splitlines())
    return lines

def run_etl(date_str, config_path):
    # Đọc cấu hình config.ini
    config = configparser.ConfigParser()
    config.read(config_path)
    
    bucket = config.get("minio", "bucket")
    endpoint = config.get("minio", "endpoint")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    
    date_normalized = date_str.replace("-", "")
    
    # 2. Khởi tạo Spark
    spark = init_spark_session(config)
    
    # 3. Đọc và giải nén file ZIP từ MinIO Raw Zone
    zip_path = f"s3a://{bucket}/raw/{date_normalized}/WEBPXTICK_DT_{date_normalized}.zip"
    logger.info(f"Spark đang đọc file ZIP từ: {zip_path}")
    
    try:
        binary_rdd = spark.sparkContext.binaryFiles(zip_path)
        lines_rdd = binary_rdd.flatMap(unzip_rdd_stream)
        
        # Kiểm tra nếu file rỗng
        if lines_rdd.isEmpty():
            logger.warning(f"File ZIP {zip_path} không chứa dữ liệu hoặc rỗng.")
            return
            
        # Đọc trực tiếp RDD dưới dạng CSV có header
        df_parsed = spark.read.csv(lines_rdd, header=True, inferSchema=False)
        
        # 5. Làm sạch, định dạng ngày/giờ và tạo cột phân vùng
        df_cleaned = df_parsed \
            .withColumn("Symbol", trim(col("Comm"))) \
            .withColumn("ContractType", trim(col("Contract_Type"))) \
            .withColumn("MonthCode", trim(col("Mth_Code"))) \
            .withColumn("DeliveryYear", trim(col("Year"))) \
            .withColumn("StrikePrice", trim(col("Strike"))) \
            .withColumn("TradeTime", trim(col("Log_Time"))) \
            .withColumn("MessageCode", trim(col("Msg_Code"))) \
            .withColumn("TradeDateParsed", to_date(col("Trade_Date"), "yyyyMMdd")) \
            .withColumn("TradePrice", col("Price").cast("decimal(18,4)")) \
            .withColumn("TradeVolume", col("Volume").cast("int")) \
            .withColumn("year", year(col("TradeDateParsed"))) \
            .withColumn("month", month(col("TradeDateParsed"))) \
            .select("Symbol", "ContractType", "MonthCode", "DeliveryYear", "StrikePrice", 
                    "TradeTime", "MessageCode", "TradeDateParsed", "TradePrice", "TradeVolume", 
                    "year", "month")
            
        # 6. Ghi dữ liệu xuống Delta Table tại Processed Zone
        output_path = f"s3a://{bucket}/processed/ticks"
        logger.info(f"Đang ghi dữ liệu vào Delta Table: {output_path}")
        
        df_cleaned.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("year", "month") \
            .save(output_path)
            
        logger.info("✓ Hoàn thành ETL Tick Data thành công!")
        
    except Exception as e:
        logger.error(f"✗ Lỗi trong quá trình chạy Spark ETL: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark ETL Tick Data")
    parser.add_argument("--date", required=True, help="Ngày xử lý dữ liệu YYYY-MM-DD")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file cấu hình config.ini")
    args = parser.parse_args()
    
    run_etl(args.date, args.config)