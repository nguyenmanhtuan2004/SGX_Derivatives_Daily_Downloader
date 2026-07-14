import os
import sys
import argparse
import configparser
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("migrate_csv_to_postgres")

def init_spark_session(endpoint, access_key, secret_key):
    logger.info("Khởi tạo Spark Session kết nối với MinIO và PostgreSQL...")
    scala_ver = "2.12"
    try:
        import pyspark
        pyspark_dir = os.path.dirname(pyspark.__file__)
        jars_dir = os.path.join(pyspark_dir, "jars")
        if os.path.exists(jars_dir):
            for f in os.listdir(jars_dir):
                if "_2.13" in f:
                    scala_ver = "2.13"
                    break
    except Exception:
        pass

    try:
        import importlib.metadata
        delta_ver = importlib.metadata.version("delta-spark")
    except Exception:
        delta_ver = None

    try:
        import pyspark
        spark_ver = pyspark.__version__
    except Exception:
        spark_ver = "3.4.0"
        
    if delta_ver:
        if delta_ver.startswith("2."):
            delta_pkg = f"io.delta:delta-core_{scala_ver}:{delta_ver}"
        else:
            delta_pkg = f"io.delta:delta-spark_{scala_ver}:{delta_ver}"
    else:
        if spark_ver.startswith("3.4"):
            delta_pkg = f"io.delta:delta-core_{scala_ver}:2.4.0"
        elif spark_ver.startswith("3.5"):
            delta_pkg = f"io.delta:delta-spark_{scala_ver}:3.3.0"
        elif spark_ver.startswith("4.0"):
            delta_pkg = f"io.delta:delta-spark_{scala_ver}:4.0.0"
        else:
            delta_pkg = f"io.delta:delta-core_{scala_ver}:2.4.0"
        
    return SparkSession.builder \
        .appName("SGX_Migration_MinIO_to_Postgres") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", f"{delta_pkg},org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path-style-access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def main():
    parser = argparse.ArgumentParser(description="SGX Migration MinIO to Postgres")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file config")
    args = parser.parse_args()

    config = configparser.ConfigParser()
    if os.path.exists(args.config):
        config.read(args.config)
        endpoint = os.getenv("MINIO_ENDPOINT", config.get("minio", "endpoint", fallback="http://localhost:9000"))
        access_key = os.getenv("MINIO_ACCESS_KEY", config.get("minio", "access_key", fallback="minioadmin"))
        secret_key = os.getenv("MINIO_SECRET_KEY", config.get("minio", "secret_key", fallback="minioadmin"))
        bucket = os.getenv("MINIO_BUCKET", config.get("minio", "bucket", fallback="sgx-lakehouse"))
        
        pg_host = os.getenv("POSTGRES_HOST", config.get("postgres", "host", fallback="localhost")).strip()
        pg_port = os.getenv("POSTGRES_PORT", config.get("postgres", "port", fallback="5432")).strip()
        pg_user = os.getenv("POSTGRES_USER", config.get("postgres", "user", fallback="airflow")).strip()
        pg_pass = os.getenv("POSTGRES_PASSWORD", config.get("postgres", "password", fallback="airflow")).strip()
        pg_db = os.getenv("POSTGRES_DB", config.get("postgres", "database", fallback="airflow")).strip()
    else:
        logger.error(f"Không tìm thấy file config tại {args.config}")
        sys.exit(1)

    endpoint = endpoint.strip()
    access_key = access_key.strip()
    secret_key = secret_key.strip()
    bucket = bucket.strip()

    spark = init_spark_session(endpoint, access_key, secret_key)
    
    try:
        summary_input_path = f"s3a://{bucket}/processed/ticks_summary"
        logger.info(f"Đang đọc dữ liệu summary dạng phân vùng từ MinIO: {summary_input_path}")
        
        # Đọc dữ liệu từ MinIO
        df = spark.read.option("header", "true").csv(summary_input_path)
        
        # Ép kiểu dữ liệu để lưu vào Postgres chuẩn xác
        df_cast = df \
            .withColumn("TradeDateParsed", col("TradeDateParsed").cast("date")) \
            .withColumn("GroupVolume", col("GroupVolume").cast("long")) \
            .withColumn("GroupTradeCount", col("GroupTradeCount").cast("long")) \
            .withColumn("GroupPriceVolume", col("GroupPriceVolume").cast("decimal(18,4)")) \
            .withColumn("GroupPriceSum", col("GroupPriceSum").cast("decimal(18,4)"))

        jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
        logger.info(f"Đang ghi đè toàn bộ dữ liệu di chuyển vào bảng ticks_summary trên Postgres...")
        
        df_cast.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "ticks_summary") \
            .option("user", pg_user) \
            .option("password", pg_pass) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
            
        logger.info("✓ Hoàn thành di chuyển dữ liệu thành công!")
    except Exception as e:
        logger.error(f"✗ Lỗi trong quá trình di chuyển dữ liệu: {e}")
        spark.stop()
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()
