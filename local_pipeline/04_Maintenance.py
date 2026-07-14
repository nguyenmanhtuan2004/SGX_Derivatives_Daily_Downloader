import os
import sys
import logging
import configparser
from pyspark.sql import SparkSession

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("local_maintenance")

def init_spark_session(endpoint, access_key, secret_key):
    logger.info("Đang khởi tạo Spark Session kết nối với MinIO...")
    # Tự động dò tìm Scala version (2.12 hoặc 2.13) từ thư mục jars của pyspark
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
        import pyspark
        spark_ver = pyspark.__version__
    except Exception:
        spark_ver = "3.4.0"
        
    # Chọn package Delta tương thích (Delta 2.4.0 có tên là delta-core, Delta 3.0.0+ có tên là delta-spark)
    if spark_ver.startswith("3.4"):
        delta_pkg = f"io.delta:delta-core_{scala_ver}:2.4.0"
    elif spark_ver.startswith("3.5"):
        delta_pkg = f"io.delta:delta-spark_{scala_ver}:3.0.0"
    elif spark_ver.startswith("4.0"):
        delta_pkg = f"io.delta:delta-spark_{scala_ver}:4.0.0"
    else:
        delta_pkg = f"io.delta:delta-core_{scala_ver}:2.4.0"
        
    return SparkSession.builder \
        .appName("SGX_Maintenance_Local") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", f"{delta_pkg},org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "50000") \
        .config("spark.hadoop.fs.s3a.connection.idle.time", "60000") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def main():
    config_path = "config/config.ini"
    config = configparser.ConfigParser()
    if os.path.exists(config_path):
        config.read(config_path)
        endpoint = os.getenv("MINIO_ENDPOINT", config.get("minio", "endpoint", fallback="http://localhost:9000"))
        access_key = os.getenv("MINIO_ACCESS_KEY", config.get("minio", "access_key", fallback="minioadmin"))
        secret_key = os.getenv("MINIO_SECRET_KEY", config.get("minio", "secret_key", fallback="minioadmin"))
        bucket = os.getenv("MINIO_BUCKET", config.get("minio", "bucket", fallback="sgx-lakehouse"))
    else:
        endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        bucket = os.getenv("MINIO_BUCKET", "sgx-lakehouse")

    # Loại bỏ khoảng trắng và ký tự xuống dòng thừa (\r) nếu có
    endpoint = endpoint.strip()
    access_key = access_key.strip()
    secret_key = secret_key.strip()
    bucket = bucket.strip()

    spark = init_spark_session(endpoint, access_key, secret_key)

    # Khai báo đường dẫn delta table trên MinIO
    tables = {
        "ticks": f"delta.s3a://{bucket}/processed/ticks",
        "trade_cancellations": f"delta.s3a://{bucket}/processed/trade_cancellations"
    }

    for name, path in tables.items():
        logger.info(f"================ BẢO TRÌ BẢNG: {name} ================")
        try:
            # 1. OPTIMIZE: Gom các file phân tán nhỏ thành các file lớn hơn để tối ưu hóa đọc/ghi
            logger.info(f"Đang chạy OPTIMIZE trên {path}...")
            spark.sql(f"OPTIMIZE {path}")
            logger.info(f"✓ Hoàn thành OPTIMIZE bảng {name}!")
            
            # 2. VACUUM: Dọn dẹp các lịch sử giao dịch và phiên bản file cũ quá 7 ngày (168 giờ)
            logger.info(f"Đang chạy VACUUM trên {path}...")
            spark.sql(f"VACUUM {path} RETAIN 168 HOURS")
            logger.info(f"✓ Hoàn thành VACUUM bảng {name}!")
        except Exception as e:
            logger.error(f"✗ Thất bại khi bảo trì bảng {name}: {e}")
            # Tiếp tục chạy bảng tiếp theo hoặc dừng lại tùy nhu cầu, ở đây ta throw lỗi để dễ giám sát
            spark.stop()
            sys.exit(1)

    spark.stop()
    logger.info("✓ Toàn bộ quá trình bảo trì Delta Lake hoàn tất!")

if __name__ == "__main__":
    main()
