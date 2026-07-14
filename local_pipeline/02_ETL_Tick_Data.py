import os
import sys
import datetime
import zipfile
import shutil
import logging
import argparse
import configparser
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, year, month, regexp_replace, lpad, substring, sum, count

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("local_etl_tick")

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
        import importlib.metadata
        delta_ver = importlib.metadata.version("delta-spark")
    except Exception:
        delta_ver = None

    try:
        import pyspark
        spark_ver = pyspark.__version__
    except Exception:
        spark_ver = "3.4.0"
        
    # Chọn package Delta tương thích (Delta 2.x dùng delta-core, Delta 3.x+ dùng delta-spark)
    if delta_ver:
        if delta_ver.startswith("2."):
            delta_pkg = f"io.delta:delta-core_{scala_ver}:{delta_ver}"
        else:
            delta_pkg = f"io.delta:delta-spark_{scala_ver}:{delta_ver}"
    else:
        # Fallback thủ công nếu không lấy được phiên bản từ python metadata
        if spark_ver.startswith("3.4"):
            delta_pkg = f"io.delta:delta-core_{scala_ver}:2.4.0"
        elif spark_ver.startswith("3.5"):
            delta_pkg = f"io.delta:delta-spark_{scala_ver}:3.3.0"
        elif spark_ver.startswith("4.0"):
            delta_pkg = f"io.delta:delta-spark_{scala_ver}:4.0.0"
        else:
            delta_pkg = f"io.delta:delta-core_{scala_ver}:2.4.0"
        
    logger.info(f"Sử dụng Spark Version: {spark_ver}, Scala: {scala_ver}, Delta Python: {delta_ver}, Delta Package: {delta_pkg}")
    
    return SparkSession.builder \
        .appName("SGX_Tick_Data_ETL_Local") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", f"{delta_pkg},org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "50000") \
        .config("spark.hadoop.fs.s3a.connection.idle.time", "60000") \
        .config("spark.hadoop.fs.s3a.connection.ttl", "86400000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

def main():
    parser = argparse.ArgumentParser(description="SGX ETL Ticks Local")
    parser.add_argument("--date", default="", help="Ngày chạy đơn ngày (YYYY-MM-DD)")
    parser.add_argument("--start-date", default="", help="Ngày bắt đầu backfill (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="", help="Ngày kết thúc backfill (YYYY-MM-DD)")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file config")
    args = parser.parse_args()

    # Đọc cấu hình
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
        endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        bucket = os.getenv("MINIO_BUCKET", "sgx-lakehouse")
        
        pg_host = os.getenv("POSTGRES_HOST", "localhost").strip()
        pg_port = os.getenv("POSTGRES_PORT", "5432").strip()
        pg_user = os.getenv("POSTGRES_USER", "airflow").strip()
        pg_pass = os.getenv("POSTGRES_PASSWORD", "airflow").strip()
        pg_db = os.getenv("POSTGRES_DB", "airflow").strip()

    # Loại bỏ khoảng trắng và ký tự xuống dòng thừa (\r) nếu có
    endpoint = endpoint.strip()
    access_key = access_key.strip()
    secret_key = secret_key.strip()
    bucket = bucket.strip()

    # Xác định danh sách ngày cần chạy
    dates = []
    if args.start_date and args.end_date:
        start_dt = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_dt = datetime.datetime.strptime(args.end_date, "%Y-%m-%d").date()
        curr_dt = start_dt
        while curr_dt <= end_dt:
            if curr_dt.weekday() < 5:
                dates.append(curr_dt.strftime("%Y-%m-%d"))
            curr_dt += datetime.timedelta(days=1)
    else:
        run_date = args.date.strip()
        if not run_date:
            run_date = datetime.date.today().strftime("%Y-%m-%d")
        dates = [run_date]

    if not dates:
        logger.info("Không có ngày nào cần xử lý. Kết thúc.")
        sys.exit(0)

    # 1. Khởi tạo s3 client để tải file raw và giải nén
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4')
    )

    local_tmp_dir = os.path.abspath("temp_extracted_ticks")
    os.makedirs(local_tmp_dir, exist_ok=True)

    success_dates = []
    for d in dates:
        d_norm = d.replace("-", "")
        s3_key = f"raw/{d_norm}/WEBPXTICK_DT_{d_norm}.zip"
        zip_path = os.path.join(local_tmp_dir, f"WEBPXTICK_DT_{d_norm}.zip")

        try:
            logger.info(f"Tải raw zip cho ngày {d} từ MinIO...")
            s3_client.download_file(bucket, s3_key, zip_path)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(local_tmp_dir)
            
            os.remove(zip_path)
            success_dates.append(d)
            logger.info(f"✓ Đã giải nén tệp thô ngày {d}")
        except Exception as e:
            logger.warning(f"⚠ Bỏ qua ngày {d} do không tải/giải nén được raw zip: {e}")

    # 2. Xử lý ETL qua Spark
    csv_files = [f for f in os.listdir(local_tmp_dir) if f.endswith(".csv")]
    if not csv_files:
        logger.warning("Không tìm thấy tệp CSV nào để xử lý. Dừng Spark ETL.")
        shutil.rmtree(local_tmp_dir, ignore_errors=True)
        sys.exit(0)

    spark = init_spark_session(endpoint, access_key, secret_key)

    try:
        logger.info("Spark đọc các tệp CSV thô local...")
        df = spark.read.csv(f"{local_tmp_dir}/*.csv", header=True, inferSchema=False)
        
        # Làm sạch và định dạng dữ liệu
        df_parsed = df.filter(col("Comm") != "Comm")
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

        # Lưu thành bảng Delta trên MinIO
        ticks_output_path = f"s3a://{bucket}/processed/ticks"
        logger.info(f"Ghi Delta table Ticks vào MinIO: {ticks_output_path}")
        df_cleaned.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("TradeDateParsed") \
            .save(ticks_output_path)

        # Tính toán Pre-Aggregated Summary và lưu
        logger.info("Tính toán summary phục vụ dashboard/analytics...")
        df_with_hour = df_cleaned.withColumn(
            "Hour", 
            substring(lpad(regexp_replace(col("TradeTime"), ":", ""), 6, "0"), 1, 2)
        )
        df_summary = df_with_hour.groupBy("TradeDateParsed", "Symbol", "MessageCode", "Hour") \
            .agg(
                sum("TradeVolume").alias("GroupVolume"),
                count("*").alias("GroupTradeCount"),
                sum(col("TradePrice") * col("TradeVolume")).alias("GroupPriceVolume"),
                sum("TradePrice").alias("GroupPriceSum")
            )
        
        summary_output_path = f"s3a://{bucket}/processed/ticks_summary"
        logger.info(f"Ghi summary vào MinIO: {summary_output_path}")
        df_summary.write \
            .format("csv") \
            .option("header", "true") \
            .mode("overwrite") \
            .partitionBy("TradeDateParsed") \
            .save(summary_output_path)

        # Ghi dữ liệu summary vào PostgreSQL với giải thuật Idempotency
        jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
        try:
            logger.info("Đang thực hiện dọn dẹp dữ liệu cũ (nếu có) trên Postgres để tránh trùng lặp...")
            spark._jvm.java.lang.Class.forName("org.postgresql.Driver")
            conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, pg_user, pg_pass)
            stmt = conn.createStatement()
            
            dates_in_batch = [r["TradeDateParsed"].strftime("%Y-%m-%d") for r in df_summary.select("TradeDateParsed").distinct().collect() if r["TradeDateParsed"] is not None]
            if dates_in_batch:
                dates_str = ", ".join([f"'{d}'" for d in dates_in_batch])
                delete_sql = f"DELETE FROM ticks_summary WHERE \"TradeDateParsed\" IN ({dates_str})"
                logger.info(f"Thực thi câu lệnh SQL: {delete_sql}")
                try:
                    stmt.executeUpdate(delete_sql)
                except Exception as e:
                    logger.info("Bảng ticks_summary chưa được tạo hoặc chưa có dữ liệu cũ, bỏ qua bước xóa.")
            stmt.close()
            conn.close()
        except Exception as e:
            logger.warning(f"Lỗi khi xóa dữ liệu trùng lặp trên Postgres: {e}")

        try:
            logger.info(f"Ghi dữ liệu summary vào bảng ticks_summary trên Postgres...")
            df_summary.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "ticks_summary") \
                .option("user", pg_user) \
                .option("password", pg_pass) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            logger.info("✓ Ghi dữ liệu vào Postgres thành công!")
        except Exception as e:
            logger.error(f"✗ Lỗi khi ghi dữ liệu vào Postgres: {e}")

        logger.info("✓ Hoàn thành ETL Tick Data thành công!")
    except Exception as e:
        logger.error(f"✗ Lỗi Spark ETL Tick: {e}")
        spark.stop()
        shutil.rmtree(local_tmp_dir, ignore_errors=True)
        sys.exit(1)

    spark.stop()
    shutil.rmtree(local_tmp_dir, ignore_errors=True)
    logger.info("Hoàn tất dọn dẹp thư mục tạm.")

if __name__ == "__main__":
    main()
