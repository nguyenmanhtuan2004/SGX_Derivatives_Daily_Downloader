# Databricks notebook source
# DBTITLE 1,Cấu hình Widgets để nhập tham số
dbutils.widgets.text("date", "", "Ngày chạy (YYYY-MM-DD)")
dbutils.widgets.text("bucket", "sgx-derivatives-daily-data-079", "Tên Cloud Bucket (S3/ADLS)")
dbutils.widgets.text("secret_scope", "sgx-scope", "Databricks Secret Scope Name")

# DBTITLE 1,Import thư viện và khởi tạo cấu hình
from pyspark.sql.functions import col, trim, to_date, year, month
import logging
import boto3
import os
import zipfile
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("db_etl_tick")

# Đọc tham số từ Widgets
date_str = dbutils.widgets.get("date").strip()
bucket_name = dbutils.widgets.get("bucket").strip()
scope_name = dbutils.widgets.get("secret_scope").strip()

if not date_str:
    raise ValueError("Tham số 'date' là bắt buộc (Định dạng YYYY-MM-DD)")

date_normalized = date_str.replace("-", "")

# 1. Xác định đường dẫn cục bộ (Databricks Serverless bắt buộc các tệp tin Spark đọc phải nằm dưới thư mục /Workspace)
current_dir = os.getcwd()
local_zip_path = os.path.join(current_dir, f"temp_WEBPXTICK_DT_{date_normalized}.zip")
local_extract_dir = os.path.join(current_dir, f"temp_extracted_ticks_{date_normalized}")

logger.info(f"Đang dùng Boto3 tải ZIP từ S3 về thư mục Workspace: {local_zip_path}")
try:
    access_key = dbutils.secrets.get(scope=scope_name, key="aws-access-key")
    secret_key = dbutils.secrets.get(scope=scope_name, key="aws-secret-key")
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    s3_key = f"raw/{date_normalized}/WEBPXTICK_DT_{date_normalized}.zip"
    s3_client.download_file(bucket_name, s3_key, local_zip_path)
    logger.info("✓ Tải file thành công!")
except Exception as e:
    logger.error(f"✗ Lỗi tải file từ S3: {e}")
    raise e

# 2. Giải nén file ZIP cục bộ trong thư mục Workspace
logger.info(f"Giải nén file ZIP vào: {local_extract_dir}")
os.makedirs(local_extract_dir, exist_ok=True)
with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
    zip_ref.extractall(local_extract_dir)

# DBTITLE 1,Xử lý ETL bằng Spark
try:
    # 3. Sao chép các file giải nén lên DBFS (để Spark Executors đọc phân tán, tốc độ cao)
    logger.info("Liệt kê các file giải nén trong Workspace...")
    extracted_files = [f for f in os.listdir(local_extract_dir) if os.path.isfile(os.path.join(local_extract_dir, f)) and not f.startswith(".")]
    logger.info(f"Các file đã giải nén: {extracted_files}")
    
    if not extracted_files:
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu nào trong thư mục giải nén: {local_extract_dir}")
        
    dbfs_temp_dir = f"dbfs:/tmp/temp_extracted_ticks_{date_normalized}"
    logger.info(f"Đang sao chép file lên DBFS tại: {dbfs_temp_dir}...")
    dbutils.fs.mkdirs(dbfs_temp_dir)
    
    for f in extracted_files:
        local_file_path = os.path.join(local_extract_dir, f)
        dbfs_file_path = f"{dbfs_temp_dir}/{f}"
        logger.info(f"Copying file:{local_file_path} -> {dbfs_file_path}")
        dbutils.fs.cp(f"file:{local_file_path}", dbfs_file_path)
        
    # 4. Spark đọc file từ DBFS (Hoàn toàn phân tán và cực nhanh!)
    logger.info("Spark đọc file từ DBFS...")
    df_parsed = spark.read.csv(dbfs_temp_dir, header=True, inferSchema=False)
    
    # 4. Loại bỏ các dòng header trùng lặp
    df_parsed = df_parsed.filter(col("Comm") != "Comm")
    
    # 5. Làm sạch dữ liệu và cast kiểu
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
        
    # 6. Ghi dữ liệu xuống Managed Table (Lưu trữ mặc định của Databricks)
    logger.info("Ghi dữ liệu vào Managed Table: sgx_lakehouse.ticks")
    df_cleaned.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("year", "month") \
        .saveAsTable("sgx_lakehouse.ticks")
        
    # 6.5. Ghi dữ liệu ra CSV tạm trong Workspace và upload lên S3 bằng Boto3
    local_output_dir = os.path.join(current_dir, f"temp_output_ticks_{date_normalized}")
    logger.info("Spark đang ghi kết quả tạm dạng CSV ra Workspace...")
    df_cleaned.coalesce(1).write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(local_output_dir)
        
    logger.info("Bắt đầu upload các file CSV lên S3...")
    for file_name in os.listdir(local_output_dir):
        if file_name.endswith(".csv") and not file_name.startswith("."):
            local_file_path = os.path.join(local_output_dir, file_name)
            s3_output_key = f"processed/ticks/year={date_normalized[:4]}/month={date_normalized[4:6]}/{file_name}"
            logger.info(f"Uploading {file_name} -> s3://{bucket_name}/{s3_output_key}")
            s3_client.upload_file(local_file_path, bucket_name, s3_output_key)
            
    logger.info("✓ Hoàn thành ETL Tick Data và ghi sang S3 thành công!")
    
except Exception as e:
    logger.error(f"✗ Gặp lỗi: {e}")
    raise e
finally:
    # 7. Dọn dẹp file cục bộ trong Workspace và DBFS
    try:
        if os.path.exists(local_zip_path):
            os.remove(local_zip_path)
        if os.path.exists(local_extract_dir):
            shutil.rmtree(local_extract_dir)
        if 'local_output_dir' in locals() and os.path.exists(local_output_dir):
            shutil.rmtree(local_output_dir)
        if 'dbfs_temp_dir' in locals():
            try:
                dbutils.fs.rm(dbfs_temp_dir, recurse=True)
                logger.info("✓ Đã dọn dẹp các tệp tạm trên DBFS.")
            except Exception:
                pass
        logger.info("✓ Đã dọn dẹp các tệp tạm cục bộ trong Workspace.")
    except Exception as e:
        logger.warning(f"Lỗi khi dọn dẹp tệp tạm: {e}")

