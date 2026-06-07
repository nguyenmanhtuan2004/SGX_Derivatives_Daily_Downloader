# Databricks notebook source
# DBTITLE 1,Cấu hình Widgets để nhập tham số
dbutils.widgets.text("date", "", "Ngày chạy (YYYY-MM-DD)")
dbutils.widgets.text("bucket", "sgx-derivatives-daily-data-079", "Tên Cloud Bucket (S3/ADLS)")
dbutils.widgets.text("secret_scope", "sgx-scope", "Databricks Secret Scope Name")

# DBTITLE 1,Import thư viện và cấu hình
from pyspark.sql.functions import col, trim, to_date, year, month
import logging
import boto3
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("db_etl_tc")

# Đọc tham số từ Widgets
date_str = dbutils.widgets.get("date").strip()
bucket_name = dbutils.widgets.get("bucket").strip()
scope_name = dbutils.widgets.get("secret_scope").strip()

if not date_str:
    raise ValueError("Tham số 'date' là bắt buộc (Định dạng YYYY-MM-DD)")

date_normalized = date_str.replace("-", "")

# 1. Tải file TXT từ S3 về máy cục bộ Workspace bằng Boto3 (Boto3 dùng được Key từ Secret)
current_dir = os.getcwd()
local_txt_path = os.path.join(current_dir, f"temp_TC_{date_normalized}.txt")

logger.info(f"Đang dùng Boto3 tải TXT từ S3 về máy cục bộ: {local_txt_path}")
try:
    access_key = dbutils.secrets.get(scope=scope_name, key="aws-access-key")
    secret_key = dbutils.secrets.get(scope=scope_name, key="aws-secret-key")
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    s3_key = f"raw/{date_normalized}/TC_{date_normalized}.txt"
    s3_client.download_file(bucket_name, s3_key, local_txt_path)
    logger.info("✓ Tải file thành công!")
except Exception as e:
    logger.error(f"Lỗi tải file từ S3: {e}")
    raise e

# DBTITLE 1,Xử lý ETL bằng Spark
try:
    # 2. Sao chép file lên DBFS để Spark Executors đọc phân tán (Bypass lỗi phân giải đường dẫn Serverless)
    dbfs_temp_path = f"dbfs:/tmp/temp_TC_{date_normalized}.txt"
    logger.info(f"Đang sao chép file lên DBFS tại: {dbfs_temp_path}...")
    dbutils.fs.cp(f"file:{local_txt_path}", dbfs_temp_path)
    
    # 3. Spark đọc file từ DBFS
    logger.info("Spark đọc file từ DBFS...")
    df_parsed = spark.read.option("delimiter", "\t").csv(dbfs_temp_path, header=True, inferSchema=False)
    
    # 3. Loại bỏ các dòng header bị trùng lặp do ghép nhiều file
    df_parsed = df_parsed.filter(col("Commodity") != "Commodity")
    
    # 4. Làm sạch ngày tháng, ép kiểu số và tạo cột phân vùng
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
        
    # 5. Ghi dữ liệu xuống Managed Table (Lưu trữ mặc định của Databricks)
    logger.info("Ghi dữ liệu vào Managed Table: sgx_lakehouse.trade_cancellations")
    df_cleaned.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("year", "month") \
        .saveAsTable("sgx_lakehouse.trade_cancellations")
        
    # 5.5. Ghi dữ liệu ra CSV tạm trong Workspace và upload lên S3 bằng Boto3
    local_output_dir = os.path.join(current_dir, f"temp_output_tc_{date_normalized}")
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
            s3_output_key = f"processed/trade_cancellations/year={date_normalized[:4]}/month={date_normalized[4:6]}/{file_name}"
            logger.info(f"Uploading {file_name} -> s3://{bucket_name}/{s3_output_key}")
            s3_client.upload_file(local_file_path, bucket_name, s3_output_key)
            
    logger.info("✓ Hoàn thành ETL Trade Cancellation và ghi sang S3 thành công!")
    
except Exception as e:
    logger.error(f"✗ Gặp lỗi: {e}")
    raise e
finally:
    # 6. Dọn dẹp file cục bộ trong Workspace và DBFS
    try:
        if os.path.exists(local_txt_path):
            os.remove(local_txt_path)
        if 'local_output_dir' in locals() and os.path.exists(local_output_dir):
            shutil.rmtree(local_output_dir)
        if 'dbfs_temp_path' in locals():
            try:
                dbutils.fs.rm(dbfs_temp_path, recurse=False)
                logger.info("✓ Đã dọn dẹp các tệp tạm trên DBFS.")
            except Exception:
                pass
        logger.info("✓ Đã dọn dẹp các tệp tạm cục bộ.")
    except Exception:
        pass
