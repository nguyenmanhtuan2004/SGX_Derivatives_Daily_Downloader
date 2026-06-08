# Databricks notebook source
# DBTITLE 1,Cấu hình Widgets để nhập tham số
dbutils.widgets.text("date", "", "Ngày chạy (YYYY-MM-DD) - Dành cho chạy đơn ngày")
dbutils.widgets.text("start_date", "", "Ngày bắt đầu batch (YYYY-MM-DD) - Dành cho backfill")
dbutils.widgets.text("end_date", "", "Ngày kết thúc batch (YYYY-MM-DD) - Dành cho backfill")
dbutils.widgets.text("bucket", "sgx-derivatives-daily-data-079", "Tên Cloud Bucket (S3/ADLS)")
dbutils.widgets.text("secret_scope", "sgx-scope", "Databricks Secret Scope Name")

# DBTITLE 1,Import thư viện và khởi tạo cấu hình
from pyspark.sql.functions import col, trim, to_date, year, month
import logging
import boto3
import os
import zipfile
import shutil
import datetime
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("db_etl_tick")

# Đọc tham số từ Widgets
date_str = dbutils.widgets.get("date").strip()
start_date_str = dbutils.widgets.get("start_date").strip()
end_date_str = dbutils.widgets.get("end_date").strip()
bucket_name = dbutils.widgets.get("bucket").strip()
scope_name = dbutils.widgets.get("secret_scope").strip()

if not date_str and not (start_date_str and end_date_str):
    raise ValueError("Phải cung cấp tham số 'date' hoặc bộ đôi 'start_date' và 'end_date'")

# Xác định danh sách các ngày cần xử lý
dates_to_process = []
if start_date_str and end_date_str:
    start_dt = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_dt = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    curr_dt = start_dt
    while curr_dt <= end_dt:
        if curr_dt.weekday() < 5:  # Chỉ lấy thứ 2 - thứ 6
            dates_to_process.append(curr_dt.strftime("%Y-%m-%d"))
        curr_dt += datetime.timedelta(days=1)
    logger.info(f"ETL Tick chạy chế độ BATCH cho {len(dates_to_process)} ngày: từ {start_date_str} đến {end_date_str}")
else:
    dates_to_process = [date_str]
    logger.info(f"ETL Tick chạy chế độ SINGLE DATE cho ngày: {date_str}")

# 1. Xác định đường dẫn trên Volume của Unity Catalog (Cả Driver và Executor đều truy cập được)
catalog_name = spark.catalog.currentCatalog()
# Đảm bảo Schema và Volume đã được tạo (tránh lỗi UC_VOLUME_NOT_FOUND khi chạy độc lập)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.sgx_lakehouse")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.sgx_lakehouse.temp_volume")

volume_path = f"/Volumes/{catalog_name}/sgx_lakehouse/temp_volume"

# Tạo thư mục tạm cục bộ trên ổ cứng SSD của Driver node (được lưu tại /tmp, POSIX chuẩn)
local_tmp_dir = f"/tmp/temp_extracted_ticks_batch_{int(time.time())}"
os.makedirs(local_tmp_dir, exist_ok=True)

logger.info(f"Khởi tạo S3 Client và tải dữ liệu thô từ S3 về Local TMP của Driver...")
try:
    access_key = dbutils.secrets.get(scope=scope_name, key="aws-access-key")
    secret_key = dbutils.secrets.get(scope=scope_name, key="aws-secret-key")
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
except Exception as e:
    logger.error(f"✗ Lỗi khởi tạo S3 client: {e}")
    raise e

# 2. Tải và giải nén lần lượt từng ngày vào thư mục tạm cục bộ /tmp
for d in dates_to_process:
    d_norm = d.replace("-", "")
    s3_key = f"raw/{d_norm}/WEBPXTICK_DT_{d_norm}.zip"
    zip_path = os.path.join(local_tmp_dir, f"temp_WEBPXTICK_DT_{d_norm}.zip")
    
    logger.info(f"Tải ZIP từ S3: s3://{bucket_name}/{s3_key} -> Local TMP: {zip_path}")
    try:
        s3_client.download_file(bucket_name, s3_key, zip_path)
        
        # Giải nén
        logger.info(f"Giải nén tệp cho ngày {d} vào {local_tmp_dir}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(local_tmp_dir)
            
        # Xóa file zip ngay sau khi giải nén thành công
        if os.path.exists(zip_path):
            os.remove(zip_path)
        logger.info(f"✓ Hoàn thành tải và giải nén ngày {d}")
    except Exception as e:
        logger.warning(f"⚠ Bỏ qua ngày {d} do không tải hoặc giải nén được: {e}")
        if os.path.exists(zip_path):
            try:
                os.remove(zip_path)
            except Exception:
                pass

# Kiểm tra xem có file CSV nào được giải nén thành công không
csv_files = []
if os.path.exists(local_tmp_dir):
    csv_files = [f for f in os.listdir(local_tmp_dir) if f.endswith(".csv")]

if not csv_files:
    logger.warning("⚠ Không tìm thấy bất kỳ tệp CSV nào được tải về và giải nén thành công. Kết thúc tiến trình ETL.")
    shutil.rmtree(local_tmp_dir, ignore_errors=True)
    dbutils.notebook.exit("No data processed")

# 2.5. Sao chép dữ liệu từ Local TMP của Driver lên Unity Catalog Volume để Spark Executors có thể đọc
volume_extract_dir = os.path.join(volume_path, f"temp_extracted_ticks_batch_{int(time.time())}")
logger.info(f"Đang đồng bộ hóa dữ liệu từ Local TMP lên Volume: {volume_extract_dir}...")
dbutils.fs.mkdirs(volume_extract_dir)

# Sử dụng shutil.copy của Python để copy qua FUSE mount thay vì dbutils.fs.cp
# Cách này tránh được lỗi LocalFilesystemAccessDeniedException đối với thư mục ngoài /Workspace
# đồng thời bỏ qua độ trễ đồng bộ (eventual consistency) của thư mục /Workspace
for file_name in os.listdir(local_tmp_dir):
    if not file_name.startswith("."):
        src_file = os.path.join(local_tmp_dir, file_name)
        dest_file = os.path.join(volume_extract_dir, file_name)
        shutil.copy(src_file, dest_file)


# DBTITLE 1,Xử lý ETL bằng Spark
try:
    # 3. Đọc dữ liệu bằng Spark native từ Volume (Bỏ qua Pandas và Arrow)
    logger.info("Spark đọc trực tiếp dữ liệu từ Volume...")
    df_parsed = spark.read.csv(f"{volume_extract_dir}/*.csv", header=True, inferSchema=False)
    
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
    logger.info(f"Ghi dữ liệu vào Managed Table: {catalog_name}.sgx_lakehouse.ticks")
    
    # Chỉ coalesce(1) khi xử lý 1 ngày duy nhất để tối ưu hiệu năng ghi của batch lớn
    write_df = df_cleaned
    if len(dates_to_process) == 1:
        write_df = df_cleaned.coalesce(1)
        
    write_df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("year", "month") \
        .saveAsTable(f"{catalog_name}.sgx_lakehouse.ticks")
        
    logger.info(f"✓ Hoàn thành ETL Tick Data cho {len(dates_to_process)} ngày và ghi vào Managed Table thành công!")
    
except Exception as e:
    logger.error(f"✗ Gặp lỗi: {e}")
    raise e
finally:
    # 7. Dọn dẹp file tạm ở cả Driver Local và Volume
    try:
        # Xóa Local TMP
        if 'local_tmp_dir' in locals():
            shutil.rmtree(local_tmp_dir, ignore_errors=True)
        # Xóa Volume TMP
        if 'volume_extract_dir' in locals():
            dbutils.fs.rm(volume_extract_dir, recurse=True)
        logger.info("✓ Đã dọn dẹp các tệp tạm cục bộ và trên Volume.")
    except Exception as e:
        logger.warning(f"Lỗi khi dọn dẹp tệp tạm: {e}")

