# Databricks notebook source
# DBTITLE 1,Cấu hình Widgets để nhập tham số
dbutils.widgets.text("date", "", "Ngày chạy (YYYY-MM-DD)")
dbutils.widgets.text("bucket", "sgx-derivatives-daily-data-079", "Tên Cloud Bucket (S3/ADLS)")

# DBTITLE 1,Import thư viện và khởi tạo cấu hình
from pyspark.sql.functions import col, trim, to_date, year, month
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("db_etl_tick")

# Đọc tham số từ Widgets
date_str = dbutils.widgets.get("date").strip()
bucket_name = dbutils.widgets.get("bucket").strip()

if not date_str:
    raise ValueError("Tham số 'date' là bắt buộc (Định dạng YYYY-MM-DD)")

date_normalized = date_str.replace("-", "")

# DBTITLE 1,Định nghĩa Helper giải nén Stream
def unzip_rdd_stream(binary_file_tuple):
    """Giải nén file ZIP trực tiếp trong bộ nhớ RAM của Spark Executor dưới dạng stream"""
    import zipfile
    import io
    file_path, binary_content = binary_file_tuple
    with zipfile.ZipFile(io.BytesIO(binary_content)) as z:
        for name in z.namelist():
            with z.open(name) as f:
                text_file = io.TextIOWrapper(f, encoding='utf-8')
                for line in text_file:
                    yield line.strip('\r\n')

# DBTITLE 1,Xử lý ETL bằng Spark
# Đường dẫn trên cloud (sử dụng s3a:// để hỗ trợ xác thực qua Secret Scope hoặc Spark Config)
zip_path = f"s3a://{bucket_name}/raw/{date_normalized}/WEBPXTICK_DT_{date_normalized}.zip"
output_path = f"s3a://{bucket_name}/processed/ticks"

logger.info(f"Spark đang đọc file ZIP từ cloud: {zip_path}")

try:
    binary_rdd = spark.sparkContext.binaryFiles(zip_path)
    lines_rdd = binary_rdd.flatMap(unzip_rdd_stream)
    
    if lines_rdd.isEmpty():
        logger.warning(f"File ZIP {zip_path} không chứa dữ liệu hoặc rỗng.")
        dbutils.notebook.exit("File ZIP rỗng")
        
    df_parsed = spark.read.csv(lines_rdd, header=True, inferSchema=False)
    
    # Loại bỏ các dòng header trùng lặp
    df_parsed = df_parsed.filter(col("Comm") != "Comm")
    
    # Làm sạch dữ liệu
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
        
    # Ghi dữ liệu xuống Delta Table
    logger.info(f"Ghi dữ liệu vào Delta Table tại: {output_path}")
    
    df_cleaned.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("year", "month") \
        .save(output_path)
        
    logger.info("✓ Hoàn thành ETL Tick Data thành công!")
    
except Exception as e:
    logger.error(f"✗ Gặp lỗi: {e}")
    raise e
