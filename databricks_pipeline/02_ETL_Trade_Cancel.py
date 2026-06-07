# Databricks notebook source
# DBTITLE 1,Cấu hình Widgets để nhập tham số
dbutils.widgets.text("date", "", "Ngày chạy (YYYY-MM-DD)")
dbutils.widgets.text("bucket", "sgx-derivatives-daily-data-079", "Tên Cloud Bucket (S3/ADLS)")

# DBTITLE 1,Import thư viện và cấu hình
from pyspark.sql.functions import col, trim, to_date, year, month
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("db_etl_tc")

# Đọc tham số từ Widgets
date_str = dbutils.widgets.get("date").strip()
bucket_name = dbutils.widgets.get("bucket").strip()

if not date_str:
    raise ValueError("Tham số 'date' là bắt buộc (Định dạng YYYY-MM-DD)")

date_normalized = date_str.replace("-", "")

# DBTITLE 1,Xử lý ETL bằng Spark
# Đường dẫn trên cloud (sử dụng s3a:// để hỗ trợ xác thực qua Secret Scope hoặc Spark Config)
tc_path = f"s3a://{bucket_name}/raw/{date_normalized}/TC_{date_normalized}.txt"
output_path = f"s3a://{bucket_name}/processed/trade_cancellations"

logger.info(f"Spark đang đọc file text thô từ cloud: {tc_path}")

try:
    df_parsed = spark.read.option("delimiter", "\t").csv(tc_path, header=True, inferSchema=False)
    
    # Loại bỏ các dòng header bị trùng lặp do ghép nhiều file
    df_parsed = df_parsed.filter(col("Commodity") != "Commodity")
    
    # Làm sạch ngày tháng, ép kiểu số và tạo cột phân vùng
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
        
    # Ghi dữ liệu xuống Delta Table
    logger.info(f"Ghi dữ liệu vào Delta Table tại: {output_path}")
    
    df_cleaned.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("year", "month") \
        .save(output_path)
        
    logger.info("✓ Hoàn thành ETL Trade Cancellation thành công!")
    
except Exception as e:
    logger.error(f"✗ Gặp lỗi: {e}")
    raise e
