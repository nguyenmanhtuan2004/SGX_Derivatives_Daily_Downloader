# Cẩm Nang Lập Trình: Tầng Data Processing (PySpark ➔ Delta Lake trên MinIO)

Tài liệu này chứa toàn bộ mã nguồn, cấu trúc và giải thích thuật toán chi tiết của **Tầng Data Processing (Phase 4)** theo mô hình **Pure Lakehouse Architecture (MinIO + Delta Lake)**.

Tầng này có nhiệm vụ đọc dữ liệu thô (ZIP, TXT) từ **MinIO Raw Zone**, giải nén trong bộ nhớ (In-memory unzipping), tự động đọc cấu trúc file `.dat` để parse chuỗi Fixed-Width và ghi xuống dưới dạng **Delta Tables** chuẩn ACID tại **MinIO Processed Zone**.

---

## 1. Cấu trúc Thư mục Processing
Hãy tạo các thư mục và file trống này trong workspace của bạn trước:
```
SGX_Derivatives_Daily_Downloader/
│
├── src/
│   ├── ...
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── schema_parser.py       # Tự động đọc & parse file cấu trúc .dat từ MinIO
│   │   ├── etl_tick_data.py       # Spark Job giải nén & xử lý dữ liệu giao dịch (Tick Data)
│   │   └── etl_trade_cancel.py    # Spark Job xử lý dữ liệu hủy lệnh (Trade Cancellation)
│   └── ...
```

---

## 2. File 1: `src/processing/__init__.py`
Tạo file rỗng để khai báo python package:
```python
# Initialise processing package
```

---

## 3. File 2: `src/processing/schema_parser.py`
Module này kết nối vào MinIO, đọc file cấu trúc `.dat` tương ứng của ngày chạy (ví dụ `TickData_structure.dat`) và phân tích vị trí bắt đầu (Start), độ dài (Length) của từng cột. Đồng thời có sẵn cơ chế fallback (schema mặc định) để đảm bảo pipeline không bao giờ bị dừng đột ngột khi file cấu trúc bị lỗi hoặc thay đổi định dạng.

```python
import boto3
import logging

logger = logging.getLogger(__name__)

# Schema dự phòng nếu file .dat trên MinIO bị lỗi hoặc không đọc được
DEFAULT_TICK_SCHEMA = [
    {"name": "RecordType", "start": 1, "length": 2, "type": "C"},
    {"name": "ExpiryDate", "start": 3, "length": 8, "type": "N"},
    {"name": "Symbol", "start": 11, "length": 10, "type": "C"},
    {"name": "TradePrice", "start": 21, "length": 12, "type": "N"},
    {"name": "TradeVolume", "start": 33, "length": 8, "type": "N"},
    {"name": "TradeTime", "start": 41, "length": 14, "type": "N"},
    {"name": "TradeDate", "start": 55, "length": 8, "type": "N"},
    {"name": "Side", "start": 63, "length": 1, "type": "C"}
]

DEFAULT_TC_SCHEMA = [
    {"name": "RecordType", "start": 1, "length": 2, "type": "C"},
    {"name": "TradeDate", "start": 3, "length": 8, "type": "N"},
    {"name": "TradeTime", "start": 11, "length": 8, "type": "N"},
    {"name": "TradeNo", "start": 19, "length": 10, "type": "C"},
    {"name": "Symbol", "start": 29, "length": 10, "type": "C"},
    {"name": "Price", "start": 39, "length": 12, "type": "N"},
    {"name": "Volume", "start": 51, "length": 8, "type": "N"},
    {"name": "CancelTime", "start": 59, "length": 8, "type": "N"}
]

class SchemaParser:
    def __init__(self, endpoint_url, access_key, secret_key, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'
        )

    def parse_schema(self, minio_key, fallback_type="tick"):
        """Đọc và phân tích file cấu trúc .dat từ MinIO"""
        schema = []
        try:
            logger.info(f"Đang đọc file cấu trúc từ MinIO: {minio_key}")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=minio_key)
            content = response['Body'].read().decode('utf-8')
            
            for line in content.splitlines():
                line = line.strip()
                if not line or line.startswith('#') or line.startswith('Field'):
                    continue
                
                # Split các phần tử bằng khoảng trắng hoặc dấu phẩy
                parts = [p.strip() for p in line.replace(',', ' ').split() if p.strip()]
                
                # SGX format thường có dạng: Index  Field_Name  Start  Length  Type
                # Ví dụ: 1  RecordType  1  2  Char
                # Hoặc: RecordType  1  2  Char
                if len(parts) >= 4:
                    if parts[0].isdigit() and len(parts) >= 5:
                        name = parts[1]
                        start = int(parts[2])
                        length = int(parts[3])
                        dtype = parts[4]
                    else:
                        name = parts[0]
                        start = int(parts[1])
                        length = int(parts[2])
                        dtype = parts[3]
                        
                    schema.append({
                        "name": name,
                        "start": start,
                        "length": length,
                        "type": dtype
                    })
            
            if schema:
                logger.info(f"✓ Phân tích cấu trúc thành công với {len(schema)} cột.")
                return schema
        except Exception as e:
            logger.warning(f"✗ Lỗi khi đọc file cấu trúc {minio_key}: {e}. Sử dụng schema dự phòng.")
        
        # Trả về schema dự phòng nếu gặp lỗi
        return DEFAULT_TICK_SCHEMA if fallback_type == "tick" else DEFAULT_TC_SCHEMA
```

---

## 4. File 3: `src/processing/etl_tick_data.py`
Spark Job chạy ETL cho dữ liệu giao dịch hàng ngày. Job này đọc trực tiếp file ZIP thô từ MinIO, tự động giải nén trong RDD bộ nhớ, parse định dạng fixed-width, làm sạch dữ liệu, ép kiểu và ghi xuống Delta Table được phân mảnh (partitioned) theo năm và tháng để tăng tốc truy vấn sau này.

```python
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
    
    # 1. Parse Schema từ file .dat trên MinIO
    parser = SchemaParser(endpoint, access_key, secret_key, bucket)
    schema_key = f"raw/{date_normalized}/TickData_structure.dat"
    columns_schema = parser.parse_schema(schema_key, fallback_type="tick")
    
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
            
        # Chuyển đổi RDD của các dòng text thành DataFrame cột đơn 'value'
        df_raw = spark.createDataFrame(lines_rdd.map(lambda x: (x,)), ["value"])
        
        # 4. Cắt chuỗi dựa trên vị trí cột từ Schema
        df_parsed = df_raw
        for col_info in columns_schema:
            name = col_info["name"]
            start = col_info["start"]
            length = col_info["length"]
            
            # Cắt chuỗi fixed-width và trim khoảng trắng
            df_parsed = df_parsed.withColumn(name, trim(substring(col("value"), start, length)))
        
        # Loại bỏ cột dữ liệu thô ban đầu
        df_parsed = df_parsed.drop("value")
        
        # 5. Làm sạch, định dạng ngày/giờ và tạo cột phân vùng
        # Giả định cột chứa ngày giao dịch trong schema là 'TradeDate' hoặc 'Date'
        date_col = "TradeDate" if "TradeDate" in df_parsed.columns else "Date"
        
        df_cleaned = df_parsed \
            .withColumn("TradeDateParsed", to_date(col(date_col), "yyyyMMdd")) \
            .withColumn("year", year(col("TradeDateParsed"))) \
            .withColumn("month", month(col("TradeDateParsed")))
            
        # Ép kiểu dữ liệu số
        if "TradePrice" in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn("TradePrice", col("TradePrice").cast("decimal(18,4)"))
        if "TradeVolume" in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn("TradeVolume", col("TradeVolume").cast("int"))
            
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
```

---

## 5. File 4: `src/processing/etl_trade_cancel.py`
Hoàn toàn tương tự, Spark Job này xử lý dữ liệu hủy lệnh giao dịch (`TC_YYYYMMDD.txt`). Do file này là định dạng văn bản thô không nén, Spark có thể đọc trực tiếp bằng API Text, sau đó áp dụng schema từ file `TC_structure.dat`.

```python
import sys
import os
import argparse
import configparser
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, trim, to_date, year, month

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from processing.schema_parser import SchemaParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("etl_trade_cancel")

def init_spark_session(config):
    endpoint = config.get("minio", "endpoint")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    
    return SparkSession.builder \
        .appName("SGX_Trade_Cancel_ETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def run_etl(date_str, config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    bucket = config.get("minio", "bucket")
    endpoint = config.get("minio", "endpoint")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    
    date_normalized = date_str.replace("-", "")
    
    # 1. Parse Schema từ file .dat
    parser = SchemaParser(endpoint, access_key, secret_key, bucket)
    schema_key = f"raw/{date_normalized}/TC_structure.dat"
    columns_schema = parser.parse_schema(schema_key, fallback_type="tc")
    
    # 2. Khởi tạo Spark
    spark = init_spark_session(config)
    
    # 3. Đọc trực tiếp file Text thô từ MinIO Raw Zone
    tc_path = f"s3a://{bucket}/raw/{date_normalized}/TC_{date_normalized}.txt"
    logger.info(f"Spark đang đọc file text thô từ: {tc_path}")
    
    try:
        # Đọc dữ liệu text
        df_raw = spark.read.text(tc_path)
        
        # 4. Parse các trường dữ liệu theo Schema
        df_parsed = df_raw
        for col_info in columns_schema:
            name = col_info["name"]
            start = col_info["start"]
            length = col_info["length"]
            
            df_parsed = df_parsed.withColumn(name, trim(substring(col("value"), start, length)))
            
        df_parsed = df_parsed.drop("value")
        
        # 5. Làm sạch ngày tháng, ép kiểu số và tạo cột phân vùng
        date_col = "TradeDate" if "TradeDate" in df_parsed.columns else "Date"
        
        df_cleaned = df_parsed \
            .withColumn("TradeDateParsed", to_date(col(date_col), "yyyyMMdd")) \
            .withColumn("year", year(col("TradeDateParsed"))) \
            .withColumn("month", month(col("TradeDateParsed")))
            
        # Ép kiểu dữ liệu số
        if "Price" in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn("Price", col("Price").cast("decimal(18,4)"))
        if "Volume" in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn("Volume", col("Volume").cast("int"))
            
        # 6. Ghi xuống Delta Table
        output_path = f"s3a://{bucket}/processed/trade_cancellations"
        logger.info(f"Đang ghi dữ liệu vào Delta Table: {output_path}")
        
        df_cleaned.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("year", "month") \
            .save(output_path)
            
        logger.info("✓ Hoàn thành ETL Trade Cancellation thành công!")
        
    except Exception as e:
        logger.error(f"✗ Lỗi trong quá trình chạy Spark ETL: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark ETL Trade Cancellation")
    parser.add_argument("--date", required=True, help="Ngày xử lý dữ liệu YYYY-MM-DD")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file cấu hình config.ini")
    args = parser.parse_args()
    
    run_etl(args.date, args.config)
```

---

## 6. Cách thức Chạy Spark ETL trên Máy cá nhân
Khi chạy PySpark nội bộ kết nối S3A (MinIO), bạn cần chỉ định gói thư viện (Spark Packages) tương thích với phiên bản Spark đang dùng để giao tiếp với S3A và Delta Lake.

```bash
# Ví dụ chạy ETL cho ngày 29/05/2026 với Spark local:
spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.2 \
  src/processing/etl_tick_data.py --date 2026-05-29

spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.2 \
  src/processing/etl_trade_cancel.py --date 2026-05-29
```

> [!TIP]
> * **Độ tương thích phiên bản:** Hãy kiểm tra kỹ phiên bản Spark của bạn để chọn gói `--packages` phù hợp. Ví dụ: Delta Core 2.4.0 tương thích tốt nhất với Spark 3.4.x.
> * **Transaction Logs:** Cấu trúc Delta Table sẽ tự động sinh ra thư mục `_delta_log` giúp bảo vệ giao dịch (ACID) khi đọc/ghi đồng thời.
