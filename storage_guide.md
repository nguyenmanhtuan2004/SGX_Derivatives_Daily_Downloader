# Cẩm Nang Lập Trình: Tầng Data Stores (MinIO + Delta Lake)

Tài liệu này chứa toàn bộ cấu trúc, đặc tả schema và mã nguồn hiện thực của **Tầng Data Stores (Phase 3 & 4)** theo mô hình **Pure Lakehouse Architecture (MinIO + Delta Lake)**.

Bạn hãy sử dụng cẩm nang này để tự tay gõ lại cấu trúc schema của các bảng Delta Lake, viết các script bảo trì dữ liệu và chạy truy vấn thử nghiệm để nắm vững kiến thức.

---

## 1. Cấu trúc Thư mục Lưu trữ & Bảo trì
Hãy tạo các thư mục và file trống này trong workspace của bạn trước:
```
SGX_Derivatives_Daily_Downloader/
│
├── src/
│   ├── ...
│   ├── processing/
│   │   ├── ...
│   │   └── maintenance.py       # Script bảo trì Delta Lake (VACUUM & OPTIMIZE)
│   │
│   └── analytics/
│       ├── __init__.py
│       └── queries.py           # Truy vấn phân tích Delta Lake qua DuckDB
│
└── storage_guide.md             # Cẩm nang này
```

---

## 2. Đặc tả Schema Vật lý các Bảng Delta Lake
Khi Spark ghi dữ liệu cột xuống MinIO, dữ liệu sẽ được ép kiểu chặt chẽ và lưu trữ dưới định dạng nén nhị phân Parquet như bảng dưới đây:

### 2.1 Bảng Giao Dịch `ticks`
Đường dẫn lưu trữ trên MinIO: `s3a://sgx-lakehouse/processed/ticks`

| Tên trường (Column) | Kiểu dữ liệu (Spark Type) | Ý nghĩa nghiệp vụ |
|---|---|---|
| `RecordType` | `StringType` | Mã định danh bản ghi giao dịch (Ví dụ: "10" = Khớp lệnh) |
| `ExpiryDate` | `DateType` | Ngày đáo hạn hợp đồng phái sinh |
| `Symbol` | `StringType` | Mã hợp đồng phái sinh (đã loại bỏ khoảng trắng thừa) |
| `TradePrice` | `DecimalType(18, 4)` | Mức giá khớp lệnh thực tế (định dạng số thập phân) |
| `TradeVolume` | `IntegerType` | Số lượng hợp đồng được khớp trong giao dịch |
| `TradeTime` | `StringType` | Thời điểm khớp (Định dạng Giờ:Phút:Giây.mili_giây) |
| `TradeDateParsed` | `DateType` | Ngày giao dịch thực tế (Dùng làm cơ sở phân vùng) |
| `Side` | `StringType` | Bên chủ động khớp lệnh (Buy = Mua, Sell = Bán) |
| **`year`** | `IntegerType` | **Cột phân vùng cấp 1** (Năm giao dịch) |
| **`month`** | `IntegerType` | **Cột phân vùng cấp 2** (Tháng giao dịch) |

### 2.2 Bảng Giao Dịch Bị Hủy `trade_cancellations`
Đường dẫn lưu trữ trên MinIO: `s3a://sgx-lakehouse/processed/trade_cancellations`

| Tên trường (Column) | Kiểu dữ liệu (Spark Type) | Ý nghĩa nghiệp vụ |
|---|---|---|
| `RecordType` | `StringType` | Mã phân loại bản ghi hủy giao dịch (Ví dụ: "20") |
| `TradeDateParsed` | `DateType` | Ngày giao dịch phát sinh lệnh hủy |
| `TradeTime` | `StringType` | Thời điểm khớp lệnh ban đầu |
| `TradeNo` | `StringType` | Số hiệu giao dịch bị hủy |
| `Symbol` | `StringType` | Mã sản phẩm phái sinh |
| `Price` | `DecimalType(18, 4)` | Mức giá của giao dịch bị hủy |
| `Volume` | `IntegerType` | Khối lượng giao dịch bị hủy |
| `CancelTime` | `StringType` | Thời điểm lệnh hủy giao dịch bắt đầu có hiệu lực |
| **`year`** | `IntegerType` | **Cột phân vùng cấp 1** (Năm phát sinh giao dịch hủy) |
| **`month`** | `IntegerType` | **Cột phân vùng cấp 2** (Tháng phát sinh giao dịch hủy) |

---

## 3. File 1: `src/processing/maintenance.py`
Do Delta Lake lưu trữ nhiều phiên bản lịch sử giao dịch (History Versioning), qua thời gian đĩa MinIO sẽ bị phình to. File này chịu trách nhiệm chạy lệnh **OPTIMIZE** để gộp các file Parquet nhỏ và **VACUUM** để xóa các file dữ liệu cũ đã quá hạn (mặc định xóa file quá 7 ngày tuổi).

```python
import sys
import os
import argparse
import configparser
import logging
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("maintenance")

def init_spark_session(config):
    endpoint = config.get("minio", "endpoint")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    
    return SparkSession.builder \
        .appName("SGX_Delta_Maintenance") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def run_maintenance(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    bucket = config.get("minio", "bucket")
    spark = init_spark_session(config)
    
    tables = [
        ("ticks", f"s3a://{bucket}/processed/ticks"),
        ("trade_cancellations", f"s3a://{bucket}/processed/trade_cancellations")
    ]
    
    try:
        for table_name, table_path in tables:
            logger.info(f"================ Bắt đầu bảo trì bảng: {table_name} ================")
            
            # 1. OPTIMIZE: Gộp các file nhỏ thành file lớn để tăng tốc độ truy vấn cột
            logger.info(f"Đang chạy OPTIMIZE trên {table_path}...")
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
            logger.info(f"✓ Hoàn thành OPTIMIZE bảng {table_name}!")
            
            # 2. VACUUM: Xóa các file lịch sử cũ đã bị loại bỏ vượt quá thời hạn lưu trữ
            # Lưu ý: Cấu hình spark.databricks.delta.vacuum.parallelDelete.enabled tăng tốc độ xóa
            logger.info(f"Đang chạy VACUUM trên {table_path}...")
            spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS") # Giữ lịch sử 7 ngày (168 giờ)
            logger.info(f"✓ Hoàn thành VACUUM bảng {table_name}!")
            
        logger.info("✓ Hoàn thành toàn bộ quy trình bảo trì Delta Lake Store!")
        
    except Exception as e:
        logger.error(f"✗ Gặp lỗi khi bảo trì Data Store: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delta Lake Data Store Maintenance Job")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file cấu hình config.ini")
    args = parser.parse_args()
    
    run_maintenance(args.config)
```

---

## 4. File 2: `src/analytics/queries.py`
Sử dụng **DuckDB** để kết nối trực tiếp vào các tệp Delta Lake trên MinIO local và thực thi các câu lệnh SQL truy vấn tốc độ cao mà không cần phải khởi động PySpark cồng kềnh.

```python
import os
import configparser
import duckdb
import argparse

def run_queries(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    endpoint = config.get("minio", "endpoint").replace("http://", "")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    bucket = config.get("minio", "bucket")
    
    # Kết nối DuckDB in-memory
    con = duckdb.connect()
    
    # Cài đặt và cấu hình giao tiếp S3 cho DuckDB
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    con.execute("SET s3_url_style='path';")
    
    # Định nghĩa đường dẫn Delta Tables trên MinIO
    ticks_path = f"s3://{bucket}/processed/ticks"
    tc_path = f"s3://{bucket}/processed/trade_cancellations"
    
    print("\n" + "="*50)
    print("TRUY VẤN THỬ NGHIỆM DELTA LAKE QUA DUCKDB")
    print("="*50)
    
    try:
        # Truy vấn 1: Xem 5 dòng đầu của bảng ticks
        print("\n1. Xem 5 dòng dữ liệu giao dịch (Ticks) đầu tiên:")
        ticks_df = con.execute(f"SELECT * FROM delta_scan('{ticks_path}') LIMIT 5").df()
        print(ticks_df)
        
        # Truy vấn 2: Thống kê tổng khối lượng giao dịch (Total Volume) theo từng mã sản phẩm
        print("\n2. Tổng khối lượng giao dịch theo sản phẩm (Symbol):")
        vol_df = con.execute(f"""
            SELECT Symbol, SUM(TradeVolume) as TotalVolume, AVG(TradePrice) as AvgPrice
            FROM delta_scan('{ticks_path}')
            GROUP BY Symbol
            ORDER BY TotalVolume DESC
        """).df()
        print(vol_df)
        
        # Truy vấn 3: Đối chiếu tìm các giao dịch bị hủy thực tế
        print("\n3. Liệt kê các giao dịch trùng khớp với dữ liệu bị hủy (Trade Cancellations):")
        matched_df = con.execute(f"""
            SELECT t.Symbol, t.TradeDateParsed, t.TradePrice, t.TradeVolume, tc.CancelTime
            FROM delta_scan('{ticks_path}') t
            INNER JOIN delta_scan('{tc_path}') tc 
              ON t.Symbol = tc.Symbol 
             AND t.TradeDateParsed = tc.TradeDateParsed 
             AND t.TradePrice = tc.Price 
             AND t.TradeVolume = tc.Volume
            LIMIT 10
        """).df()
        print(matched_df)
        
    except Exception as e:
        print(f"✗ Lỗi khi truy vấn dữ liệu từ Delta Lake: {e}")
        print("Mẹo: Hãy đảm bảo rằng bạn đã khởi chạy Spark ETL ít nhất một lần để tạo dữ liệu Delta Tables trên MinIO.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DuckDB Delta Lake Analytics Queries")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file cấu hình config.ini")
    args = parser.parse_args()
    
    run_queries(args.config)
```

---

## 5. Hướng dẫn kiểm thử và bảo trì
Sau khi đã chạy thành công Spark ETL ít nhất 1 lần để sinh dữ liệu Delta, bạn hãy thử chạy các script sau để kiểm tra:

### 5.1 Chạy bảo trì nén file và dọn rác
```bash
spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.2 \
  src/processing/maintenance.py
```

### 5.2 Chạy phân tích truy vấn dữ liệu cực nhanh
```bash
python src/analytics/queries.py
```
*(Script này sẽ gọi DuckDB quét trực tiếp các file Parquet nằm trong bucket MinIO của bạn mà không cần chạy Spark server, cho tốc độ hiển thị kết quả chỉ trong mili giây!)*
