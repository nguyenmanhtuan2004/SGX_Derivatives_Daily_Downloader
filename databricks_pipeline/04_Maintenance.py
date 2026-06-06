# Databricks notebook source
# DBTITLE 1,Cấu hình Widgets để nhập tham số
dbutils.widgets.text("bucket", "sgx-lakehouse", "Tên Cloud Bucket (S3/ADLS)")

# DBTITLE 1,Import thư viện và khởi tạo
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("db_maintenance")

bucket = dbutils.widgets.get("bucket").strip()

# Danh sách bảng cần tối ưu và bảo trì
tables = [
    ("ticks", f"s3://{bucket}/processed/ticks"),
    ("trade_cancellations", f"s3://{bucket}/processed/trade_cancellations")
]

# DBTITLE 1,Thực thi OPTIMIZE và VACUUM
for table_name, table_path in tables:
    logger.info(f"================ Bắt đầu bảo trì bảng: {table_name} ================")
    try:
        # 1. OPTIMIZE: Gộp các file nhỏ thành các file lớn hơn (~1GB) để tối ưu hóa truy vấn
        logger.info(f"Đang chạy OPTIMIZE trên {table_path}...")
        spark.sql(f"OPTIMIZE delta.`{table_path}`")
        logger.info(f"✓ Hoàn thành OPTIMIZE bảng {table_name}!")
        
        # 2. VACUUM: Loại bỏ các tệp dữ liệu cũ đã bị xóa/thay thế lâu hơn 7 ngày (168 giờ)
        logger.info(f"Đang chạy VACUUM trên {table_path}...")
        # Đảm bảo bật xóa song song để tăng tốc độ nếu cluster có nhiều node
        spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS")
        logger.info(f"✓ Hoàn thành VACUUM bảng {table_name}!")
        
    except Exception as e:
        logger.error(f"✗ Gặp lỗi khi bảo trì bảng {table_name}: {e}")
        raise e

print("✓ Toàn bộ quá trình bảo trì Delta Lake đã hoàn tất!")
