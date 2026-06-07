# Databricks notebook source
# DBTITLE 1,Cấu hình Widgets để nhập tham số
# (Widget bucket vẫn giữ lại để tương thích các Workflow cũ nếu có, nhưng không dùng cho đường dẫn nữa)
dbutils.widgets.text("bucket", "sgx-derivatives-daily-data-079", "Tên Cloud Bucket (S3/ADLS)")

# DBTITLE 1,Import thư viện và khởi tạo
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("db_maintenance")

# Danh sách Managed Tables cần tối ưu và bảo trì
tables = [
    ("ticks", "sgx_lakehouse.ticks"),
    ("trade_cancellations", "sgx_lakehouse.trade_cancellations")
]

# DBTITLE 1,Thực thi OPTIMIZE và VACUUM
for table_name, table_path in tables:
    logger.info(f"================ Bắt đầu bảo trì bảng: {table_name} ================")
    try:
        # 1. OPTIMIZE: Gộp các file nhỏ thành các file lớn hơn (~1GB) để tối ưu hóa truy vấn
        logger.info(f"Đang chạy OPTIMIZE trên {table_path}...")
        spark.sql(f"OPTIMIZE {table_path}")
        logger.info(f"✓ Hoàn thành OPTIMIZE bảng {table_name}!")
        
        # 2. VACUUM: Loại bỏ các tệp dữ liệu cũ đã bị xóa/thay thế lâu hơn 7 ngày (168 giờ)
        logger.info(f"Đang chạy VACUUM trên {table_path}...")
        # Đảm bảo bật xóa song song để tăng tốc độ nếu cụm có nhiều node
        spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        spark.sql(f"VACUUM {table_path} RETAIN 168 HOURS")
        logger.info(f"✓ Hoàn thành VACUUM bảng {table_name}!")
        
    except Exception as e:
        logger.error(f"✗ Gặp lỗi khi bảo trì bảng {table_name}: {e}")
        raise e

print("✓ Toàn bộ quá trình bảo trì Delta Lake đã hoàn tất!")
