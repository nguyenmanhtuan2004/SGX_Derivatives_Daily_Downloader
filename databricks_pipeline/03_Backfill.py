# Databricks notebook source
# DBTITLE 1,Cấu hình Widgets để nhập tham số
dbutils.widgets.text("start_date", "2026-03-01", "Ngày bắt đầu backfill (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2026-04-30", "Ngày kết thúc backfill (YYYY-MM-DD)")
dbutils.widgets.text("bucket", "sgx-derivatives-daily-data-079", "Tên Cloud Bucket (S3/ADLS)")
dbutils.widgets.dropdown("skip_ingestion", "False", ["False", "True"], "Bỏ qua Ingestion (Nếu raw data đã có trên S3)")

# DBTITLE 1,Import thư viện và cấu hình ngày tháng
import datetime
import time

start_date_str = dbutils.widgets.get("start_date").strip()
end_date_str = dbutils.widgets.get("end_date").strip()
bucket_name = dbutils.widgets.get("bucket").strip()
skip_ingestion_str = dbutils.widgets.get("skip_ingestion").strip()

start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
skip_ingestion = skip_ingestion_str == "True"

print(f"Bắt đầu nạp bù lịch sử từ {start_date} đến {end_date} (Bỏ qua Ingestion: {skip_ingestion})")

# DBTITLE 1,Phase 1: Ingestion hàng loạt (tải từng ngày từ SGX lên S3)
current_date = start_date
success_ingest_dates = []
failed_ingest_dates = []

print("=== BẮT ĐẦU PHASE 1: INGESTION HÀNG LOẠT ===")
if skip_ingestion:
    print("➔ Bỏ qua Phase 1 Ingestion theo cấu hình. Tự động thêm toàn bộ ngày trong khoảng vào danh sách ETL.")
    temp_date = start_date
    while temp_date <= end_date:
        if temp_date.weekday() < 5:
            success_ingest_dates.append(temp_date.strftime("%Y-%m-%d"))
        temp_date += datetime.timedelta(days=1)
else:
    while current_date <= end_date:
        # Chỉ chạy các ngày trong tuần
        if current_date.weekday() < 5:
            date_str = current_date.strftime("%Y-%m-%d")
            print(f"Tải raw data cho ngày: {date_str}...")
            try:
                dbutils.notebook.run(
                    "./01_Ingestion", 
                    timeout_seconds=300, 
                    arguments={"date": date_str, "bucket": bucket_name}
                )
                success_ingest_dates.append(date_str)
            except Exception as e:
                print(f"✗ Ingestion thất bại ngày: {date_str}. Lỗi: {e}")
                failed_ingest_dates.append(date_str)
                
        current_date += datetime.timedelta(days=1)

print(f"\n=== KẾT THÚC PHASE 1: Hoàn thành Ingest {len(success_ingest_dates)} ngày. Lỗi {len(failed_ingest_dates)} ngày. ===")

# DBTITLE 2,Phase 2: Spark Batch ETL (Xử lý gộp tất cả các ngày)
print("\n=== BẮT ĐẦU PHASE 2: SPARK BATCH ETL ===")
if success_ingest_dates:
    try:
        # Gọi notebook ETL Ticks một lần duy nhất cho toàn bộ khoảng ngày
        print(f"Gọi Batch ETL Ticks cho khoảng từ {start_date_str} đến {end_date_str}...")
        dbutils.notebook.run(
            "./02_ETL_Tick_Data",
            timeout_seconds=1800,  # Tăng thời gian chờ cho batch lớn
            arguments={
                "start_date": start_date_str,
                "end_date": end_date_str,
                "bucket": bucket_name
            }
        )
        print("✓ Hoàn thành Batch ETL Ticks thành công!")
        
        # Gọi notebook ETL Trade Cancel một lần duy nhất cho toàn bộ khoảng ngày
        print(f"Gọi Batch ETL Trade Cancel cho khoảng từ {start_date_str} đến {end_date_str}...")
        dbutils.notebook.run(
            "./02_ETL_Trade_Cancel",
            timeout_seconds=1200,
            arguments={
                "start_date": start_date_str,
                "end_date": end_date_str,
                "bucket": bucket_name
            }
        )
        print("✓ Hoàn thành Batch ETL Trade Cancel thành công!")
        
    except Exception as e:
        print(f"✗ Thất bại trong Phase 2 Batch ETL. Lỗi: {e}")
        raise e
else:
    print("⚠ Không có ngày nào Ingest thành công, bỏ qua Phase 2 ETL.")

# DBTITLE 3,Báo cáo kết quả chung
print("\n================ [BÁO CÁO NẠP BÙ BATCH] ================")
print(f"Ingestion thành công ({len(success_ingest_dates)} ngày): {success_ingest_dates}")
if failed_ingest_dates:
    print(f"Ingestion thất bại ({len(failed_ingest_dates)} ngày): {failed_ingest_dates}")
print("✓ Tiến trình nạp bù Batch ETL hoàn tất!")

