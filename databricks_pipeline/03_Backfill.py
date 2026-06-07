# Databricks notebook source
# DBTITLE 1,Cấu hình Widgets để nhập tham số
dbutils.widgets.text("start_date", "2026-03-01", "Ngày bắt đầu backfill (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2026-04-30", "Ngày kết thúc backfill (YYYY-MM-DD)")
dbutils.widgets.text("bucket", "sgx-derivatives-daily-data-079", "Tên Cloud Bucket (S3/ADLS)")

# DBTITLE 1,Import thư viện và cấu hình ngày tháng
import datetime
import time

start_date_str = dbutils.widgets.get("start_date").strip()
end_date_str = dbutils.widgets.get("end_date").strip()
bucket_name = dbutils.widgets.get("bucket").strip()

start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()

print(f"Bắt đầu nạp bù lịch sử từ {start_date} đến {end_date}")

# DBTITLE 1,Vòng lặp nạp bù các ngày làm việc (Thứ 2 - Thứ 6)
current_date = start_date
success_dates = []
failed_dates = []

while current_date <= end_date:
    # Chỉ chạy các ngày trong tuần
    if current_date.weekday() < 5:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"\n==================================================")
        print(f">>> ĐANG XỬ LÝ NGÀY: {date_str} <<<")
        print(f"==================================================")
        
        try:
            # 1. Gọi Notebook Ingestion
            print(f"1. Gọi Ingestion cho ngày {date_str}...")
            dbutils.notebook.run(
                "./01_Ingestion", 
                timeout_seconds=300, 
                arguments={"date": date_str, "bucket": bucket_name}
            )
            
            # 2. Gọi Notebook ETL Tick Data
            print(f"2. Gọi ETL Ticks cho ngày {date_str}...")
            dbutils.notebook.run(
                "./02_ETL_Tick_Data", 
                timeout_seconds=300, 
                arguments={"date": date_str, "bucket": bucket_name}
            )
            
            # 3. Gọi Notebook ETL Trade Cancel
            print(f"3. Gọi ETL Trade Cancel cho ngày {date_str}...")
            dbutils.notebook.run(
                "./02_ETL_Trade_Cancel", 
                timeout_seconds=300, 
                arguments={"date": date_str, "bucket": bucket_name}
            )
            
            print(f"✓ Hoàn thành thành công ngày: {date_str}")
            success_dates.append(date_str)
            
        except Exception as e:
            print(f"✗ Thất bại ngày: {date_str}. Lỗi: {e}")
            failed_dates.append(date_str)
            
    current_date += datetime.timedelta(days=1)

# DBTITLE 1,Báo cáo kết quả
print("\n================ [BÁO CÁO NẠP BÙ] ================")
print(f"Thành công ({len(success_dates)} ngày): {success_dates}")
if failed_dates:
    print(f"Thất bại ({len(failed_dates)} ngày): {failed_dates}")
    raise Exception(f"Có ngày bị lỗi trong tiến trình nạp bù: {failed_dates}")
else:
    print("✓ Toàn bộ quá trình nạp bù hoàn tất tốt đẹp!")
