import os
import sys
import datetime
import subprocess
import argparse

def main():
    parser = argparse.ArgumentParser(description="SGX Backfill Local")
    parser.add_argument("--start-date", default="2026-03-01", help="Ngày bắt đầu backfill (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="2026-04-30", help="Ngày kết thúc backfill (YYYY-MM-DD)")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file config")
    parser.add_argument("--skip-ingestion", action="store_true", help="Bỏ qua giai đoạn nạp thô")
    args = parser.parse_args()

    start_date_str = args.start_date
    end_date_str = args.end_date
    
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()

    print(f"=== BẮT ĐẦU TIẾN TRÌNH NẠP BÙ MÁY LOCAL TỪ {start_date} ĐẾN {end_date} ===")
    
    success_dates = []
    failed_dates = []

    # Phase 1: Ingest raw data day-by-day
    if not args.skip_ingestion:
        print("\n--- PHASE 1: INGESTION HÀNG NGÀY ---")
        current_date = start_date
        while current_date <= end_date:
            if current_date.weekday() < 5:  # Chỉ lấy thứ 2 - thứ 6
                date_str = current_date.strftime("%Y-%m-%d")
                print(f"-> Đang tải dữ liệu thô ngày: {date_str}...")
                
                # Gọi 01_Ingestion.py dưới dạng subprocess để cô lập tiến trình
                cmd = [
                    sys.executable,
                    os.path.join(os.path.dirname(__file__), "01_Ingestion.py"),
                    "--date", date_str,
                    "--config", args.config
                ]
                res = subprocess.run(cmd)
                
                if res.returncode == 0:
                    success_dates.append(date_str)
                else:
                    print(f"✗ Ingestion thất bại ngày: {date_str}")
                    failed_dates.append(date_str)
            current_date += datetime.timedelta(days=1)
    else:
        print("\n--- PHASE 1: BỎ QUA INGESTION THEO CẤU HÌNH ---")
        temp_date = start_date
        while temp_date <= end_date:
            if temp_date.weekday() < 5:
                success_dates.append(temp_date.strftime("%Y-%m-%d"))
            temp_date += datetime.timedelta(days=1)

    # Phase 2: Spark Batch ETL
    print("\n--- PHASE 2: SPARK BATCH ETL ---")
    if success_dates:
        # Chạy Batch ETL cho Ticks
        print(f"Khởi chạy Batch ETL Ticks cho khoảng từ {start_date_str} đến {end_date_str}...")
        cmd_ticks = [
            sys.executable,
            os.path.join(os.path.dirname(__file__), "02_ETL_Tick_Data.py"),
            "--start-date", start_date_str,
            "--end-date", end_date_str,
            "--config", args.config
        ]
        res_ticks = subprocess.run(cmd_ticks)
        
        # Chạy Batch ETL cho Trade Cancel
        print(f"Khởi chạy Batch ETL Trade Cancel cho khoảng từ {start_date_str} đến {end_date_str}...")
        cmd_tc = [
            sys.executable,
            os.path.join(os.path.dirname(__file__), "02_ETL_Trade_Cancel.py"),
            "--start-date", start_date_str,
            "--end-date", end_date_str,
            "--config", args.config
        ]
        res_tc = subprocess.run(cmd_tc)
        
        if res_ticks.returncode == 0 and res_tc.returncode == 0:
            print("\n✓ Hoàn tất Batch ETL Ticks và Trade Cancel thành công!")
        else:
            print("\n✗ Một số tác vụ ETL đã thất bại!")
            sys.exit(1)
    else:
        print("⚠ Không có ngày nào Ingest thành công, bỏ qua Phase 2 ETL.")

    print("\n================ [BÁO CÁO HOÀN TẤT BACKFILL] ================")
    print(f"Ingestion thành công ({len(success_dates)} ngày): {success_dates}")
    if failed_dates:
        print(f"Ingestion thất bại ({len(failed_dates)} ngày): {failed_dates}")

if __name__ == "__main__":
    main()
