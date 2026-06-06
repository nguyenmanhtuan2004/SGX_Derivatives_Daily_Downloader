import os
import sys
import argparse
import datetime
import logging
import configparser

# Thêm src vào path và biến môi trường PYTHONPATH để các Python worker của Spark nhận diện được module local
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "src"))
sys.path.append(src_path)
os.environ["PYTHONPATH"] = src_path + os.pathsep + os.environ.get("PYTHONPATH", "")

from processing.etl_tick_data import init_spark_session, run_etl as run_tick_etl
from processing.etl_trade_cancel import run_etl as run_tc_etl

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("efficient_etl")

def main():
    parser = argparse.ArgumentParser(description="Efficient Spark ETL with Subprocess Isolation Loop")
    parser.add_argument("--start-date", default="2026-03-01", help="Ngày bắt đầu backfill (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="2026-04-30", help="Ngày kết thúc backfill (YYYY-MM-DD)")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file cấu hình config.ini")
    args = parser.parse_args()
    
    start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d").date()
    
    # Đặt biến môi trường cho Spark
    os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\Anaconda3\envs\spark_env\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\HP\Anaconda3\envs\spark_env\python.exe"
    os.environ["SPARK_LOCAL_DIRS"] = r"E:\spark-temp"
    os.environ["SPARK_SUBMIT_OPTS"] = r"-Divy.default.ivy.user.dir=E:\.ivy2"
    
    import subprocess
    
    if start_date != end_date:
        logger.info(f"=== CHẠY NẠP BÙ TỪ {args.start_date} ĐẾN {args.end_date} (CHẾ ĐỘ CÔ LẬP TIẾN TRÌNH) ===")
        current_date = start_date
        success_dates = []
        failed_dates = []
        
        while current_date <= end_date:
            # Chỉ chạy các ngày trong tuần (Thứ 2 - Thứ 6)
            if current_date.weekday() < 5:
                date_str = current_date.strftime("%Y-%m-%d")
                logger.info(f"\n>>> [TIẾN TRÌNH CON] Khởi chạy ETL cho ngày: {date_str}")
                
                cmd = [
                    os.environ.get("PYSPARK_PYTHON", sys.executable),
                    __file__,
                    "--start-date", date_str,
                    "--end-date", date_str,
                    "--config", args.config
                ]
                
                # Gọi tiến trình con chạy độc lập
                res = subprocess.run(cmd)
                
                if res.returncode == 0:
                    logger.info(f"✓ Thành công ngày: {date_str}")
                    success_dates.append(date_str)
                else:
                    logger.error(f"✗ Thất bại ngày {date_str} (Mã lỗi: {res.returncode})")
                    failed_dates.append(date_str)
                    
            current_date += datetime.timedelta(days=1)
            
        logger.info("\n================ [HOÀN TẤT BÁO CÁO NẠP BÙ] ================")
        logger.info(f"Thành công ({len(success_dates)} ngày): {success_dates}")
        if failed_dates:
            logger.warning(f"Thất bại ({len(failed_dates)} ngày): {failed_dates}")
            sys.exit(1)
            
    else:
        # Chạy ngày đơn lẻ
        date_str = start_date.strftime("%Y-%m-%d")
        
        # Đọc cấu hình config.ini
        config = configparser.ConfigParser()
        config.read(args.config)
        
        logger.info(f"=== ĐANG XỬ LÝ NGÀY ĐƠN LẺ: {date_str} ===")
        spark = init_spark_session(config)
        
        try:
            # 1. Chạy Tick Data ETL
            logger.info(f"Đang chạy Tick Data ETL cho ngày {date_str}...")
            run_tick_etl(date_str, args.config, spark=spark)
            
            # 2. Chạy Trade Cancellation ETL
            logger.info(f"Đang chạy Trade Cancellation ETL cho ngày {date_str}...")
            run_tc_etl(date_str, args.config, spark=spark)
            
            logger.info(f"✓ Hoàn thành ngày: {date_str}")
        except Exception as e:
            logger.error(f"✗ Thất bại ngày {date_str}: {e}")
            sys.exit(1)
        finally:
            logger.info("Đang dừng Spark Session...")
            spark.stop()

if __name__ == "__main__":
    main()
