import sys
import os
import configparser
import logging
import datetime
import sqlite3
from logging.handlers import RotatingFileHandler

# Thêm src vào sys.path để Python nhận diện module
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from ingestion.cli import parse_args
from ingestion.id_resolver import IDResolver
from ingestion.downloader import SGXDownloader
from ingestion.recovery import RecoveryManager

def setup_logging(config):
    """Cấu hình Dual-handler logging"""
    log_file = config.get("logging", "log_file")
    log_level_console = config.get("logging", "log_level_console")
    log_level_file = config.get("logging", "log_level_file")
    max_size = int(config.get("logging", "max_log_size_mb")) * 1024 * 1024
    backup_count = int(config.get("logging", "backup_count"))

    # Đảm bảo thư mục log tồn tại
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Khởi tạo root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Handler 1: Console Handler (Hiển thị màn hình - INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level_console))
    console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Handler 2: File Handler (Ghi file xoay vòng - DEBUG)
    file_handler = RotatingFileHandler(log_file, maxBytes=max_size, backupCount=backup_count, encoding="utf-8")
    file_handler.setLevel(getattr(logging, log_level_file))
    file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s:%(lineno)d] %(message)s')
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

def run_ingestion_for_date(date_str, id_resolver, downloader, recovery, force=False):
    """Quy trình nạp dữ liệu khép kín cho 1 ngày"""
    # 0. Kiểm tra trùng lặp: Nếu ngày đó đã thành công và không chạy force ➔ Bỏ qua
    with sqlite3.connect(recovery.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM ingestion_runs WHERE job_date = ?", (date_str,))
        row = cursor.fetchone()
        if row and row[0] == "success" and not force:
            logging.info(f"Ngày {date_str} đã nạp thành công trước đây. Bỏ qua (Dùng --force để nạp lại).")
            return
            
    logging.info(f"================ Bắt đầu Ingest ngày: {date_str} ================")
    recovery.record_start(date_str)
    
    # 1. Tìm ID tương ứng trên SGX
    resolved_id = id_resolver.resolve(date_str)
    if not resolved_id:
        logging.warning(f"Bỏ qua ngày {date_str} do không tìm thấy ID phù hợp trên SGX (Ngày nghỉ/lễ).")
        recovery.record_end(date_str, [{"filename": "All files", "status": "skipped", "error": "Holiday/Weekend"}], status_override="skipped")
        return

    # 2. Tải và upload trực tiếp lên MinIO Raw Zone
    results = downloader.download_and_ingest(resolved_id, date_str)
    
    # 3. Cập nhật kết quả vào SQLite
    recovery.record_end(date_str, results)

def main():
    # 1. Parse arguments từ CLI
    args = parse_args()

    # 2. Đọc file cấu hình config.ini
    config = configparser.ConfigParser()
    if not os.path.exists(args.config):
        print(f"Lỗi: Không tìm thấy file config tại {args.config}")
        sys.exit(1)
    config.read(args.config)

    # 3. Thiết lập hệ thống Logs
    setup_logging(config)
    logger = logging.getLogger("main")
    logger.info("Khởi động hệ thống SGX Ingestion Pipeline (MinIO & SQLite)...")

    # 4. Cấu hình đường dẫn Database SQLite quản lý trạng thái
    db_path = "state/ingestion_runs.db"
    
    # 5. Khởi tạo các Modules
    minio_endpoint = config.get("minio", "endpoint")
    minio_access_key = config.get("minio", "access_key")
    minio_secret_key = config.get("minio", "secret_key")
    minio_bucket = config.get("minio", "bucket")
    user_agent = config.get("general", "user_agent")
    
    # Khởi tạo ID Resolver
    id_resolver = IDResolver(
        base_url=config.get("sgx", "base_url"),
        ref_id=config.get("sgx", "reference_id"),
        ref_date=config.get("sgx", "reference_date"),
        user_agent=user_agent,
        delay=float(config.get("general", "request_delay_seconds"))
    )
    
    # Khởi tạo Downloader kết nối MinIO
    downloader = SGXDownloader(
        base_url=config.get("sgx", "base_url"),
        minio_endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        bucket_name=minio_bucket,
        user_agent=user_agent
    )
    
    # Khởi tạo SQLite Recovery Manager
    recovery = RecoveryManager(db_path)

    # 6. Kiểm tra và tự động TẢI BÙ backlog lỗi cũ
    logger.info("Quét kiểm tra backlog lỗi trong SQLite...")
    failed_dates = recovery.get_failed_dates()
    if failed_dates:
        logger.info(f"Phát hiện {len(failed_dates)} ngày bị lỗi trước đây cần chạy bù: {failed_dates}")
        for date_str in failed_dates:
            run_ingestion_for_date(date_str, id_resolver, downloader, recovery)
    else:
        logger.info("Tuyệt vời! Không phát hiện backlog lỗi cũ cần xử lý.")

    # 7. Điều hướng chạy theo Mode
    if args.mode == "today":
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        run_ingestion_for_date(today_str, id_resolver, downloader, recovery, force=args.force)
        
    elif args.mode == "history":
        if not args.start_date:
            logger.error("Lỗi: Chế độ history bắt buộc phải truyền thêm --start-date YYYY-MM-DD")
            sys.exit(1)
            
        start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d").date()
        
        current_date = start_date
        while current_date <= end_date:
            # Chỉ tải ngày làm việc (Thứ 2 đến Thứ 6)
            if current_date.weekday() < 5:
                date_str = current_date.strftime("%Y-%m-%d")
                run_ingestion_for_date(date_str, id_resolver, downloader, recovery, force=args.force)
            current_date += datetime.timedelta(days=1)

    logger.info("Pipeline Ingestion hoàn tất phiên chạy.")

if __name__ == "__main__":
    main()
