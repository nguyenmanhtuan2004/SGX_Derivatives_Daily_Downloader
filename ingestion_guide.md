# Cẩm Nang Lập Trình: Tầng Data Ingestion (Python ➔ MinIO & SQLite)

Tài liệu này chứa toàn bộ mã nguồn, cấu trúc và giải thích thuật toán chi tiết của **Tầng Data Ingestion (Phase 1 & 2)** theo mô hình **Pure Lakehouse Architecture (MinIO + Delta Lake)** mới được phê duyệt.

Bạn hãy sử dụng cẩm nang này để tự tay gõ lại code (code tay) vào các file `.py` tương ứng trong dự án để ghi nhớ sâu bài học.

---

## 1. Cấu trúc Thư mục Ingestion
Hãy tạo các thư mục và file trống này trong workspace của bạn trước:
```
SGX_Derivatives_Daily_Downloader/
│
├── config/
│   └── config.ini
│
├── src/
│   ├── __init__.py
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── cli.py            # Argparse CLI Parser
│   │   ├── id_resolver.py    # Thuật toán dò tìm Date ↔ ID (Không đổi)
│   │   ├── downloader.py     # Download & Upload trực tiếp lên MinIO
│   │   └── recovery.py       # Quản lý Backlog & Trạng thái qua SQLite
│   └── ...
│
├── docker/
│   └── docker-compose.yml    # Docker khởi chạy duy nhất MinIO
│
├── state/
│   └── ingestion_runs.db     # SQLite DB (sẽ tự động sinh ra khi chạy code)
│
└── main.py                   # Điểm khởi chạy chính của ứng dụng
```

---

## 2. File 1: `config/config.ini`
File cấu hình chứa thông số kết nối MinIO và mốc ID tham chiếu quan trọng ngày `29/05/2026` với ID `6211`.

```ini
[general]
retry_count = 3
retry_delay_seconds = 5
request_delay_seconds = 1.5
user_agent = Mozilla/5.0 (Windows NT 10.0; Win64; x64)

[minio]
endpoint = http://localhost:9000
access_key = minioadmin
secret_key = minioadmin
bucket = sgx-lakehouse

[sgx]
base_url = https://links.sgx.com/1.0.0/derivatives-historical
reference_id = 6211
reference_date = 20260529

[logging]
log_file = logs/sgx_downloader.log
log_level_console = INFO
log_level_file = DEBUG
max_log_size_mb = 10
backup_count = 5
```

---

## 3. File 2: `src/ingestion/id_resolver.py`
Nhiệm vụ của nó là tìm xem ngày bạn yêu cầu (ví dụ `2026-05-27`) tương ứng với `{id}` nào trên server SGX.

```python
import datetime
import re
import time
import logging
import requests

logger = logging.getLogger(__name__)

class IDResolver:
    def __init__(self, base_url, ref_id, ref_date, user_agent, max_retries=3, delay=1.5):
        self.base_url = base_url
        self.ref_id = int(ref_id)
        self.ref_date = datetime.datetime.strptime(str(ref_date), "%Y%m%d").date()
        self.headers = {"User-Agent": user_agent}
        self.max_retries = max_retries
        self.delay = delay

    def _count_business_days(self, start_date, end_date):
        """Tính số ngày làm việc (Thứ 2 - Thứ 6) giữa hai ngày (có hướng âm/dương)"""
        if start_date == end_date:
            return 0
        
        reverse = False
        if start_date > end_date:
            start_date, end_date = end_date, start_date
            reverse = True
            
        day_generator = (start_date + datetime.timedelta(x + 1) for x in range((end_date - start_date).days))
        bus_days = sum(1 for day in day_generator if day.weekday() < 5)
        
        # Nếu start_date là ngày làm việc thì cộng thêm 1
        if start_date.weekday() < 5:
            bus_days += 1
            
        return -bus_days if reverse else bus_days

    def _check_id_date(self, test_id):
        """Gửi request HEAD để lấy ngày thực tế của ID đó từ header Content-Disposition"""
        url = f"{self.base_url}/{test_id}/WEBPXTICK_DT.zip"
        for attempt in range(self.max_retries):
            try:
                # Dùng HEAD để không tải toàn bộ file ZIP về, chỉ lấy Header -> Tối ưu hóa băng thông!
                response = requests.head(url, headers=self.headers, allow_redirects=True, timeout=10)
                if response.status_code == 404:
                    return None # ID này không tồn tại file (có thể là ngày nghỉ/lễ)
                
                # Trích xuất tên file từ header Content-Disposition
                cd_header = response.headers.get("Content-Disposition", "")
                match = re.search(r"WEBPXTICK_DT-(\d{8})\.zip", cd_header)
                if match:
                    date_str = match.group(1)
                    return datetime.datetime.strptime(date_str, "%Y%m%d").date()
            except Exception as e:
                logger.warning(f"Lỗi khi kiểm tra ID {test_id} (lần {attempt+1}): {e}")
                time.sleep(self.delay)
        return None

    def resolve(self, target_date_str):
        """Tìm chính xác ID của ngày được yêu cầu"""
        target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        
        # 1. Tính toán ước lượng dựa trên ngày làm việc
        bus_days = self._count_business_days(self.ref_date, target_date)
        estimated_id = self.ref_id + bus_days
        
        logger.info(f"Đang dò tìm ID cho ngày {target_date_str}. Ước lượng ban đầu: {estimated_id}")
        
        current_id = estimated_id
        visited = set()
        
        # 2. Vòng lặp tinh chỉnh để tìm ID khớp chính xác ngày
        for step in range(15): # Giới hạn tối đa 15 bước nhảy để tránh vòng lặp vô hạn
            if current_id in visited:
                break
            visited.add(current_id)
            
            actual_date = self._check_id_date(current_id)
            if not actual_date:
                # Nếu đụng ngày 404 (ngày nghỉ/lễ), thử dò các ID lân cận
                logger.debug(f"ID {current_id} trả về 404, đang dò các ID lân cận...")
                found = False
                for offset in [1, -1, 2, -2]:
                    actual_date = self._check_id_date(current_id + offset)
                    if actual_date:
                        current_id += offset
                        found = True
                        break
                if not found:
                    break # Không tìm thấy ID hợp lệ xung quanh
            
            logger.debug(f"ID {current_id} thực tế thuộc ngày: {actual_date}")
            
            if actual_date == target_date:
                logger.info(f"Tìm thấy khớp chính xác: Ngày {target_date_str} ➔ ID {current_id} ✓")
                return current_id
            elif actual_date < target_date:
                # Ngày thực tế nhỏ hơn ngày mong muốn -> Nhảy tiến ID
                current_id += max(1, (target_date - actual_date).days // 2)
            else:
                # Ngày thực tế lớn hơn ngày mong muốn -> Nhảy lùi ID
                current_id -= max(1, (actual_date - target_date).days // 2)
                
        logger.error(f"Không thể tìm thấy ID phù hợp cho ngày {target_date_str}")
        return None
```

---

## 4. File 3: `src/ingestion/downloader.py`
File này chịu trách nhiệm tải tệp từ SGX về RAM và sử dụng thư viện `boto3` để **upload thẳng lên MinIO bucket `raw/`** mà không cần qua MongoDB và không lưu file tạm local.

```python
import logging
import requests
import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)

class SGXDownloader:
    def __init__(self, base_url, minio_endpoint, access_key, secret_key, bucket_name, user_agent):
        self.base_url = base_url
        self.bucket_name = bucket_name
        self.headers = {"User-Agent": user_agent}
        
        # Thiết lập kết nối S3 API đến MinIO local
        self.s3_client = boto3.client(
            's3',
            endpoint_url=minio_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1' # dummy region bắt buộc
        )
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Kiểm tra và tự động tạo Bucket trên MinIO nếu chưa có"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except Exception:
            logger.info(f"Bucket {self.bucket_name} chưa tồn tại. Đang tiến hành tạo mới...")
            self.s3_client.create_bucket(Bucket=self.bucket_name)
            logger.info(f"✓ Tạo thành công bucket: {self.bucket_name}")

    def download_and_ingest(self, resolved_id, target_date_str):
        """Tải 4 loại tệp và upload thẳng lên MinIO raw/ directory"""
        date_normalized = target_date_str.replace("-", "") # YYYYMMDD
        
        # Cấu hình danh sách tệp cần tải từ SGX
        files_to_download = [
            "WEBPXTICK_DT.zip",
            "TC.txt",
            "TickData_structure.dat",
            "TC_structure.dat"
        ]
        
        results = []
        
        for filename in files_to_download:
            # Ghi nhận tên file thực tế kèm ngày (chỉ đổi tên file dữ liệu, giữ nguyên file structure.dat)
            actual_filename = filename if "structure" in filename else filename.replace(".", f"_{date_normalized}.")
            url = f"{self.base_url}/{resolved_id}/{filename}"
            minio_key = f"raw/{date_normalized}/{actual_filename}"
            
            logger.info(f"Đang tải {filename} từ {url}...")
            
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                if response.status_code != 200:
                    logger.error(f"Tải tệp thất bại {filename}: HTTP {response.status_code}")
                    results.append({"filename": actual_filename, "status": "failed", "error": f"HTTP {response.status_code}"})
                    continue
                
                # Upload trực tiếp mảng bytes nhị phân từ RAM lên MinIO (S3)
                logger.info(f"Đang upload {actual_filename} lên MinIO tại: {minio_key}...")
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=minio_key,
                    Body=response.content
                )
                
                logger.info(f"✓ Nạp thành công {actual_filename} lên MinIO!")
                results.append({"filename": actual_filename, "status": "success"})
                
            except Exception as e:
                logger.error(f"Lỗi trong quá trình xử lý tệp {actual_filename}: {e}")
                results.append({"filename": actual_filename, "status": "failed", "error": str(e)})
                
        return results
```

---

## 5. File 4: `src/ingestion/recovery.py`
Module quản lý Backlog và trạng thái chạy bằng cơ sở dữ liệu **SQLite** cục bộ siêu nhẹ (`state/ingestion_runs.db`).

```python
import os
import sqlite3
import json
import datetime
import logging

logger = logging.getLogger(__name__)

class RecoveryManager:
    def __init__(self, db_path):
        self.db_path = db_path
        # Đảm bảo thư mục lưu file DB SQLite luôn tồn tại
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._setup_db()

    def _setup_db(self):
        """Khởi tạo bảng quản lý chạy trong SQLite"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_runs (
                    job_date TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    last_attempt TIMESTAMP,
                    files_json TEXT
                )
            """)
            conn.commit()

    def get_failed_dates(self):
        """Quét SQLite và tìm tất cả các ngày bị lỗi để chạy bù"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT job_date FROM ingestion_runs WHERE status IN ('failed', 'partial_success') ORDER BY job_date ASC"
            )
            return [row[0] for row in cursor.fetchall()]

    def record_start(self, date_str):
        """Đánh dấu bắt đầu tiến trình chạy cho ngày cụ thể"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO ingestion_runs (job_date, status, last_attempt, retry_count)
                VALUES (?, 'running', datetime('now'), 0)
                ON CONFLICT(job_date) DO UPDATE SET
                    status = 'running',
                    last_attempt = datetime('now')
            """, (date_str,))
            conn.commit()

    def record_end(self, date_str, file_results):
        """Đánh dấu kết thúc tiến trình và phân tích lỗi"""
        failed_files = [f for f in file_results if f["status"] == "failed"]
        
        if len(failed_files) == 0:
            status = "success"
            error_msg = ""
        elif len(failed_files) == len(file_results):
            status = "failed"
            error_msg = "Tất cả các tệp đều tải thất bại"
        else:
            status = "partial_success"
            error_msg = f"Tải thất bại {len(failed_files)}/{len(file_results)} tệp"

        # Đọc retry_count cũ để tăng số lần thử nếu có lỗi lặp lại
        current_retries = 0
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT retry_count, status FROM ingestion_runs WHERE job_date = ?", (date_str,))
            row = cursor.fetchone()
            if row:
                current_retries = row[0]
                old_status = row[1]
                if status != "success" and old_status in ["failed", "partial_success"]:
                    current_retries += 1

            # Cập nhật kết quả chạy mới vào DB
            conn.execute("""
                INSERT INTO ingestion_runs (job_date, status, error_message, retry_count, last_attempt, files_json)
                VALUES (?, ?, ?, ?, datetime('now'), ?)
                ON CONFLICT(job_date) DO UPDATE SET
                    status = excluded.status,
                    error_message = excluded.error_message,
                    retry_count = ?,
                    last_attempt = excluded.last_attempt,
                    files_json = excluded.files_json
            """, (date_str, status, error_msg, current_retries, json.dumps(file_results), current_retries))
            conn.commit()
            
        if status == "success":
            logger.info(f"✓ Hoàn thành Ingest thành công ngày: {date_str}")
        else:
            logger.warning(f"✗ Hoàn thành Ingest với lỗi ({status}) ngày {date_str}: {error_msg}")
```

---

## 6. File 5: `src/ingestion/cli.py` (Không đổi)
```python
import argparse
import datetime

def parse_args():
    parser = argparse.ArgumentParser(description="SGX Derivatives Daily Ingestion CLI")
    
    parser.add_argument(
        "--mode",
        choices=["today", "history"],
        default="today",
        help="Chế độ chạy: today (mặc định) hoặc history"
    )
    
    parser.add_argument(
        "--start-date",
        help="Ngày bắt đầu chế độ history (YYYY-MM-DD), bắt buộc khi chạy mode history"
    )
    
    parser.add_argument(
        "--end-date",
        default=datetime.date.today().strftime("%Y-%m-%d"),
        help="Ngày kết thúc chế độ history (YYYY-MM-DD), mặc định là ngày hôm nay"
    )
    
    parser.add_argument(
        "--config",
        default="config/config.ini",
        help="Đường dẫn file cấu hình config.ini (mặc định: config/config.ini)"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bắt buộc chạy lại tải dữ liệu kể cả khi ngày đó đã chạy thành công trước đây"
    )
    
    return parser.parse_args()
```

---

## 7. File 6: `main.py`
File entry point điều phối chính kết nối SQLite và MinIO:

```python
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
        logging.error(f"Không tìm thấy ID phù hợp cho ngày {date_str}. Hủy Job.")
        recovery.record_end(date_str, [{"filename": "All files", "status": "failed", "error": "Could not resolve ID"}])
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
```
