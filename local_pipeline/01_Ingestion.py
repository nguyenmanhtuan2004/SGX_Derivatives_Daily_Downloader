import os
import sys
import datetime
import requests
import re
import sqlite3
import json
import logging
import argparse
import configparser
import boto3
from botocore.client import Config

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("local_ingestion")

class LocalIDResolver:
    def __init__(self, base_url="https://links.sgx.com/1.0.0/derivatives-historical", ref_id="6211", ref_date="20260529"):
        self.base_url = base_url
        self.ref_id = int(ref_id)
        self.ref_date = datetime.datetime.strptime(ref_date, "%Y%m%d").date()
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        self.has_network_error = False

    def _count_business_days(self, start_date, end_date):
        if start_date == end_date:
            return 0
        reverse = False
        if start_date > end_date:
            start_date, end_date = end_date, start_date
            reverse = True
        day_generator = (start_date + datetime.timedelta(x + 1) for x in range((end_date - start_date).days))
        bus_days = sum(1 for day in day_generator if day.weekday() < 5)
        if start_date.weekday() < 5:
            bus_days += 1
        return -bus_days if reverse else bus_days

    def _check_id_date(self, test_id):
        url = f"{self.base_url}/{test_id}/WEBPXTICK_DT.zip"
        try:
            response = requests.head(url, headers=self.headers, allow_redirects=True, timeout=5)
            if response.status_code == 404:
                return None
            cd_header = response.headers.get("Content-Disposition", "")
            match = re.search(r"WEBPXTICK_DT-(\d{8})\.zip", cd_header)
            if match:
                date_str = match.group(1)
                return datetime.datetime.strptime(date_str, "%Y%m%d").date()
        except Exception as e:
            logger.warning(f"Lỗi kết nối khi kiểm tra ID {test_id}: {e}")
            self.has_network_error = True
        return None

    def resolve(self, target_date_str):
        target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        bus_days = self._count_business_days(self.ref_date, target_date)
        estimated_id = self.ref_id + bus_days
        logger.info(f"Đang tìm kiếm ID SGX cho ngày {target_date_str}. Dự đoán ban đầu: {estimated_id}")
        
        self.has_network_error = False
        current_id = estimated_id
        visited = set()
        resolved_id = None
        
        for step in range(15):
            if current_id in visited:
                break
            visited.add(current_id)
            
            actual_date = self._check_id_date(current_id)
            if self.has_network_error:
                break
                
            if not actual_date:
                logger.info(f"ID {current_id} không có dữ liệu, đang kiểm tra các lân cận...")
                found = False
                for offset in [1, -1, 2, -2]:
                    actual_date = self._check_id_date(current_id + offset)
                    if self.has_network_error:
                        break
                    if actual_date:
                        current_id += offset
                        found = True
                        break
                if self.has_network_error or not found:
                    break
            
            logger.info(f"ID {current_id} tương ứng ngày: {actual_date}")
            if actual_date == target_date:
                resolved_id = current_id
                break
            elif actual_date < target_date:
                current_id += max(1, (target_date - actual_date).days // 2)
            else:
                current_id -= max(1, (actual_date - target_date).days // 2)
        
        if resolved_id is None:
            if self.has_network_error:
                logger.warning(f"Sử dụng thuật toán tĩnh dự phòng: ID {estimated_id}")
                return estimated_id
            return None
        return resolved_id

class LocalSGXDownloader:
    def __init__(self, endpoint, access_key, secret_key, bucket_name):
        self.bucket_name = bucket_name
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )

    def check_raw_files_exist(self, target_date_str):
        date_normalized = target_date_str.replace("-", "")
        tick_key = f"raw/{date_normalized}/WEBPXTICK_DT_{date_normalized}.zip"
        tc_key = f"raw/{date_normalized}/TC_{date_normalized}.txt"
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=tick_key)
            self.s3_client.head_object(Bucket=self.bucket_name, Key=tc_key)
            return True
        except Exception:
            return False

    def download_and_ingest(self, resolved_id, target_date_str):
        date_normalized = target_date_str.replace("-", "")
        files = ["WEBPXTICK_DT.zip", "TC.txt", "TickData_structure.dat", "TC_structure.dat"]
        results = []
        
        # Đảm bảo bucket tồn tại
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except Exception:
            logger.info(f"Đang tạo bucket mới: {self.bucket_name}")
            self.s3_client.create_bucket(Bucket=self.bucket_name)

        for filename in files:
            actual_filename = filename if "structure" in filename else filename.replace(".", f"_{date_normalized}.")
            url = f"https://links.sgx.com/1.0.0/derivatives-historical/{resolved_id}/{filename}"
            s3_key = f"raw/{date_normalized}/{actual_filename}"
            
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                if response.status_code != 200:
                    results.append({"filename": actual_filename, "status": "failed", "error": f"HTTP {response.status_code}"})
                    continue
                
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=response.content
                )
                logger.info(f"✓ Đã tải và lưu: {s3_key}")
                results.append({"filename": actual_filename, "status": "success"})
            except Exception as e:
                results.append({"filename": actual_filename, "status": "failed", "error": str(e)})
        return results

def get_db_connection(db_dir="state"):
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, "ingestion_runs.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_runs (
            job_date TEXT PRIMARY KEY,
            status TEXT,
            started_at TEXT,
            ended_at TEXT,
            results TEXT
        )
    """)
    conn.commit()
    return conn

def main():
    parser = argparse.ArgumentParser(description="SGX Ingestion Local")
    parser.add_argument("--date", default="", help="Ngày chạy (YYYY-MM-DD) - Để trống lấy hôm nay")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file config")
    args = parser.parse_args()

    # Đọc cấu hình
    config = configparser.ConfigParser()
    if os.path.exists(args.config):
        config.read(args.config)
        endpoint = os.getenv("MINIO_ENDPOINT", config.get("minio", "endpoint", fallback="http://localhost:9000"))
        access_key = os.getenv("MINIO_ACCESS_KEY", config.get("minio", "access_key", fallback="minioadmin"))
        secret_key = os.getenv("MINIO_SECRET_KEY", config.get("minio", "secret_key", fallback="minioadmin"))
        bucket = os.getenv("MINIO_BUCKET", config.get("minio", "bucket", fallback="sgx-lakehouse"))
    else:
        endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        bucket = os.getenv("MINIO_BUCKET", "sgx-lakehouse")

    run_date = args.date.strip()
    if not run_date:
        run_date = datetime.date.today().strftime("%Y-%m-%d")

    started_at = datetime.datetime.now().isoformat()
    logger.info(f"Bắt đầu nạp dữ liệu thô ngày: {run_date}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    downloader = LocalSGXDownloader(endpoint, access_key, secret_key, bucket)
    
    # 1. Kiểm tra xem file đã có trên MinIO chưa
    if downloader.check_raw_files_exist(run_date):
        logger.info(f"✓ Bỏ qua tải: Tệp thô ngày {run_date} đã có trên MinIO.")
        ended_at = datetime.datetime.now().isoformat()
        results = [{"filename": "All files", "status": "success", "info": "Already exists"}]
        cursor.execute(
            "INSERT OR REPLACE INTO ingestion_runs VALUES (?, ?, ?, ?, ?)",
            (run_date, "success", started_at, ended_at, json.dumps(results))
        )
        conn.commit()
        sys.exit(0)

    # 2. Dò tìm ID và Tải
    resolver = LocalIDResolver()
    resolved_id = resolver.resolve(run_date)
    if resolved_id is None:
        logger.warning(f"⚠ Bỏ qua ngày {run_date} (không tìm thấy ID trên SGX).")
        ended_at = datetime.datetime.now().isoformat()
        cursor.execute(
            "INSERT OR REPLACE INTO ingestion_runs VALUES (?, ?, ?, ?, ?)",
            (run_date, "skipped", started_at, ended_at, "Holiday or Weekend")
        )
        conn.commit()
        sys.exit(0)

    cursor.execute(
        "INSERT OR REPLACE INTO ingestion_runs VALUES (?, ?, ?, NULL, NULL)",
        (run_date, "running", started_at)
    )
    conn.commit()

    try:
        results = downloader.download_and_ingest(resolved_id, run_date)
        ended_at = datetime.datetime.now().isoformat()
        
        failed = [r for r in results if r["status"] != "success"]
        status = "failed" if failed else "success"
        
        cursor.execute(
            "UPDATE ingestion_runs SET status = ?, ended_at = ?, results = ? WHERE job_date = ?",
            (status, ended_at, json.dumps(results), run_date)
        )
        conn.commit()
        
        if status == "failed":
            logger.error(f"Tải tệp thất bại: {results}")
            sys.exit(1)
        logger.info(f"✓ Nạp thô thành công cho ngày {run_date}")
    except Exception as e:
        ended_at = datetime.datetime.now().isoformat()
        cursor.execute(
            "UPDATE ingestion_runs SET status = 'failed', ended_at = ?, results = ? WHERE job_date = ?",
            (ended_at, str(e), run_date)
        )
        conn.commit()
        logger.error(f"Lỗi Ingestion: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
