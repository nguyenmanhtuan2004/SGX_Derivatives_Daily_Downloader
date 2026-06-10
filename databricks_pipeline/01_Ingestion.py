# Databricks notebook source
# DBTITLE 1,Cấu hình Widgets để nhập tham số
dbutils.widgets.text("date", "", "Ngày chạy (YYYY-MM-DD) - Để trống nếu chạy ngày hôm nay")
dbutils.widgets.text("bucket", "sgx-derivatives-daily-data-079", "Tên Cloud Bucket (S3/ADLS)")
dbutils.widgets.text("secret_scope", "sgx-scope", "Databricks Secret Scope Name")

# DBTITLE 1,Import thư viện cần thiết
import os
import sys
import datetime
import urllib.request
import requests
import re
import time
import boto3
from botocore.client import Config
import logging

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("db_ingestion")

# DBTITLE 1,Đọc tham số từ Widgets
run_date_str = dbutils.widgets.get("date").strip()
bucket_name = dbutils.widgets.get("bucket").strip()
scope_name = dbutils.widgets.get("secret_scope").strip()

if not run_date_str:
    run_date_str = datetime.date.today().strftime("%Y-%m-%d")

print(f"Ngày xử lý: {run_date_str}")
print(f"Cloud Bucket Target: {bucket_name}")

# DBTITLE 1,Định nghĩa các Class Ingestion (tích hợp Cloud)
class DatabricksIDResolver:
    def __init__(self, base_url="https://links.sgx.com/1.0.0/derivatives-historical", ref_id="6211", ref_date="20260529"):
        self.base_url = base_url
        self.ref_id = int(ref_id)
        self.ref_date = datetime.datetime.strptime(ref_date, "%Y%m%d").date()
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        self.has_network_error = False

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
        for attempt in range(2):  # Giảm số lần thử xuống 2 để phản hồi lỗi mạng nhanh hơn
            try:
                # Giảm timeout xuống 4 giây để tránh bị treo lâu khi sàn SGX chặn/nghẽn mạng
                response = requests.head(url, headers=self.headers, allow_redirects=True, timeout=4)
                if response.status_code == 404:
                    return None # ID này không tồn tại file (có thể là ngày nghỉ/lễ)
                
                # Trích xuất tên file từ header Content-Disposition
                cd_header = response.headers.get("Content-Disposition", "")
                match = re.search(r"WEBPXTICK_DT-(\d{8})\.zip", cd_header)
                if match:
                    date_str = match.group(1)
                    return datetime.datetime.strptime(date_str, "%Y%m%d").date()
            except requests.RequestException as e:
                logger.warning(f"Lỗi kết nối mạng khi kiểm tra ID {test_id} (lần {attempt+1}): {e}")
                self.has_network_error = True
                return None  # Trả về None lập tức để kích hoạt vòng lặp dừng
            except Exception as e:
                logger.warning(f"Lỗi không xác định khi kiểm tra ID {test_id}: {e}")
                return None
        return None

    def resolve(self, target_date_str):
        target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        
        # 1. Tính toán ước lượng ban đầu dựa trên ngày làm việc (thuật toán tĩnh cũ)
        bus_days = self._count_business_days(self.ref_date, target_date)
        estimated_id = self.ref_id + bus_days
        
        logger.info(f"Đang dò tìm ID cho ngày {target_date_str}. Ước lượng ban đầu: {estimated_id}")
        
        # Reset cờ lỗi mạng trước mỗi phiên resolve
        self.has_network_error = False
        
        current_id = estimated_id
        visited = set()
        resolved_id = None
        
        # 2. Vòng lặp tinh chỉnh để tìm ID khớp chính xác ngày
        for step in range(15):
            if current_id in visited:
                break
            visited.add(current_id)
            
            actual_date = self._check_id_date(current_id)
            if self.has_network_error:
                logger.warning("Phát hiện lỗi mạng trong lúc dò tìm ID. Ngắt tìm kiếm để dùng Fallback ngay lập tức.")
                break
                
            if not actual_date:
                # Nếu đụng ngày 404 hoặc lỗi mạng, thử các ID lân cận
                logger.info(f"ID {current_id} không xác định ngày thực tế, đang dò các ID lân cận...")
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
                    break # Lỗi mạng hoặc không tìm thấy ID hợp lệ xung quanh
            
            logger.info(f"ID {current_id} thực tế thuộc ngày: {actual_date}")
            
            if actual_date == target_date:
                logger.info(f"Tìm thấy khớp chính xác: Ngày {target_date_str} ➔ ID {current_id} ✓")
                resolved_id = current_id
                break
            elif actual_date < target_date:
                # Ngày thực tế nhỏ hơn ngày mong muốn -> Nhảy tiến ID
                current_id += max(1, (target_date - actual_date).days // 2)
            else:
                # Ngày thực tế lớn hơn ngày mong muốn -> Nhảy lùi ID
                current_id -= max(1, (actual_date - target_date).days // 2)
        
        # 3. Áp dụng Fallback (Phương án A) nếu có lỗi mạng xảy ra hoặc không dò được ID
        if resolved_id is None:
            if self.has_network_error:
                logger.warning(f"✗ Gặp lỗi kết nối mạng trong quá trình dò tìm ID cho ngày {target_date_str}.")
                logger.warning(f"➔ Kích hoạt Fallback Phương án A: Sử dụng ID ước lượng tính toán tĩnh: {estimated_id}")
                return estimated_id
            else:
                logger.warning(f"✗ Không tìm thấy ID chính xác cho ngày {target_date_str} (có thể là ngày nghỉ/lễ).")
                return None
                
        return resolved_id

class DatabricksSGXDownloader:
    def __init__(self, bucket_name, base_url="https://links.sgx.com/1.0.0/derivatives-historical"):
        self.base_url = base_url
        self.bucket_name = bucket_name
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        
        # Databricks S3 Client (tự động nhận diện IAM Role gắn với cluster)
        # Nếu dùng key thủ công từ secret:
        try:
            access_key = dbutils.secrets.get(scope=scope_name, key="aws-access-key")
            secret_key = dbutils.secrets.get(scope=scope_name, key="aws-secret-key")
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=Config(signature_version='s3v4')
            )
            logger.info("Khởi tạo S3 Client sử dụng Access Key từ Secret Scope.")
        except Exception:
            # Fallback dùng IAM role / Default credentials của Cluster Instance Profile
            self.s3_client = boto3.client('s3')
            logger.info("Khởi tạo S3 Client sử dụng Default Cluster Credentials (IAM Instance Profile).")

    def check_raw_files_exist(self, target_date_str):
        """Kiểm tra xem các tệp thô chính đã tồn tại trên S3 raw/ hay chưa"""
        date_normalized = target_date_str.replace("-", "")
        tick_key = f"raw/{date_normalized}/WEBPXTICK_DT_{date_normalized}.zip"
        tc_key = f"raw/{date_normalized}/TC_{date_normalized}.txt"
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=tick_key)
            self.s3_client.head_object(Bucket=self.bucket_name, Key=tc_key)
            logger.info(f"✓ Phát hiện tệp thô cho ngày {target_date_str} đã tồn tại đầy đủ trên S3.")
            return True
        except Exception:
            return False

    def download_and_ingest(self, resolved_id, target_date_str):
        date_normalized = target_date_str.replace("-", "")
        files_to_download = [
            "WEBPXTICK_DT.zip",
            "TC.txt",
            "TickData_structure.dat",
            "TC_structure.dat"
        ]
        
        results = []
        for filename in files_to_download:
            actual_filename = filename if "structure" in filename else filename.replace(".", f"_{date_normalized}.")
            url = f"{self.base_url}/{resolved_id}/{filename}"
            s3_key = f"raw/{date_normalized}/{actual_filename}"
            
            logger.info(f"Đang tải {filename} từ {url}...")
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                if response.status_code != 200:
                    logger.error(f"Tải tệp thất bại {filename}: HTTP {response.status_code}")
                    results.append({"filename": actual_filename, "status": "failed", "error": f"HTTP {response.status_code}"})
                    continue
                
                logger.info(f"Đang upload {actual_filename} lên S3 tại: {s3_key}...")
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=response.content
                )
                logger.info(f"✓ Nạp thành công {actual_filename} lên S3!")
                results.append({"filename": actual_filename, "status": "success"})
            except Exception as e:
                logger.error(f"Lỗi khi xử lý tệp {actual_filename}: {e}")
                results.append({"filename": actual_filename, "status": str(e)})
                
        return results

# DBTITLE 1,Khởi tạo Database và Volume dùng Unity Catalog
catalog_name = spark.catalog.currentCatalog()
logger.info(f"Đang dùng Catalog: {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.sgx_lakehouse")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.sgx_lakehouse.temp_volume")

# DBTITLE 1,Khởi tạo table logs trạng thái nếu chưa có
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.sgx_lakehouse.ingestion_runs (
        job_date STRING,
        status STRING,
        started_at TIMESTAMP,
        ended_at TIMESTAMP,
        results STRING
    ) USING DELTA
""")

# DBTITLE 1,Chạy quy trình Ingestion
import json

started_at = datetime.datetime.now()
downloader = DatabricksSGXDownloader(bucket_name=bucket_name)

# 1. Kiểm tra nếu file thô đã có trên S3 raw/ thì bỏ qua không download lại từ SGX
if downloader.check_raw_files_exist(run_date_str):
    print(f"✓ Bỏ qua tải từ SGX vì tệp thô ngày {run_date_str} đã tồn tại đầy đủ trên S3.")
    ended_at = datetime.datetime.now()
    results = [
        {"filename": f"WEBPXTICK_DT_{run_date_str.replace('-', '')}.zip", "status": "success", "info": "Already exists on S3"},
        {"filename": f"TC_{run_date_str.replace('-', '')}.txt", "status": "success", "info": "Already exists on S3"}
    ]
    spark.sql(f"""
        INSERT INTO {catalog_name}.sgx_lakehouse.ingestion_runs 
        VALUES ('{run_date_str}', 'success', CAST('{started_at}' AS TIMESTAMP), CAST('{ended_at}' AS TIMESTAMP), '{json.dumps(results)}')
    """)
    dbutils.notebook.exit("success")

# 2. Nếu chưa có trên S3, tiến hành dò tìm ID trên SGX
resolver = DatabricksIDResolver()
resolved_id = resolver.resolve(run_date_str)
print(f"Resolved ID trên SGX: {resolved_id}")

# Nếu không tìm thấy ID do ngày lễ / ngày nghỉ, bỏ qua không báo lỗi
if resolved_id is None:
    print(f"⚠ Bỏ qua ngày chạy {run_date_str} do không tìm thấy ID tương ứng trên SGX (Ngày nghỉ/lễ).")
    ended_at = datetime.datetime.now()
    spark.sql(f"""
        INSERT INTO {catalog_name}.sgx_lakehouse.ingestion_runs 
        VALUES ('{run_date_str}', 'skipped', CAST('{started_at}' AS TIMESTAMP), CAST('{ended_at}' AS TIMESTAMP), 'Skipped holiday/weekend (No ID resolved)')
    """)
    # Thoát notebook Databricks một cách sạch sẽ trả về trạng thái "skipped"
    dbutils.notebook.exit("skipped")

# Cập nhật bắt đầu chạy thật
spark.sql(f"""
    INSERT INTO {catalog_name}.sgx_lakehouse.ingestion_runs 
    VALUES ('{run_date_str}', 'running', CAST('{started_at}' AS TIMESTAMP), NULL, NULL)
""")

try:
    results = downloader.download_and_ingest(resolved_id, run_date_str)
    ended_at = datetime.datetime.now()
    
    # Kiểm tra xem có file nào thất bại không
    failed = [r for r in results if r["status"] != "success"]
    final_status = "failed" if failed else "success"
    
    # Cập nhật kết quả vào Delta Log Table
    spark.sql(f"""
        INSERT INTO {catalog_name}.sgx_lakehouse.ingestion_runs 
        VALUES ('{run_date_str}', '{final_status}', CAST('{started_at}' AS TIMESTAMP), CAST('{ended_at}' AS TIMESTAMP), '{json.dumps(results)}')
    """)
    
    if final_status == "failed":
        raise Exception(f"Một số tệp tải thất bại: {results}")
    print(f"✓ Ingestion hoàn thành xuất sắc ngày {run_date_str}!")
    
except Exception as e:
    ended_at = datetime.datetime.now()
    spark.sql(f"""
        INSERT INTO {catalog_name}.sgx_lakehouse.ingestion_runs 
        VALUES ('{run_date_str}', 'failed', CAST('{started_at}' AS TIMESTAMP), CAST('{ended_at}' AS TIMESTAMP), '{str(e)}')
    """)
    raise e
