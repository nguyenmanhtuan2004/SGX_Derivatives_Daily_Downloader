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

    def resolve(self, target_date_str):
        target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        delta_days = (target_date - self.ref_date).days
        
        # Chỉ đếm các ngày trong tuần (Thứ 2 - Thứ 6)
        weekdays_count = 0
        current = self.ref_date
        
        if delta_days > 0:
            while current < target_date:
                current += datetime.timedelta(days=1)
                if current.weekday() < 5:
                    weekdays_count += 1
            resolved_id = self.ref_id + weekdays_count
        else:
            while current > target_date:
                if current.weekday() < 5:
                    weekdays_count += 1
                current -= datetime.timedelta(days=1)
            resolved_id = self.ref_id - weekdays_count
            
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

# DBTITLE 1,Khởi tạo table logs trạng thái nếu chưa có
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS sgx_lakehouse.ingestion_runs (
        job_date STRING,
        status STRING,
        started_at TIMESTAMP,
        ended_at TIMESTAMP,
        results STRING
    ) USING DELTA
""")

# DBTITLE 1,Chạy quy trình Ingestion
import json

resolver = DatabricksIDResolver()
downloader = DatabricksSGXDownloader(bucket_name=bucket_name)

resolved_id = resolver.resolve(run_date_str)
print(f"Resolved ID trên SGX: {resolved_id}")

started_at = datetime.datetime.now()

# Cập nhật bắt đầu
spark.sql(f"""
    INSERT INTO sgx_lakehouse.ingestion_runs 
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
        INSERT INTO sgx_lakehouse.ingestion_runs 
        VALUES ('{run_date_str}', '{final_status}', CAST('{started_at}' AS TIMESTAMP), CAST('{ended_at}' AS TIMESTAMP), '{json.dumps(results)}')
    """)
    
    if final_status == "failed":
        raise Exception(f"Một số tệp tải thất bại: {results}")
    print(f"✓ Ingestion hoàn thành xuất sắc ngày {run_date_str}!")
    
except Exception as e:
    ended_at = datetime.datetime.now()
    spark.sql(f"""
        INSERT INTO sgx_lakehouse.ingestion_runs 
        VALUES ('{run_date_str}', 'failed', CAST('{started_at}' AS TIMESTAMP), CAST('{ended_at}' AS TIMESTAMP), '{str(e)}')
    """)
    raise e
