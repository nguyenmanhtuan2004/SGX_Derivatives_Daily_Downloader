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