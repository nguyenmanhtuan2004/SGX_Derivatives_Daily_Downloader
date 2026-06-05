import boto3
import logging

logger = logging.getLogger(__name__)

# Schema dự phòng nếu file .dat trên MinIO bị lỗi hoặc không đọc được
DEFAULT_TICK_SCHEMA = [
    {"name": "RecordType", "start": 1, "length": 2, "type": "C"},
    {"name": "ExpiryDate", "start": 3, "length": 8, "type": "N"},
    {"name": "Symbol", "start": 11, "length": 10, "type": "C"},
    {"name": "TradePrice", "start": 21, "length": 12, "type": "N"},
    {"name": "TradeVolume", "start": 33, "length": 8, "type": "N"},
    {"name": "TradeTime", "start": 41, "length": 14, "type": "N"},
    {"name": "TradeDate", "start": 55, "length": 8, "type": "N"},
    {"name": "Side", "start": 63, "length": 1, "type": "C"}
]

DEFAULT_TC_SCHEMA = [
    {"name": "RecordType", "start": 1, "length": 2, "type": "C"},
    {"name": "TradeDate", "start": 3, "length": 8, "type": "N"},
    {"name": "TradeTime", "start": 11, "length": 8, "type": "N"},
    {"name": "TradeNo", "start": 19, "length": 10, "type": "C"},
    {"name": "Symbol", "start": 29, "length": 10, "type": "C"},
    {"name": "Price", "start": 39, "length": 12, "type": "N"},
    {"name": "Volume", "start": 51, "length": 8, "type": "N"},
    {"name": "CancelTime", "start": 59, "length": 8, "type": "N"}
]

class SchemaParser:
    def __init__(self, endpoint_url, access_key, secret_key, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'
        )

    def parse_schema(self, minio_key, fallback_type="tick"):
        """Đọc và phân tích file cấu trúc .dat từ MinIO"""
        schema = []
        try:
            logger.info(f"Đang đọc file cấu trúc từ MinIO: {minio_key}")
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=minio_key)
            content = response['Body'].read().decode('utf-8')
            
            for line in content.splitlines():
                line = line.strip()
                if not line or line.startswith('#') or line.startswith('Field'):
                    continue
                
                # Split các phần tử bằng khoảng trắng hoặc dấu phẩy
                parts = [p.strip() for p in line.replace(',', ' ').split() if p.strip()]
                
                # SGX format thường có dạng: Index  Field_Name  Start  Length  Type
                # Ví dụ: 1  RecordType  1  2  Char
                # Hoặc: RecordType  1  2  Char
                if len(parts) >= 4:
                    if parts[0].isdigit() and len(parts) >= 5:
                        name = parts[1]
                        start = int(parts[2])
                        length = int(parts[3])
                        dtype = parts[4]
                    else:
                        name = parts[0]
                        start = int(parts[1])
                        length = int(parts[2])
                        dtype = parts[3]
                        
                    schema.append({
                        "name": name,
                        "start": start,
                        "length": length,
                        "type": dtype
                    })
            
            if schema:
                logger.info(f"✓ Phân tích cấu trúc thành công với {len(schema)} cột.")
                return schema
        except Exception as e:
            logger.warning(f"✗ Lỗi khi đọc file cấu trúc {minio_key}: {e}. Sử dụng schema dự phòng.")
        
        # Trả về schema dự phòng nếu gặp lỗi
        return DEFAULT_TICK_SCHEMA if fallback_type == "tick" else DEFAULT_TC_SCHEMA