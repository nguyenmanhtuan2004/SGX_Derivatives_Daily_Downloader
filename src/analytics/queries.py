import os
import configparser
import duckdb
import argparse

def run_queries(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    endpoint = config.get("minio", "endpoint").replace("http://", "")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    bucket = config.get("minio", "bucket")
    
    # Kết nối DuckDB in-memory
    con = duckdb.connect()
    
    # Cài đặt và cấu hình giao tiếp S3 cho DuckDB
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    con.execute("SET s3_url_style='path';")
    
    # Định nghĩa đường dẫn Delta Tables trên MinIO
    ticks_path = f"s3://{bucket}/processed/ticks"
    tc_path = f"s3://{bucket}/processed/trade_cancellations"
    
    print("\n" + "="*50)
    print("TRUY VẤN THỬ NGHIỆM DELTA LAKE QUA DUCKDB")
    print("="*50)
    
    try:
        # Truy vấn 1: Xem 5 dòng đầu của bảng ticks
        print("\n1. Xem 5 dòng dữ liệu giao dịch (Ticks) đầu tiên:")
        ticks_df = con.execute(f"SELECT * FROM delta_scan('{ticks_path}') LIMIT 5").df()
        print(ticks_df)
        
        # Truy vấn 2: Thống kê tổng khối lượng giao dịch (Total Volume) theo từng mã sản phẩm
        print("\n2. Tổng khối lượng giao dịch theo sản phẩm (Symbol):")
        vol_df = con.execute(f"""
            SELECT Symbol, SUM(TradeVolume) as TotalVolume, AVG(TradePrice) as AvgPrice
            FROM delta_scan('{ticks_path}')
            GROUP BY Symbol
            ORDER BY TotalVolume DESC
        """).df()
        print(vol_df)
        
        # Truy vấn 3: Đối chiếu tìm các giao dịch bị hủy thực tế
        print("\n3. Liệt kê các giao dịch trùng khớp với dữ liệu bị hủy (Trade Cancellations):")
        matched_df = con.execute(f"""
            SELECT t.Symbol, t.TradeDateParsed, t.TradePrice, t.TradeVolume, tc.CancelTime
            FROM delta_scan('{ticks_path}') t
            INNER JOIN delta_scan('{tc_path}') tc 
              ON t.Symbol = tc.Symbol 
             AND t.TradeDateParsed = tc.TradeDateParsed 
             AND t.TradePrice = tc.Price 
             AND t.TradeVolume = tc.Volume
            LIMIT 10
        """).df()
        print(matched_df)
        
    except Exception as e:
        print(f"✗ Lỗi khi truy vấn dữ liệu từ Delta Lake: {e}")
        print("Mẹo: Hãy đảm bảo rằng bạn đã khởi chạy Spark ETL ít nhất một lần để tạo dữ liệu Delta Tables trên MinIO.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DuckDB Delta Lake Analytics Queries")
    parser.add_argument("--config", default="config/config.ini", help="Đường dẫn file cấu hình config.ini")
    args = parser.parse_args()
    
    run_queries(args.config)