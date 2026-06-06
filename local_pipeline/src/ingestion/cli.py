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