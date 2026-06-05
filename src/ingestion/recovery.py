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