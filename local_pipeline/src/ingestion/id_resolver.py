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
            except requests.RequestException as e:
                logger.warning(f"Lỗi kết nối mạng khi kiểm tra ID {test_id} (lần {attempt+1}): {e}")
                self.has_network_error = True
                time.sleep(self.delay)
            except Exception as e:
                logger.warning(f"Lỗi không xác định khi kiểm tra ID {test_id}: {e}")
                time.sleep(self.delay)
        return None

    def resolve(self, target_date_str):
        """Tìm chính xác ID của ngày được yêu cầu"""
        target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        
        # 1. Tính toán ước lượng dựa trên ngày làm việc
        bus_days = self._count_business_days(self.ref_date, target_date)
        estimated_id = self.ref_id + bus_days
        
        logger.info(f"Đang dò tìm ID cho ngày {target_date_str}. Ước lượng ban đầu: {estimated_id}")
        
        # Reset cờ lỗi mạng trước mỗi phiên resolve
        self.has_network_error = False
        
        current_id = estimated_id
        visited = set()
        resolved_id = None
        
        # 2. Vòng lặp tinh chỉnh để tìm ID khớp chính xác ngày
        for step in range(15): # Giới hạn tối đa 15 bước nhảy để tránh vòng lặp vô hạn
            if current_id in visited:
                break
            visited.add(current_id)
            
            actual_date = self._check_id_date(current_id)
            if not actual_date:
                # Nếu đụng ngày 404 (ngày nghỉ/lễ) hoặc lỗi mạng, thử dò các ID lân cận
                logger.debug(f"ID {current_id} không xác định ngày thực tế, đang dò các ID lân cận...")
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