-- =========================================================================
-- GRAFANA & DATAGRIP SQL TEMPLATES FOR SGX DERIVATIVES LAKEHOUSE
-- File này chứa các câu lệnh SQL mẫu để kiểm tra dữ liệu và cấu hình biểu đồ trên Grafana
-- =========================================================================

-- -------------------------------------------------------------------------
-- PHẦN I: CÁC CÂU LỆNH KIỂM TRA DỮ LIỆU (Chạy trên DataGrip / DBeaver)
-- Dùng để xác nhận dữ liệu đã được nạp thành công từ MinIO vào Postgres
-- -------------------------------------------------------------------------

-- 1. Kiểm tra tổng số bản ghi và phạm vi ngày dữ liệu
SELECT 
    COUNT(*) as total_rows,
    MIN("TradeDateParsed") as start_date,
    MAX("TradeDateParsed") as end_date,
    COUNT(DISTINCT "Symbol") as unique_symbols
FROM ticks_summary;

-- 2. Kiểm tra danh sách các mã Symbol phái sinh hiện có và lượng volume tương ứng
SELECT 
    "Symbol", 
    SUM("GroupVolume") as total_volume, 
    COUNT(*) as row_count
FROM ticks_summary
GROUP BY "Symbol"
ORDER BY total_volume DESC;

-- 3. Xem thử 10 dòng dữ liệu bất kỳ để hiểu cấu trúc
SELECT * 
FROM ticks_summary 
LIMIT 10;


-- -------------------------------------------------------------------------
-- PHẦN II: CẤU HÌNH BỘ LỌC ĐỘNG (GRAFANA VARIABLES)
-- Thực hiện tạo các biến này trong cài đặt của Dashboard (Dashboard Settings -> Variables)
-- -------------------------------------------------------------------------
/*
1. Biến $symbol (Loại Query):
   - Name: symbol
   - Type: Query
   - Data source: Chọn Postgres của bạn
   - Query: SELECT DISTINCT "Symbol" FROM ticks_summary ORDER BY "Symbol";
   - Selection Options: Bật Multi-value và Bật Include All option

2. Biến $message_code (Loại Query):
   - Name: message_code
   - Type: Query
   - Query: SELECT DISTINCT "MessageCode" FROM ticks_summary ORDER BY "MessageCode";
   - Selection Options: Bật Multi-value và Bật Include All option
*/


-- -------------------------------------------------------------------------
-- PHẦN III: CÂU LỆNH SQL DÙNG TRONG GRAFANA PANELS
-- Dán các câu lệnh này vào SQL Editor của từng Panel trong Grafana
-- -------------------------------------------------------------------------

-- 1. PANEL STAT: Tổng khối lượng giao dịch (Total Volume)
-- Sử dụng: Stat visualization
SELECT SUM("GroupVolume") as "Total Volume"
FROM ticks_summary
WHERE 
    $__timeFilter("TradeDateParsed")
    AND "Symbol" IN (${symbol:sqlstring})
    AND "MessageCode" IN (${message_code:sqlstring});


-- 2. PANEL STAT: Tổng số lượt sự kiện/ticks (Total Ticks)
-- Sử dụng: Stat visualization
SELECT SUM("GroupTradeCount") as "Total Ticks"
FROM ticks_summary
WHERE 
    $__timeFilter("TradeDateParsed")
    AND "Symbol" IN (${symbol:sqlstring})
    AND "MessageCode" IN (${message_code:sqlstring});


-- 3. PANEL TIME SERIES: Biến động giá trung bình & Khối lượng (Khuyên dùng trục Y kép)
-- Sử dụng: Time Series visualization
-- Hướng dẫn trục Y kép & Dạng cột cho Volume:
--   1. Đi tới "Overrides" bên phải -> Click "+ Add field override" -> Chọn "Fields with name" -> Chọn "Volume"
--   2. Tại "Graph styles > Style", chọn "Bars" (để Volume hiển thị dạng cột)
--   3. Click "+ Add override property" -> Chọn "Standard options > Axis > Placement" -> Đặt là "Right" (Trục Y bên phải)
SELECT 
    -- Ghép Ngày và Giờ để tạo trục thời gian liên tục cho đồ thị
    ("TradeDateParsed" + CAST("Hour" || ':00:00' AS interval)) as time,
    -- Tính giá trung bình khớp lệnh có trọng số (VWAP)
    ROUND(SUM("GroupPriceVolume") / NULLIF(SUM("GroupVolume"), 0), 4) as "Weighted Avg Price",
    -- Tổng Volume tương ứng
    SUM("GroupVolume") as "Volume"
FROM ticks_summary
WHERE 
    $__timeFilter("TradeDateParsed")
    AND "Symbol" IN (${symbol:sqlstring})
    AND "MessageCode" IN (${message_code:sqlstring})
GROUP BY time
ORDER BY time ASC;


-- 4. PANEL BAR CHART: Phân bổ thanh khoản theo Khung giờ (Intraday Liquidity)
-- Sử dụng: Bar Chart visualization (Trục X chọn cột Hour, Trục Y chọn cột Total Volume)
SELECT 
    "Hour", 
    SUM("GroupVolume") as "Total Volume"
FROM ticks_summary
WHERE 
    $__timeFilter("TradeDateParsed")
    AND "Symbol" IN (${symbol:sqlstring})
    AND "MessageCode" IN (${message_code:sqlstring})
GROUP BY "Hour"
ORDER BY "Hour" ASC;


-- 5. PANEL PIE CHART: Tỷ lệ giao dịch giữa các mã chứng khoán (Market Share)
-- Sử dụng: Pie Chart / Donut Chart visualization
SELECT 
    NOW() as time,                    -- Trục thời gian giả lập
    "Symbol" as metric,               -- Nhãn (tên series)
    SUM("GroupVolume") as value       -- Giá trị biểu diễn
FROM ticks_summary
WHERE 
    $__timeFilter("TradeDateParsed")
    AND "Symbol" IN (${symbol:sqlstring})
    AND "MessageCode" IN (${message_code:sqlstring})
GROUP BY "Symbol"
ORDER BY value DESC;



-- 6. PANEL TABLE: Bảng chi tiết lịch sử giao dịch
-- Sử dụng: Table visualization (Đặt ở cuối trang Dashboard để tra cứu)
SELECT 
    "TradeDateParsed" as "Date",
    "Hour" as "Hour",
    "Symbol" as "Symbol",
    "MessageCode" as "Type",
    "GroupVolume" as "Volume",
    "GroupTradeCount" as "Ticks",
    ROUND("GroupPriceVolume" / NULLIF("GroupVolume", 0), 4) as "Avg Price"
FROM ticks_summary
WHERE 
    $__timeFilter("TradeDateParsed")
    AND "Symbol" IN (${symbol:sqlstring})
    AND "MessageCode" IN (${message_code:sqlstring})
ORDER BY "TradeDateParsed" DESC, "Hour" DESC
LIMIT 500;
