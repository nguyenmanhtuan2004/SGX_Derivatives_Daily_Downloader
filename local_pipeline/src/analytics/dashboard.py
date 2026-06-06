import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import configparser
import os

# Cấu hình giao diện Streamlit
st.set_page_config(
    page_title="SGX Derivatives Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load cấu hình MinIO (Không dùng cache_resource để luôn đọc Delta Lake metadata mới nhất sau khi Spark ETL chạy)
def get_duckdb_connection():
    config = configparser.ConfigParser()
    config_path = "config/config.ini"
    if not os.path.exists(config_path):
        config_path = "../../config/config.ini"
        
    config.read(config_path)
    
    endpoint = config.get("minio", "endpoint").replace("http://", "")
    access_key = config.get("minio", "access_key")
    secret_key = config.get("minio", "secret_key")
    bucket = config.get("minio", "bucket")
    
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL delta; LOAD delta;")
    
    con.execute(f"""
        CREATE SECRET secret_minio (
            TYPE S3,
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{endpoint}',
            USE_SSL false,
            URL_STYLE 'path'
        );
    """)
    return con, bucket

try:
    con, bucket = get_duckdb_connection()
    ticks_path = f"s3://{bucket}/processed/ticks"
    tc_path = f"s3://{bucket}/processed/trade_cancellations"
except Exception as e:
    st.error(f"Không thể kết nối đến MinIO: {e}")
    st.stop()

# --- SIDEBAR: BỘ LỌC THỜI GIAN (Hỗ trợ chạy nhiều tháng/ngày) ---
st.sidebar.header("🎛️ Bộ lọc thời gian")

# Lấy danh sách các Phân vùng (Year-Month) thực tế từ Delta Lake
periods_list = ["Tất cả các tháng"]
try:
    partitions_df = con.execute(f"""
        SELECT DISTINCT year, month 
        FROM delta_scan('{ticks_path}') 
        ORDER BY year DESC, month DESC
    """).df()
    
    for _, row in partitions_df.iterrows():
        periods_list.append(f"{int(row['year'])}-{int(row['month']):02d}")
except Exception:
    pass

selected_period = st.sidebar.selectbox("Chọn tháng báo cáo:", periods_list)

# Tạo mệnh đề SQL WHERE dựa trên bộ lọc phân vùng để tối ưu hóa truy vấn (Partition Pruning)
where_clause = "1=1"
where_clause_tc = "1=1"
if selected_period != "Tất cả các tháng":
    y, m = map(int, selected_period.split("-"))
    where_clause = f"year = {y} AND month = {m}"
    where_clause_tc = f"year = {y} AND month = {m}"

# --- Tiêu đề Dashboard ---
st.title("📊 SGX Derivatives Market Lakehouse Dashboard")
st.markdown("### Phân tích trực quan dữ liệu giao dịch phái sinh từ Delta Lake (MinIO)")

# --- Load dữ liệu tổng quan ---
@st.cache_data(ttl=30)
def load_overview_data(filter_sql, filter_sql_tc):
    # Tổng số bản ghi giao dịch
    total_ticks = con.execute(f"SELECT COUNT(*) FROM delta_scan('{ticks_path}') WHERE {filter_sql}").fetchone()[0]
    # Tổng khối lượng giao dịch
    total_volume = con.execute(f"SELECT SUM(TradeVolume) FROM delta_scan('{ticks_path}') WHERE {filter_sql}").fetchone()[0]
    # Tổng số giao dịch hủy
    total_cancelled = con.execute(f"SELECT COUNT(*) FROM delta_scan('{tc_path}') WHERE {filter_sql_tc}").fetchone()[0]
    
    return total_ticks, total_volume, total_cancelled

try:
    total_ticks, total_volume, total_cancelled = load_overview_data(where_clause, where_clause_tc)
except Exception as e:
    st.warning("Chưa có dữ liệu Delta Lake hoặc bảng trống. Hãy chạy Spark ETL ít nhất một lần trước.")
    st.error(str(e))
    st.stop()

# --- Hiển thị các Metric Cards ---
col1, col2, col3 = st.columns(3)
col1.metric("Tổng Số Bản Ghi Ticks", f"{total_ticks:,}")
col2.metric("Tổng Khối Lượng Giao Dịch", f"{int(total_volume):,}" if total_volume else "0")
col3.metric("Số Giao Dịch Bị Hủy", f"{total_cancelled:,}")

st.markdown("---")

# --- BIỂU ĐỒ XU HƯỚNG THEO THỜI GIAN (Hữu ích khi nạp nhiều ngày/tháng) ---
st.subheader("📅 Xu hướng khối lượng giao dịch hàng ngày (Daily Trading Volume)")
df_daily_trend = con.execute(f"""
    SELECT TradeDateParsed, SUM(TradeVolume) as DailyVolume
    FROM delta_scan('{ticks_path}')
    WHERE {where_clause}
    GROUP BY TradeDateParsed
    ORDER BY TradeDateParsed
""").df()

if not df_daily_trend.empty:
    fig_daily = px.line(
        df_daily_trend,
        x="TradeDateParsed",
        y="DailyVolume",
        markers=True,
        labels={"TradeDateParsed": "Ngày giao dịch", "DailyVolume": "Tổng khối lượng"},
        title="Biến động khối lượng giao dịch qua các ngày",
        color_discrete_sequence=["#1f77b4"]
    )
    st.plotly_chart(fig_daily, use_container_width=True)
else:
    st.info("Không có dữ liệu xu hướng ngày.")

st.markdown("---")

# --- Layout Biểu đồ Phân tích Sản phẩm ---
col_left, col_right = st.columns([1, 1])

with col_left:
    st.subheader("🏆 Top 10 sản phẩm giao dịch nhiều nhất")
    # Truy vấn top 10 sản phẩm của tháng đã lọc
    df_top_symbols = con.execute(f"""
        SELECT Symbol, SUM(TradeVolume) as TotalVolume, AVG(TradePrice) as AvgPrice
        FROM delta_scan('{ticks_path}')
        WHERE {where_clause}
        GROUP BY Symbol
        ORDER BY TotalVolume DESC
        LIMIT 10
    """).df()
    
    if not df_top_symbols.empty and "Symbol" in df_top_symbols.columns:
        fig_bar = px.bar(
            df_top_symbols,
            x="Symbol",
            y="TotalVolume",
            color="TotalVolume",
            labels={"TotalVolume": "Khối Lượng Giao Dịch", "Symbol": "Mã Sản Phẩm"},
            color_continuous_scale="Viridis",
            title="Khối lượng giao dịch theo mã"
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("Không có dữ liệu sản phẩm.")

with col_right:
    st.subheader("📈 Phân bổ giá giao dịch trung bình")
    if not df_top_symbols.empty and "Symbol" in df_top_symbols.columns:
        fig_scatter = px.scatter(
            df_top_symbols,
            x="Symbol",
            y="AvgPrice",
            size="TotalVolume",
            color="AvgPrice",
            color_continuous_scale="Plasma",
            labels={"AvgPrice": "Giá Trung Bình (USD)", "Symbol": "Mã Sản Phẩm"},
            title="Giá giao dịch trung bình so với khối lượng"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("Không có dữ liệu phân bổ giá.")

st.markdown("---")

# --- Bộ lọc chi tiết theo Symbol ---
st.subheader("🔍 Truy vấn chi tiết theo Mã Sản Phẩm")

# Lấy danh sách Symbols duy nhất của tháng đã lọc
symbols_list = []
if not df_top_symbols.empty and "Symbol" in df_top_symbols.columns:
    symbols_list = con.execute(f"""
        SELECT DISTINCT Symbol 
        FROM delta_scan('{ticks_path}') 
        WHERE {where_clause}
        ORDER BY Symbol
    """).df()["Symbol"].tolist()

if symbols_list:
    selected_symbol = st.selectbox("Chọn mã sản phẩm để xem chi tiết:", symbols_list)
    
    if selected_symbol:
        # Truy vấn chi tiết cho mã đã chọn
        df_symbol = con.execute(f"""
            SELECT Symbol, TradeDateParsed, TradeTime, TradePrice, TradeVolume
            FROM delta_scan('{ticks_path}')
            WHERE Symbol = '{selected_symbol}' AND {where_clause}
            ORDER BY TradeDateParsed DESC, TradeTime DESC
            LIMIT 100
        """).df()
        
        st.write(f"Dữ liệu giao dịch gần đây của mã **{selected_symbol}** (Tối đa 100 dòng):")
        st.dataframe(df_symbol, use_container_width=True)
        
        # Biểu đồ biến động giá của mã được chọn
        if not df_symbol.empty:
            # Sắp xếp thời gian tăng dần để vẽ đường xu hướng chính xác
            df_symbol_sorted = df_symbol.copy()
            # Kết hợp Ngày + Giờ để làm trục X tăng dần khi nạp nhiều ngày/tháng
            df_symbol_sorted["DateTimeString"] = df_symbol_sorted["TradeDateParsed"].astype(str) + " " + df_symbol_sorted["TradeTime"]
            df_symbol_sorted = df_symbol_sorted.sort_values(by="DateTimeString")
            
            fig_price_trend = px.line(
                df_symbol_sorted,
                x="DateTimeString",
                y="TradePrice",
                markers=True,
                title=f"Biến động giá giao dịch của mã {selected_symbol}",
                labels={"TradePrice": "Giá", "DateTimeString": "Thời gian (Ngày Giờ)"}
            )
            st.plotly_chart(fig_price_trend, use_container_width=True)
else:
    st.info("Không có mã sản phẩm nào khả dụng.")

st.markdown("---")

# --- Thống kê giao dịch bị hủy ---
st.subheader("❌ Thống kê giao dịch bị hủy (Trade Cancellations)")
df_cancelled_all = con.execute(f"SELECT * FROM delta_scan('{tc_path}') WHERE {where_clause_tc}").df()

if not df_cancelled_all.empty:
    st.write("Bảng dữ liệu giao dịch bị hủy ghi nhận trên hệ thống:")
    st.dataframe(df_cancelled_all, use_container_width=True)
    
    fig_cancel = px.bar(
        df_cancelled_all.groupby("Symbol")["Volume"].sum().reset_index(),
        x="Symbol",
        y="Volume",
        color="Symbol",
        title="Tổng khối lượng bị hủy theo mã sản phẩm"
    )
    st.plotly_chart(fig_cancel, use_container_width=True)
else:
    st.info("Không ghi nhận giao dịch hủy nào trong khoảng thời gian đã chọn.")
