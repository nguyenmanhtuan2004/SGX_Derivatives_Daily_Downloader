// Global Chart Instances
let chartHourlyTrend = null;
let chartProductVolume = null;
let chartMessageDist = null;

const API_BASE_URL = window.location.origin;

const dateSelect = document.getElementById("date-select");
const btnRefresh = document.getElementById("btn-refresh");
const btnClearCache = document.getElementById("btn-clear-cache");
const loadingOverlay = document.getElementById("loading-overlay");
const loadingText = document.getElementById("loading-text");
const errorAlert = document.getElementById("error-alert");
const errorMessage = document.getElementById("error-message");

// Summary Metrics Elements
const metricVolume = document.getElementById("metric-volume");
const metricTrades = document.getElementById("metric-trades");
const metricPrice = document.getElementById("metric-price");

// Init application on load
window.addEventListener("DOMContentLoaded", () => {
    initCharts();
    fetchDates();
    setupEventListeners();
});

// Setup responsive behavior for charts
window.addEventListener("resize", () => {
    chartHourlyTrend && chartHourlyTrend.resize();
    chartProductVolume && chartProductVolume.resize();
    chartMessageDist && chartMessageDist.resize();
});

// Initialize ECharts instances
function initCharts() {
    chartHourlyTrend = echarts.init(document.getElementById("chart-hourly-trend"));
    chartProductVolume = echarts.init(document.getElementById("chart-product-volume"));
    chartMessageDist = echarts.init(document.getElementById("chart-message-distribution"));
}

// Setup Event Listeners
function setupEventListeners() {
    dateSelect.addEventListener("change", (e) => {
        if (e.target.value) {
            loadDashboardData(e.target.value);
        }
    });

    btnRefresh.addEventListener("click", () => {
        const currentDate = dateSelect.value;
        if (currentDate) {
            loadDashboardData(currentDate, true);
        }
    });

    btnClearCache.addEventListener("click", async () => {
        const currentDate = dateSelect.value;
        if (!currentDate) return;
        
        showLoading("Đang dọn dẹp cache dữ liệu...");
        try {
            const response = await fetch(`${API_BASE_URL}/api/clear-cache?date=${currentDate}`, { method: "POST" });
            if (response.ok) {
                logger("Cache cleared successfully.");
                loadDashboardData(currentDate);
            } else {
                showError("Không thể xóa cache dữ liệu.");
                hideLoading();
            }
        } catch (err) {
            showError("Có lỗi xảy ra khi gọi API xóa cache.");
            hideLoading();
        }
    });
}

// Fetch list of dates from backend
async function fetchDates() {
    showLoading("Đang quét các phân vùng ngày trên S3...");
    try {
        const response = await fetch(`${API_BASE_URL}/api/dates`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        const dates = await response.json();
        
        // Populate dropdown
        dateSelect.innerHTML = "";
        
        if (dates.length === 0) {
            const opt = document.createElement("option");
            opt.text = "Không tìm thấy dữ liệu";
            opt.disabled = true;
            dateSelect.add(opt);
            hideLoading();
            showError("Không tìm thấy bất kỳ phân vùng ngày nào trong thư mục processed/ticks_summary/ trên S3.");
            return;
        }

        dates.forEach(date => {
            const opt = document.createElement("option");
            opt.value = date;
            opt.text = formatDateString(date);
            dateSelect.add(opt);
        });

        // Trigger load for the first (most recent) date
        dateSelect.value = dates[0];
        loadDashboardData(dates[0]);
        
    } catch (err) {
        logger("Error loading dates:", err);
        showError("Lỗi kết nối đến Backend API. Hãy chắc chắn uvicorn server đang chạy tại localhost:8000.");
        hideLoading();
    }
}

// Load metrics & trends for the specified date
async function loadDashboardData(dateStr, forceRefresh = false) {
    showLoading(`Đang nạp dữ liệu ngày ${formatDateString(dateStr)}...`);
    hideError();

    try {
        if (forceRefresh) {
            // First clear cache for this specific date
            await fetch(`${API_BASE_URL}/api/clear-cache?date=${dateStr}`, { method: "POST" });
        }

        const response = await fetch(`${API_BASE_URL}/api/dashboard-data?date=${dateStr}`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        updateSummaryMetrics(data.summary);
        renderHourlyTrendChart(data.hourly_trend);
        renderProductVolumeChart(data.product_distribution);
        renderMessageDistChart(data.message_distribution);
        
        hideLoading();
    } catch (err) {
        logger("Error fetching dashboard data:", err);
        showError(`Không thể tải dữ liệu cho ngày ${formatDateString(dateStr)} từ S3.`);
        hideLoading();
        clearMetrics();
    }
}

// Update text statistics in the top cards
function updateSummaryMetrics(summary) {
    metricVolume.textContent = formatNumber(summary.total_volume);
    metricTrades.textContent = formatNumber(summary.total_trades);
    metricPrice.textContent = summary.avg_price.toLocaleString("vi-VN", { minimumFractionDigits: 2, maximumFractionDigits: 4 });
}

function clearMetrics() {
    metricVolume.textContent = "-";
    metricTrades.textContent = "-";
    metricPrice.textContent = "-";
}

// Chart Renderings
function renderHourlyTrendChart(hourlyData) {
    const hours = hourlyData.map(item => `${item.Hour}:00`);
    const volumes = hourlyData.map(item => item.HourlyVolume);
    const avgPrices = hourlyData.map(item => item.AvgPrice > 0 ? parseFloat(item.AvgPrice.toFixed(4)) : null);

    const option = {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'cross',
                label: {
                    backgroundColor: '#0284c7'
                }
            },
            backgroundColor: 'rgba(255, 255, 255, 0.96)',
            borderColor: '#e2e8f0',
            textStyle: { color: '#0f172a' },
            shadowColor: 'rgba(148, 163, 184, 0.1)',
            shadowBlur: 10
        },
        grid: {
            top: '12%',
            left: '4%',
            right: '4%',
            bottom: '10%',
            containLabel: true
        },
        xAxis: [
            {
                type: 'category',
                data: hours,
                axisLine: { lineStyle: { color: '#cbd5e1' } },
                axisLabel: { color: '#475569', fontSize: 10 }
            }
        ],
        yAxis: [
            {
                type: 'value',
                name: 'Volume',
                nameTextStyle: { color: '#0284c7', fontWeight: 'bold' },
                axisLabel: { color: '#475569' },
                splitLine: { lineStyle: { color: '#f1f5f9' } }
            },
            {
                type: 'value',
                name: 'Average Price',
                nameTextStyle: { color: '#10b981', fontWeight: 'bold' },
                axisLabel: { color: '#475569' },
                splitLine: { show: false }
            }
        ],
        series: [
            {
                name: 'Volume',
                type: 'bar',
                yAxisIndex: 0,
                data: volumes,
                itemStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: '#38bdf8' },
                        { offset: 1, color: '#0284c7' }
                    ]),
                    borderRadius: [4, 4, 0, 0]
                }
            },
            {
                name: 'Price',
                type: 'line',
                yAxisIndex: 1,
                data: avgPrices,
                symbol: 'circle',
                symbolSize: 6,
                connectNulls: true,
                itemStyle: { color: '#10b981' },
                lineStyle: { width: 3 }
            }
        ]
    };

    chartHourlyTrend.setOption(option);
}

function renderProductVolumeChart(prodData) {
    // Show top 8 symbols
    const topData = prodData.slice(0, 8).reverse();
    const symbols = topData.map(item => item.Symbol);
    const volumes = topData.map(item => item.TradeVolume);

    const option = {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'shadow' },
            backgroundColor: 'rgba(255, 255, 255, 0.96)',
            borderColor: '#e2e8f0',
            textStyle: { color: '#0f172a' },
            shadowColor: 'rgba(148, 163, 184, 0.1)',
            shadowBlur: 10
        },
        grid: {
            top: '3%',
            left: '3%',
            right: '8%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'value',
            axisLabel: { color: '#475569' },
            splitLine: { lineStyle: { color: '#f1f5f9' } }
        },
        yAxis: {
            type: 'category',
            data: symbols,
            axisLine: { lineStyle: { color: '#cbd5e1' } },
            axisLabel: { color: '#0f172a', fontWeight: 'semibold' }
        },
        series: [
            {
                name: 'Volume',
                type: 'bar',
                data: volumes,
                itemStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                        { offset: 0, color: '#38bdf8' },
                        { offset: 1, color: '#0284c7' }
                    ]),
                    borderRadius: [0, 4, 4, 0]
                },
                label: {
                    show: true,
                    position: 'right',
                    color: '#475569',
                    fontSize: 10,
                    formatter: (params) => formatNumber(params.value)
                }
            }
        ]
    };

    chartProductVolume.setOption(option);
}

function renderMessageDistChart(msgData) {
    // Standardize labels
    const labelMapping = {
        "T": "Trade (Khớp Lệnh)",
        "Q": "Quote (Báo Giá)",
        "C": "Cancel (Hủy Lệnh)"
    };
    
    const formattedData = msgData.map(item => ({
        name: labelMapping[item.MessageCode] || `Code ${item.MessageCode}`,
        value: item.Count
    }));

    const option = {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'item',
            backgroundColor: 'rgba(255, 255, 255, 0.96)',
            borderColor: '#e2e8f0',
            textStyle: { color: '#0f172a' },
            shadowColor: 'rgba(148, 163, 184, 0.1)',
            shadowBlur: 10
        },
        legend: {
            bottom: '5%',
            left: 'center',
            textStyle: { color: '#475569' }
        },
        series: [
            {
                name: 'Message Code',
                type: 'pie',
                radius: ['40%', '70%'],
                center: ['50%', '42%'],
                avoidLabelOverlap: false,
                itemStyle: {
                    borderRadius: 8,
                    borderColor: '#ffffff',
                    borderWidth: 2
                },
                label: {
                    show: false,
                    position: 'center'
                },
                emphasis: {
                    label: {
                        show: true,
                        fontSize: 16,
                        fontWeight: 'bold',
                        color: '#0f172a'
                    }
                },
                labelLine: {
                    show: false
                },
                data: formattedData,
                color: ['#0ea5e9', '#06b6d4', '#f43f5e', '#f59e0b', '#10b981']
            }
        ]
    };

    chartMessageDist.setOption(option);
}

// Helpers & Utilities
function showLoading(text) {
    loadingText.textContent = text;
    loadingOverlay.classList.remove("hidden");
}

function hideLoading() {
    loadingOverlay.classList.add("hidden");
}

function showError(msg) {
    errorMessage.textContent = msg;
    errorAlert.classList.remove("hidden");
}

function hideError() {
    errorAlert.classList.add("hidden");
}

function formatDateString(dateStr) {
    // input is YYYY-MM-DD
    const parts = dateStr.split("-");
    if (parts.length === 3) {
        return `Ngày ${parts[2]}/${parts[1]}/${parts[0]}`;
    }
    return dateStr;
}

function formatNumber(num) {
    if (num === null || num === undefined) return "-";
    return Number(num).toLocaleString("en-US");
}

function logger(...args) {
    console.log("[Dashboard]", ...args);
}
