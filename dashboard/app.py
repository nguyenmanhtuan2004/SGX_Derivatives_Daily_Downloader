import os
import io
import csv
import json
import logging
from typing import List, Dict, Any, Optional
import boto3
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from dotenv import load_dotenv

# Load env variables if .env exists
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dashboard_api")

app = FastAPI(
    title="SGX Derivatives Dashboard API",
    description="API to serve processed tick and trade cancellation data from AWS S3",
    version="1.0.0"
)

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "sgx-derivatives-daily-data-079")
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Initialize AWS S3 client
def get_s3_client():
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region_name = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")
    
    if aws_access_key and aws_secret_key:
        logger.info("Initializing S3 client using explicit credentials from environment variables.")
        return boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )
    else:
        logger.info("Initializing S3 client using default credential chain (AWS CLI config/IAM Role).")
        return boto3.client("s3")

s3_client = get_s3_client()

def list_available_dates_from_s3() -> List[str]:
    """Lists available dates by scanning partition prefixes in s3://bucket/processed/ticks_summary/"""
    dates = set()
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        # List subfolders under processed/ticks_summary/
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix="processed/ticks_summary/", Delimiter="/")
        for page in pages:
            if "CommonPrefixes" in page:
                for prefix in page["CommonPrefixes"]:
                    # prefix is like "processed/ticks_summary/TradeDateParsed=YYYY-MM-DD/"
                    folder_name = prefix["Prefix"].strip("/").split("/")[-1]
                    if "TradeDateParsed=" in folder_name:
                        date_str = folder_name.split("=")[-1]
                        dates.add(date_str)
        return sorted(list(dates), reverse=True)
    except Exception as e:
        logger.error(f"Error listing dates from S3 bucket {BUCKET_NAME}: {e}")
        return []

def fetch_and_aggregate_s3_data(date_str: str) -> Optional[Dict[str, Any]]:
    """Downloads all CSV summary files for a given date partition and computes final response dashboard-data without losing data"""
    prefix = f"processed/ticks_summary/TradeDateParsed={date_str}/"
    logger.info(f"Listing S3 files in prefix: {prefix}")
    
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        
        # Fallback to raw ticks if summary is not found
        if "Contents" not in response:
            logger.warning(f"No summary files found for prefix: {prefix}. Trying raw ticks fallback.")
            prefix = f"processed/ticks/TradeDateParsed={date_str}/"
            response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
            if "Contents" not in response:
                logger.warning(f"No raw ticks found for prefix: {prefix}")
                return None
                
        csv_files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".csv")]
        if not csv_files:
            logger.warning(f"No CSV files found for date {date_str} under prefix: {prefix}")
            return None
            
        # Initialize aggregates
        total_volume = 0
        total_trades = 0
        total_price_volume = 0.0
        total_price_sum = 0.0
        
        # Product distribution: symbol -> {volume, count}
        product_stats = {}
        
        # Hourly trend: hour -> {volume, count, price_sum}
        hourly_stats = {str(h).zfill(2): {"volume": 0, "count": 0, "price_sum": 0.0} for h in range(24)}
        
        # Message distribution: msg_code -> count
        message_stats = {}
        
        # Process each CSV file (could be one summary.csv or multiple part-*.csv files)
        for s3_key in csv_files:
            logger.info(f"Streaming and parsing S3 object: {s3_key}")
            csv_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            
            stream = io.TextIOWrapper(csv_obj["Body"], encoding='utf-8')
            reader = csv.DictReader(stream)
            
            # Check if this is a summary file or a raw tick file
            is_summary = "ticks_summary" in s3_key
            
            for row in reader:
                symbol = row.get("Symbol") or row.get("Comm")
                if not symbol or symbol == "Symbol" or symbol == "Comm":
                    continue
                    
                if is_summary:
                    # Summary format parsing
                    hour = row.get("Hour", "").zfill(2)
                    msg_code = row.get("MessageCode", "")
                    
                    try:
                        group_volume = int(row.get("GroupVolume", 0) or 0)
                    except ValueError:
                        group_volume = 0
                        
                    try:
                        group_trade_count = int(row.get("GroupTradeCount", 0) or 0)
                    except ValueError:
                        group_trade_count = 0
                        
                    try:
                        group_price_volume = float(row.get("GroupPriceVolume", 0.0) or 0.0)
                    except ValueError:
                        group_price_volume = 0.0
                        
                    try:
                        group_price_sum = float(row.get("GroupPriceSum", 0.0) or 0.0)
                    except ValueError:
                        group_price_sum = 0.0
                        
                    total_volume += group_volume
                    total_trades += group_trade_count
                    total_price_volume += group_price_volume
                    total_price_sum += group_price_sum
                    
                    if symbol not in product_stats:
                        product_stats[symbol] = {"TradeVolume": 0, "TradeCount": 0}
                    product_stats[symbol]["TradeVolume"] += group_volume
                    product_stats[symbol]["TradeCount"] += group_trade_count
                    
                    if hour.isdigit() and int(hour) < 24:
                        hourly_stats[hour]["volume"] += group_volume
                        hourly_stats[hour]["count"] += group_trade_count
                        hourly_stats[hour]["price_sum"] += group_price_sum
                        
                    if msg_code:
                        message_stats[msg_code] = message_stats.get(msg_code, 0) + group_trade_count
                else:
                    # Raw tick format parsing (Fallback)
                    try:
                        volume = int(row.get("TradeVolume", 0) or int(row.get("Volume", 0) or 0))
                    except ValueError:
                        volume = 0
                        
                    try:
                        price = float(row.get("TradePrice", 0.0) or float(row.get("Price", 0.0) or 0.0))
                    except ValueError:
                        price = 0.0
                        
                    msg_code = row.get("MessageCode", "") or row.get("Msg_Code", "")
                    trade_time = row.get("TradeTime", "") or row.get("Log_Time", "")
                    
                    total_volume += volume
                    total_trades += 1
                    total_price_volume += price * volume
                    total_price_sum += price
                    
                    if symbol not in product_stats:
                        product_stats[symbol] = {"TradeVolume": 0, "TradeCount": 0}
                    product_stats[symbol]["TradeVolume"] += volume
                    product_stats[symbol]["TradeCount"] += 1
                    
                    hour = trade_time.replace(":", "")[:2].zfill(2)
                    if hour.isdigit() and int(hour) < 24:
                        hourly_stats[hour]["volume"] += volume
                        hourly_stats[hour]["count"] += 1
                        hourly_stats[hour]["price_sum"] += price
                        
                    if msg_code:
                        message_stats[msg_code] = message_stats.get(msg_code, 0) + 1
                        
            stream.close()
            
        if total_trades == 0:
            return None
            
        # Format outputs
        avg_price = total_price_volume / total_volume if total_volume > 0 else (total_price_sum / total_trades)
        
        # Format product_distribution
        prod_dist = []
        for sym, stats in product_stats.items():
            prod_dist.append({
                "Symbol": sym,
                "TradeVolume": stats["TradeVolume"],
                "TradeCount": stats["TradeCount"]
            })
        prod_dist = sorted(prod_dist, key=lambda x: x["TradeVolume"], reverse=True)
        
        # Format hourly_trend
        hourly_trend = []
        for hr, stats in sorted(hourly_stats.items()):
            hourly_trend.append({
                "Hour": hr,
                "HourlyVolume": stats["volume"],
                "HourlyTradeCount": stats["count"],
                "AvgPrice": round(stats["price_sum"] / stats["count"], 4) if stats["count"] > 0 else 0.0
            })
            
        # Format message_distribution
        msg_dist = []
        for msg, count in message_stats.items():
            msg_dist.append({
                "MessageCode": msg,
                "Count": count
            })
            
        return {
            "summary": {
                "total_volume": total_volume,
                "total_trades": total_trades,
                "avg_price": round(avg_price, 4)
            },
            "product_distribution": prod_dist,
            "hourly_trend": hourly_trend,
            "message_distribution": msg_dist
        }
        
    except Exception as e:
        logger.error(f"Error reading and aggregating S3 files for date {date_str}: {e}")
        return None

@app.get("/api/dates", response_model=List[str])
def get_dates():
    """Returns a list of all dates available in the S3 processed folder"""
    dates = list_available_dates_from_s3()
    if not dates:
        # Fallback to local cache files names if S3 fails
        logger.warning("S3 scanning returned empty. Checking local cache directory.")
        dates = []
        for file in os.listdir(CACHE_DIR):
            if file.startswith("data_") and file.endswith(".json"):
                dates.append(file.replace("data_", "").replace(".json", ""))
        dates = sorted(list(set(dates)), reverse=True)
    return dates

@app.get("/api/dashboard-data")
def get_dashboard_data(date: str = Query(..., description="Date partition formatted as YYYY-MM-DD")):
    """Returns all aggregated metrics for the dashboard on a specific date, utilizing local cache if available"""
    cache_file = os.path.join(CACHE_DIR, f"data_{date}.json")
    
    # Check if cache file exists
    if os.path.exists(cache_file):
        logger.info(f"Serving aggregated data for date {date} from local cache.")
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading cache file {cache_file}: {e}")
            
    # Cache miss - fetch from S3 using memory efficient streaming
    logger.info(f"Cache miss for date {date}. Streaming CSV files from S3...")
    aggregates = fetch_and_aggregate_s3_data(date)
    
    if aggregates is None:
        raise HTTPException(
            status_code=404, 
            detail=f"No data found for date {date} in S3 bucket {BUCKET_NAME}."
        )
        
    # Save to cache
    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(aggregates, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved aggregated data for date {date} to local cache.")
    except Exception as e:
        logger.error(f"Error saving to cache file {cache_file}: {e}")
        
    return aggregates

@app.post("/api/clear-cache")
def clear_cache(date: Optional[str] = None):
    """Utility endpoint to clear the cached aggregated files"""
    try:
        if date:
            cache_file = os.path.join(CACHE_DIR, f"data_{date}.json")
            if os.path.exists(cache_file):
                os.remove(cache_file)
                return {"status": "success", "message": f"Cache cleared for date {date}"}
            else:
                return {"status": "not_found", "message": f"No cache file found for date {date}"}
        else:
            files_removed = 0
            for file in os.listdir(CACHE_DIR):
                if file.endswith(".json"):
                    os.remove(os.path.join(CACHE_DIR, file))
                    files_removed += 1
            return {"status": "success", "message": f"Cleared all {files_removed} cache files"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Setup frontend static files mounting
frontend_path = os.path.join(os.path.dirname(__file__), "frontend")
os.makedirs(frontend_path, exist_ok=True)

# If frontend files exist, serve them. Otherwise, expose root as API status.
@app.get("/")
def read_root():
    index_file = os.path.join(frontend_path, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {
        "status": "online",
        "message": "SGX Derivatives Dashboard API is running. Build frontend inside dashboard/frontend/ to see visual charts."
    }

# Mount static files (JS, CSS, images) if directory is not empty
if os.path.exists(frontend_path):
    app.mount("/static", StaticFiles(directory=frontend_path), name="static")

if __name__ == "__main__":
    import uvicorn
    # Start uvicorn server on port 8000
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
