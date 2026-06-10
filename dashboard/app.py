import os
import io
import json
import logging
from typing import List, Dict, Any, Optional
import boto3
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
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
    """Lists available dates by scanning partition prefixes in s3://bucket/processed/ticks/"""
    dates = set()
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        # List subfolders under processed/ticks/
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix="processed/ticks/", Delimiter="/")
        for page in pages:
            if "CommonPrefixes" in page:
                for prefix in page["CommonPrefixes"]:
                    # prefix is like "processed/ticks/TradeDateParsed=YYYY-MM-DD/"
                    folder_name = prefix["Prefix"].strip("/").split("/")[-1]
                    if "TradeDateParsed=" in folder_name:
                        date_str = folder_name.split("=")[-1]
                        dates.add(date_str)
        return sorted(list(dates), reverse=True)
    except Exception as e:
        logger.error(f"Error listing dates from S3 bucket {BUCKET_NAME}: {e}")
        return []

def get_merged_s3_data(date_str: str) -> Optional[pd.DataFrame]:
    """Downloads all CSV part files for a given date partition and merges them into a Pandas DataFrame"""
    prefix = f"processed/ticks/TradeDateParsed={date_str}/"
    logger.info(f"Listing S3 files in prefix: {prefix}")
    
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if "Contents" not in response:
            logger.warning(f"No objects found for prefix: {prefix}")
            return None
            
        csv_files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".csv")]
        if not csv_files:
            logger.warning(f"No CSV files found for date {date_str}")
            return None
            
        dataframes = []
        for s3_key in csv_files:
            logger.info(f"Downloading file from S3: {s3_key}")
            csv_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            df_part = pd.read_csv(io.BytesIO(csv_obj["Body"].read()))
            if not df_part.empty:
                dataframes.append(df_part)
                
        if not dataframes:
            return None
            
        return pd.concat(dataframes, ignore_index=True)
    except Exception as e:
        logger.error(f"Error fetching data from S3 for date {date_str}: {e}")
        return None

def compute_daily_aggregates(df: pd.DataFrame) -> Dict[str, Any]:
    """Computes necessary metrics and time series aggregations for ECharts visualization"""
    # Standardize TradeVolume and TradePrice types
    df["TradeVolume"] = pd.to_numeric(df["TradeVolume"], errors="coerce").fillna(0).astype(int)
    df["TradePrice"] = pd.to_numeric(df["TradePrice"], errors="coerce").fillna(0.0)
    
    # 1. Overall Summary Metrics
    total_volume = int(df["TradeVolume"].sum())
    total_trades = int(df.shape[0])
    avg_price = float((df["TradePrice"] * df["TradeVolume"]).sum() / total_volume) if total_volume > 0 else float(df["TradePrice"].mean())
    
    # 2. Product Volume Distribution (Bar/Pie Chart data)
    prod_volume = df.groupby("Symbol")["TradeVolume"].sum().reset_index()
    prod_trades = df.groupby("Symbol").size().reset_index(name="TradeCount")
    prod_merged = pd.merge(prod_volume, prod_trades, on="Symbol")
    prod_dist = prod_merged.sort_values(by="TradeVolume", ascending=False).to_dict(orient="records")
    
    # 3. Hourly Trend (Line/Bar Chart data)
    # Parse hour from TradeTime (TradeTime is string like '143210' or '14:32:10')
    df["Hour"] = df["TradeTime"].astype(str).str.replace(":", "").str[:2]
    # Ensure hour format is HH (padded with zero)
    df["Hour"] = df["Hour"].str.zfill(2)
    # Exclude invalid hours
    df = df[df["Hour"].str.isdigit() & (df["Hour"].astype(int) < 24)]
    
    hourly_grp = df.groupby("Hour").agg(
        HourlyVolume=("TradeVolume", "sum"),
        HourlyTradeCount=("TradeVolume", "size"),
        AvgPrice=("TradePrice", "mean")
    ).reset_index()
    
    # Fill in missing hours to ensure complete 24h timeline
    all_hours = pd.DataFrame({"Hour": [str(h).zfill(2) for h in range(24)]})
    hourly_merged = pd.merge(all_hours, hourly_grp, on="Hour", how="left").fillna(0)
    
    hourly_trend = hourly_merged.to_dict(orient="records")
    
    # 4. Message Type Distribution
    msg_dist = df.groupby("MessageCode").size().reset_index(name="Count").to_dict(orient="records")
    
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
            
    # Cache miss - fetch from S3 and process
    logger.info(f"Cache miss for date {date}. Fetching CSV files from S3...")
    df = get_merged_s3_data(date)
    
    if df is None or df.empty:
        raise HTTPException(
            status_code=404, 
            detail=f"No data found for date {date} in S3 bucket {BUCKET_NAME}."
        )
        
    logger.info(f"Successfully loaded {df.shape[0]} rows for {date}. Generating aggregates...")
    aggregates = compute_daily_aggregates(df)
    
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
