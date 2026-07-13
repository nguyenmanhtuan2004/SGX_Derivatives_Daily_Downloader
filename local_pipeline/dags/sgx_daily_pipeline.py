from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình các tham số mặc định của DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa DAG chạy từ thứ 2 đến thứ 6 hàng tuần lúc 22:00
with DAG(
    'sgx_derivatives_daily_pipeline',
    default_args=default_args,
    description='Automated local pipeline for SGX Derivatives Market data',
    schedule_interval='0 22 * * 1-5',  # 22:00 SGT (Thứ 2 - Thứ 6)
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['sgx', 'pyspark', 'delta'],
) as dag:

    # Task 1: Nạp dữ liệu thô (Ingestion)
    ingestion_task = BashOperator(
        task_id='01_Ingestion',
        bash_command='python /opt/airflow/workspace/01_Ingestion.py --date {{ ds }} --config /opt/airflow/workspace/config/config.ini',
    )

    # Task 2: Xử lý ETL dữ liệu Ticks (Chạy song song)
    etl_tick_task = BashOperator(
        task_id='02_ETL_Tick_Data',
        bash_command='python /opt/airflow/workspace/02_ETL_Tick_Data.py --date {{ ds }} --config /opt/airflow/workspace/config/config.ini',
    )

    # Task 3: Xử lý ETL dữ liệu Hủy lệnh (Chạy song song)
    etl_trade_cancel_task = BashOperator(
        task_id='02_ETL_Trade_Cancel',
        bash_command='python /opt/airflow/workspace/02_ETL_Trade_Cancel.py --date {{ ds }} --config /opt/airflow/workspace/config/config.ini',
    )

    # Task 4: Bảo trì, tối ưu hóa các Delta tables
    maintenance_task = BashOperator(
        task_id='04_Maintenance',
        bash_command='python /opt/airflow/workspace/04_Maintenance.py',
    )

    # Biểu diễn Sơ đồ phụ thuộc (Workflow DAG)
    ingestion_task >> [etl_tick_task, etl_trade_cancel_task] >> maintenance_task
