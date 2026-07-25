"""
daily_exchange_rate_pipeline.py

DAG điều phối luồng dữ liệu Tỷ giá hằng ngày:
  Bronze (API fetch) → Silver (Delta Table)

Schedule : 0 1 * * *  (01:00 UTC = 08:00 Việt Nam)
Catch-up  : False — Tránh gọi API dư thừa và bị rate-limit
Max Runs  : 1 — Không cho phép 2 lần fetch API cùng lúc
"""
# pyrefly: ignore [missing-import]
import pendulum
from datetime import timedelta

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator
# pyrefly: ignore [missing-import]
from airflow.operators.empty import EmptyOperator

from utils.callbacks import on_failure_callback
from utils.spark_config import spark_cmd, SPARK_POOL

DOC_MD = """\
## 📅 Daily Exchange Rate Pipeline

**Luồng:** API (ExchangeRate-API) → Bronze (JSON) → Silver (Delta Table)

**Lịch chạy:** `0 1 * * *` — 01:00 UTC (08:00 VN) mỗi ngày

| Task | Operator | Mô tả |
|---|---|---|
| `fetch_exchange_rate_to_bronze` | BashOperator | Gọi API, lưu JSON xuống MinIO Bronze Zone |
| `silver_exchange_rate` | BashOperator | Đọc JSON Bronze → Transform → Ghi Delta Silver |
| `gold_placeholder` | EmptyOperator | Chỗ trống cho Gold task (sẽ bổ sung sau) |

**Lưu ý:** `max_active_runs=1` tránh race condition với API.
"""

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="daily_exchange_rate_pipeline",
    description="Bronze (API) → Silver (Delta): Tỷ giá ngoại tệ hằng ngày",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 7, 20, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["silver", "exchange_rate", "daily"],
    doc_md=DOC_MD,
) as dag:

    # TASK 1: Fetch từ API → Bronze Zone
    fetch_to_bronze = BashOperator(
        task_id="fetch_exchange_rate_to_bronze",
        bash_command="python /opt/airflow/scripts/ingestion/fetch/fetch_exchange_rates.py",
    )

    # TASK 2: Bronze → Silver (Delta Table)
    silver_exchange_rate = BashOperator(
        task_id="silver_exchange_rate",
        bash_command=spark_cmd("silver/silver_exchange_rate.py"),
        pool=SPARK_POOL,
    )

    # TASK 3: Placeholder — Gold layer (sẽ thay bằng SparkJob sau)
    gold_placeholder = EmptyOperator(
        task_id="gold_sales_dashboard__pending",
    )

    # Dependency chain
    fetch_to_bronze >> silver_exchange_rate >> gold_placeholder
