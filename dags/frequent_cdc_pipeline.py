"""
frequent_cdc_pipeline.py

DAG điều phối luồng CDC (Change Data Capture) từ PostgreSQL:
  Bronze (Debezium JSON) → Silver (Delta Table, MERGE/Upsert)

Xử lý 3 bảng: orders, products, users — chạy SONG SONG để tiết kiệm thời gian.

Schedule     : */15 * * * *  (Mỗi 15 phút)
Catch-up     : False
Max Runs     : 1 — CRITICAL: Ngăn 2 batch MERGE vào cùng Delta Table đồng thời
Pool         : spark_pool (3 slots) — Ngăn khởi động > 3 Spark Sessions cùng lúc

QUAN TRỌNG: Phải tạo pool trước khi bật DAG:
    docker exec retailflow_airflow_scheduler \\
        airflow pools set spark_pool 3 "Spark resource pool"
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
from airflow.datasets import Dataset

from utils.callbacks import on_failure_callback
from utils.spark_config import spark_cmd, SPARK_POOL

DOC_MD = """\
## ♻️ Frequent CDC Pipeline (Micro-batch)

**Luồng:** Debezium S3 JSONs (Bronze) → MERGE INTO Delta Tables (Silver)

**Lịch chạy:** `*/15 * * * *` — Mỗi 15 phút

**Bảng xử lý:** `orders`, `products`, `users` — chạy song song (Fan-out)

| Task | Pool | Mô tả |
|---|---|---|
| `silver_cdc_orders` | spark_pool | Upsert bảng orders (Soft Delete) |
| `silver_cdc_products` | spark_pool | Upsert bảng products (Soft Delete) |
| `silver_cdc_users` | spark_pool | Upsert bảng users (Soft Delete) |
| `wait_for_all_silver` | — | Fan-in: Chờ cả 3 task CDC hoàn thành |
| `gold_*__pending` | — | Placeholder cho Gold layer |

**⚠️ max_active_runs=1:** Bắt buộc. Nếu 2 batch chạy cùng lúc → MERGE conflict → Data corruption.
"""

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

# Khai báo cấu hình 3 bảng CDC: (tên_bảng, khóa_chính)
CDC_TABLES = [
    ("orders",   "id"),
    ("products", "product_id"),
    ("users",    "id"),
]

dataset_cdc = Dataset("s3://silver-zone/cdc")

with DAG(
    dag_id="frequent_cdc_pipeline",
    description="Bronze (Debezium) → Silver (Delta Upsert): orders, products, users — mỗi 15 phút",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 7, 20, tz="UTC"),
    catchup=False,
    max_active_runs=1,   # CRITICAL: Ngăn MERGE conflict giữa 2 batch
    default_args=default_args,
    tags=["silver", "cdc", "frequent"],
    doc_md=DOC_MD,
) as dag:

    # TASK GROUP: 3 tasks CDC chạy SONG SONG (Fan-out từ DAG start)
    cdc_tasks = []
    for table_name, primary_key in CDC_TABLES:
        task = BashOperator(
            task_id=f"silver_cdc_{table_name}",
            bash_command=spark_cmd(
                "silver/silver_cdc_processor.py",
                "--table_name", table_name,
                "--primary_key", primary_key,
            ),
            pool=SPARK_POOL,
        )
        cdc_tasks.append(task)

    # Fan-in: Chờ TẤT CẢ 3 tasks CDC thành công mới chạy Gold
    wait_all_silver = EmptyOperator(
        task_id="wait_for_all_silver",
        trigger_rule="all_success",  # Chỉ thành công nếu CẢ 3 task trên đều success
        outlets=[dataset_cdc]
    )

    # Dependency: [orders, products, users] → wait_all
    cdc_tasks >> wait_all_silver
