"""
frequent_clickstream_pipeline.py

DAG điều phối luồng Clickstream (Batch-on-Streaming):
  Bronze (Parquet từ Kafka Streaming) → Silver (Delta Table, Append)

Sử dụng trigger(availableNow=True) trong Spark Job: Job tự động tắt sau khi
xử lý hết các file mới nhất — không cần Airflow kill thủ công.

Schedule     : 7-59/30 * * * *  (Mỗi 30 phút, bắt đầu từ phút :07)
               Lệch 7 phút so với frequent_cdc_pipeline (*/15) để tránh tranh spark_pool
Catch-up     : False
Max Runs     : 1 — Spark Streaming Checkpoint tự nhớ offset, không cần chạy bù
Pool         : spark_pool (1 slot)
"""
# pyrefly: ignore [missing-import]
import pendulum

# pyrefly: ignore [missing-import]
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
## 🌊 Frequent Clickstream Pipeline (Batch-on-Streaming)

**Luồng:** Bronze Parquet (Kafka → Spark Streaming) → Silver (Delta Table, Append)

**Lịch chạy:** `7-59/30 * * * *` — Mỗi 30 phút lúc :07 và :37
(Lệch 7 phút so với CDC pipeline để tránh tranh `spark_pool`)

**Cơ chế:** Spark Job dùng `trigger(availableNow=True)`:
- Bật lên → Đọc tất cả Parquet files mới kể từ lần chạy trước (nhờ Checkpoint)
- Ghi xuống Silver Delta Table
- Tự động tắt → Airflow ghi nhận SUCCESS

| Task | Pool | Mô tả |
|---|---|---|
| `silver_clickstream` | spark_pool | Batch-on-Streaming: Đọc Parquet mới → Ghi Delta |
| `gold_traffic__pending` | — | Placeholder cho Gold Traffic Analytics |
"""

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,                          # Clickstream ít retry hơn vì data luôn có sẵn
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

dataset_clickstream = Dataset("s3://silver-zone/clickstream")

with DAG(
    dag_id="frequent_clickstream_pipeline",
    description="Bronze Parquet → Silver Delta: Clickstream — mỗi 30 phút",
    schedule="7-59/30 * * * *",           # :07 và :37 — lệch 7 phút so với CDC
    start_date=pendulum.datetime(2026, 7, 20, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["silver", "clickstream", "streaming", "frequent"],
    doc_md=DOC_MD,
) as dag:

    # TASK 1: Silver Clickstream (Batch-on-Streaming)
    silver_clickstream = BashOperator(
        task_id="silver_clickstream",
        bash_command=spark_cmd("silver/silver_clickstream.py"),
        pool=SPARK_POOL,
        outlets=[dataset_clickstream]
    )
