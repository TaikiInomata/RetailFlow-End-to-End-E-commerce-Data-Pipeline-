"""
gold_dbt_pipeline.py
────────────────────
DAG orchestrating all dbt Gold Layer models via Trino.

Pipeline triggers:
  - Sales + Customer snapshot: sau khi CDC pipeline hoàn tất
  - Funnel: sau khi Clickstream batch hoàn tất
  - dbt test: sau tất cả mart chạy xong

Task Graph:
  [sensor_cdc_done]
       │
       ▼
  [dbt_staging]  ←── Staging views (stg_orders, stg_products, stg_users, stg_exchange_rates, stg_clickstream)
       │
       ├──► [dbt_sales]          ──► [dbt_test_sales]
       │      (daily_sales_summary,                │
       │       product_performance)                │
       │                                          ▼
       ├──► [dbt_customer]  ──────────────────► [dbt_test_all]
       │      (customer_snapshot)                  ▲
       │      (chỉ chạy 1 lần/ngày lúc 2AM)        │
       │                                          │
  [sensor_clickstream_done]                       │
       │                                          │
       ▼                                          │
  [dbt_funnel]  ────────────────────────────────► ┘
       (daily_funnel, product_engagement)
"""

from datetime import datetime, timedelta
# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators. bash import BashOperator
# pyrefly: ignore [missing-import]
from airflow.sensors.external_task import ExternalTaskSensor
# pyrefly: ignore [missing-import]
from airflow.utils.task_group import TaskGroup
# pyrefly: ignore [missing-import]
from airflow.datasets import Dataset

# ── Constants ─────────────────────────────────────────────
DBT_DIR = "/opt/airflow/dbt"
DBT_GLOBAL = "--no-use-colors"
DBT_FLAGS = f"--profiles-dir {DBT_DIR} --project-dir {DBT_DIR}"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dataset_cdc = Dataset("s3://silver-zone/cdc")
dataset_clickstream = Dataset("s3://silver-zone/clickstream")

with DAG(
    dag_id="gold_dbt_pipeline",
    description="Gold Layer — dbt models via Trino (Sales + Customer + Funnel)",
    default_args=default_args,
    # Chạy tự động ngay khi CẢ HAI luồng CDC và Clickstream hoàn thành cập nhật Silver
    schedule=[dataset_cdc, dataset_clickstream],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["gold", "dbt", "trino"],
    max_active_runs=1,  # Tránh concurrent runs xung đột File-based metastore
) as dag:

    # ── dbt: Staging Layer ─────────────────────────────────
    # Staging là views → chạy nhanh, phải chạy trước tất cả mart
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"dbt {DBT_GLOBAL} run {DBT_FLAGS} --select staging",
    )

    # ── dbt: Mart 1 — Sales Analytics ─────────────────────
    with TaskGroup("sales_analytics") as tg_sales:
        dbt_sales = BashOperator(
            task_id="dbt_run_sales",
            bash_command=f"dbt {DBT_GLOBAL} run {DBT_FLAGS} --select models/marts/sales_analytics",
        )
        dbt_test_sales = BashOperator(
            task_id="dbt_test_sales",
            bash_command=f"dbt {DBT_GLOBAL} test {DBT_FLAGS} --select models/marts/sales_analytics",
        )
        dbt_sales >> dbt_test_sales

    # ── dbt: Mart 2 — Customer 360 ─────────────────────────
    # Customer snapshot chỉ cần chạy 1 lần/ngày (2AM)
    # ShortCircuitOperator skip nếu không phải giờ 2AM
    with TaskGroup("customer_360") as tg_customer:
        # pyrefly: ignore [missing-import]
        from airflow.operators.python import ShortCircuitOperator

        def _is_daily_run(**context):
            """Chỉ chạy customer snapshot khi DAG được trigger lúc 2AM."""
            execution_hour = context["logical_date"].hour
            return execution_hour == 2

        check_daily_run = ShortCircuitOperator(
            task_id="check_is_daily_run",
            python_callable=_is_daily_run,
            ignore_downstream_trigger_rules=False,
        )
        dbt_customer = BashOperator(
            task_id="dbt_run_customer",
            bash_command=f"dbt {DBT_GLOBAL} run {DBT_FLAGS} --select models/marts/customer_360",
        )
        dbt_test_customer = BashOperator(
            task_id="dbt_test_customer",
            bash_command=f"dbt {DBT_GLOBAL} test {DBT_FLAGS} --select models/marts/customer_360",
        )
        check_daily_run >> dbt_customer >> dbt_test_customer

    # ── dbt: Mart 3 — Funnel Analytics ────────────────────
    with TaskGroup("funnel_analytics") as tg_funnel:
        dbt_funnel = BashOperator(
            task_id="dbt_run_funnel",
            bash_command=f"dbt {DBT_GLOBAL} run {DBT_FLAGS} --select models/marts/funnel_analytics",
        )
        dbt_test_funnel = BashOperator(
            task_id="dbt_test_funnel",
            bash_command=f"dbt {DBT_GLOBAL} test {DBT_FLAGS} --select models/marts/funnel_analytics",
        )
        dbt_funnel >> dbt_test_funnel

    # ── dbt: Source Freshness Check ────────────────────────
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"dbt {DBT_GLOBAL} source freshness {DBT_FLAGS}",
        # Không fail DAG nếu freshness warning (chỉ log)
        trigger_rule="all_done",
    )

    # ── Task Dependencies ──────────────────────────────────
    # Trigger by Datasets -> run staging directly -> split to marts
    dbt_staging >> [tg_sales, tg_customer, tg_funnel]

    # Source freshness chạy sau tất cả mart
    [tg_sales, tg_customer, tg_funnel] >> dbt_source_freshness
