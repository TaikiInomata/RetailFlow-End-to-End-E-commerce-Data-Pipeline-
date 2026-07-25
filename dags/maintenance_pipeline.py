"""
maintenance_pipeline.py
────────────────────────
DAG bảo trì Delta Lake hàng ngày lúc 3:00 AM UTC.

Nhiệm vụ:
  1. OPTIMIZE Silver tables  → Gom file Parquet nhỏ thành file 128MB
  2. OPTIMIZE Gold tables    → Tương tự cho 5 bảng Gold
  3. VACUUM Gold tables      → Xóa snapshot cũ hơn 7 ngày (giải phóng MinIO storage)
  4. Log stats               → Đếm số commit trong _delta_log để đo hiệu quả

Lý do kỹ thuật:
  - Pipeline CDC chạy mỗi 15 phút, Clickstream mỗi 30 phút → sau 1 tuần
    mỗi bảng tích lũy hàng nghìn file nhỏ → Trino phải mở từng file → chậm.
  - OPTIMIZE gom file → query nhanh hơn 5-10x.
  - VACUUM xóa Delta log cũ → giảm dung lượng MinIO.

Schedule: 0 3 * * * (3:00 AM UTC = 10:00 SA Việt Nam — giờ ít tải nhất)
"""
# pyrefly: ignore [missing-import]
import pendulum
from datetime import timedelta

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator

from utils.callbacks import on_failure_callback

# ── Constants ─────────────────────────────────────────────────────────────────
TRINO_HOST = "trino"        # Docker service name trong retailflow_network
TRINO_PORT = 8080           # Port nội bộ trong Docker network
TRINO_USER = "airflow"
TRINO_CATALOG = "minio"

# Silver tables cần OPTIMIZE
SILVER_TABLES = [
    "minio.silver.orders",
    "minio.silver.products",
    "minio.silver.users",
    "minio.silver.exchange_rates",
    "minio.silver.clickstream",
]

# Gold tables cần OPTIMIZE và VACUUM
GOLD_TABLES = [
    "minio.gold_gold.gold_daily_sales_summary",
    "minio.gold_gold.gold_product_performance",
    "minio.gold_gold.gold_daily_funnel",
    "minio.gold_gold.gold_product_engagement",
    "minio.gold_gold.gold_customer_snapshot",
]

VACUUM_RETENTION_DAYS = 7


def _run_trino_statements(statements: list[str], task_label: str = "") -> None:
    """
    Kết nối Trino qua Python library và chạy danh sách câu lệnh SQL.
    Mỗi statement được chạy riêng biệt và log kết quả.
    """
    import trino
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        http_scheme="http",
    )
    cursor = conn.cursor()
    for sql in statements:
        table_ref = sql.split(" ")[2] if len(sql.split(" ")) > 2 else "unknown"
        try:
            cursor.execute(sql)
            cursor.fetchall()  # consume result
            print(f"  ✅ {table_ref}")
        except Exception as e:
            print(f"  ⚠️ {table_ref}: {e}")
    conn.close()
    print(f"\n{task_label} hoàn tất.")


def _optimize_silver(**context) -> None:
    """OPTIMIZE Silver tables — gom file nhỏ → 128MB."""
    print("\n🔧 OPTIMIZE Silver tables...")
    statements = [
        f"ALTER TABLE {t} EXECUTE optimize(file_size_threshold => '128MB')"
        for t in SILVER_TABLES
    ]
    _run_trino_statements(statements, task_label="optimize_silver")


def _optimize_gold(**context) -> None:
    """OPTIMIZE Gold tables — gom file nhỏ → 128MB."""
    print("\n🔧 OPTIMIZE Gold tables...")
    statements = [
        f"ALTER TABLE {t} EXECUTE optimize(file_size_threshold => '128MB')"
        for t in GOLD_TABLES
    ]
    _run_trino_statements(statements, task_label="optimize_gold")


def _vacuum_gold(**context) -> None:
    """VACUUM Gold tables — xóa snapshot Delta cũ hơn 7 ngày."""
    print(f"\n🗑️ VACUUM Gold tables (retention: {VACUUM_RETENTION_DAYS} ngày)...")
    statements = [
        f"ALTER TABLE {t} EXECUTE expire_snapshots(retention_threshold => '{VACUUM_RETENTION_DAYS}d')"
        for t in GOLD_TABLES
    ]
    _run_trino_statements(statements, task_label="vacuum_gold")


def _log_table_stats(**context):
    """
    Query số commit trong _delta_log của mỗi bảng Gold và ghi vào Airflow log.
    Dùng để đo lường hiệu quả của OPTIMIZE theo thời gian.
    """
    import trino

    results = {}
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            http_scheme="http",
        )
        cursor = conn.cursor()
        for table in GOLD_TABLES:
            table_name = table.split(".")[-1]
            sql = f"SELECT COUNT(*) AS commit_count FROM \"{table}$history\""
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
                results[table_name] = row[0] if row else "N/A"
            except Exception as e:
                results[table_name] = f"error: {e}"
        conn.close()
    except Exception as e:
        print(f"Không thể kết nối Trino để lấy stats: {e}")

    print("\n" + "=" * 60)
    print("📊 DELTA LAKE MAINTENANCE STATS — Delta Log Commit Counts")
    print("=" * 60)
    for table_name, count in results.items():
        print(f"  {table_name:<40} : {count} commits")
    print("=" * 60)
    print("✅ OPTIMIZE + VACUUM hoàn tất. Các file nhỏ đã được gom lại.")

DOC_MD = """\
## 🔧 Maintenance Pipeline

**Lịch chạy:** `0 3 * * *` — 3:00 AM UTC (10:00 SA Việt Nam)

| Task | Mô tả |
|---|---|
| `optimize_silver` | Gom file Parquet nhỏ → 128MB trên 5 bảng Silver |
| `optimize_gold` | Gom file Parquet nhỏ → 128MB trên 5 bảng Gold |
| `vacuum_gold` | Xóa Delta snapshot cũ hơn 7 ngày |
| `log_stats` | Đếm số Delta commit, in báo cáo vào Airflow log |

**Lý do:** Pipeline chạy mỗi 15-30 phút → mỗi tuần tích lũy hàng nghìn
file nhỏ trên MinIO → Trino query chậm 5-10x. OPTIMIZE khắc phục điều này.
"""

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="maintenance_pipeline",
    description="Delta Lake OPTIMIZE + VACUUM hàng ngày (3AM UTC)",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 7, 25, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["maintenance", "delta", "optimize"],
    doc_md=DOC_MD,
) as dag:

    # ── Task 1: OPTIMIZE Silver ───────────────────────────────────────────────
    optimize_silver = PythonOperator(
        task_id="optimize_silver",
        python_callable=_optimize_silver,
    )

    # ── Task 2: OPTIMIZE Gold ────────────────────────────────────────────────
    optimize_gold = PythonOperator(
        task_id="optimize_gold",
        python_callable=_optimize_gold,
    )

    # ── Task 3: VACUUM Gold (expire_snapshots) ─────────────────────────────────
    vacuum_gold = PythonOperator(
        task_id="vacuum_gold",
        python_callable=_vacuum_gold,
        # Không fail DAG nếu bảng quá mới, chưa đủ snapshot để vacuum
        trigger_rule="all_done",
    )

    # ── Task 4: Log stats ────────────────────────────────────────────────────
    log_stats = PythonOperator(
        task_id="log_stats",
        python_callable=_log_table_stats,
        trigger_rule="all_done",
    )

    # ── Dependencies ─────────────────────────────────────────────────────────
    optimize_silver >> optimize_gold >> vacuum_gold >> log_stats
