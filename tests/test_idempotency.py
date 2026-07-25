"""
test_idempotency.py
────────────────────
Script kiểm thử tính Lũy đẳng (Idempotency) của Gold Pipeline.

Nguyên lý: Chạy DAG Gold nhiều lần → Số dòng trong bảng Gold PHẢI KHÔNG THAY ĐỔI.

Cách chạy (từ máy host):
    python tests/test_idempotency.py

Yêu cầu:
    - Docker containers đang chạy (Airflow, Trino)
    - pip install trino requests tabulate
"""
import subprocess
import time
import sys
from datetime import datetime

try:
    import trino
    from tabulate import tabulate
    import requests
except ImportError:
    print("❌ Thiếu thư viện. Chạy: pip install trino requests tabulate")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────────────────
TRINO_HOST = "localhost"
TRINO_PORT = 8082          # Port expose ra máy host
AIRFLOW_BASE_URL = "http://localhost:8081"  # Port Airflow webserver trên máy host
AIRFLOW_USER = "admin"
AIRFLOW_PASSWORD = "admin"

GOLD_TABLES = [
    ("minio", "gold_gold", "gold_daily_sales_summary"),
    ("minio", "gold_gold", "gold_product_performance"),
    ("minio", "gold_gold", "gold_daily_funnel"),
    ("minio", "gold_gold", "gold_product_engagement"),
    ("minio", "gold_gold", "gold_customer_snapshot"),
]

DAG_ID = "gold_dbt_pipeline"
WAIT_SECONDS = 90          # Thời gian đợi mỗi lần trigger DAG chạy xong


def get_table_row_counts() -> dict:
    """Query số dòng hiện tại trong từng bảng Gold qua Trino."""
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="test_script",
        http_scheme="http",
    )
    cursor = conn.cursor()
    counts = {}
    for catalog, schema, table in GOLD_TABLES:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {catalog}.{schema}.{table}")
            row = cursor.fetchone()
            counts[f"{schema}.{table}"] = row[0] if row else 0
        except Exception as e:
            counts[f"{schema}.{table}"] = f"ERROR: {e}"
    conn.close()
    return counts


def trigger_dag() -> str:
    """Trigger DAG gold_dbt_pipeline qua Airflow CLI."""
    import subprocess
    run_id = f"test_idempotency_{int(time.time())}"
    cmd = [
        "docker", "exec", "retailflow_airflow_webserver",
        "airflow", "dags", "trigger", DAG_ID, "--run-id", run_id
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode == 0:
        return run_id
    raise RuntimeError(f"Trigger thất bại: {proc.stderr}")


def wait_for_dag(run_id: str, timeout: int = 300) -> str:
    """Polling trạng thái DAG Run qua Airflow CLI."""
    import subprocess
    start = time.time()
    while time.time() - start < timeout:
        cmd = [
            "docker", "exec", "retailflow_airflow_webserver",
            "airflow", "dags", "state", DAG_ID, run_id
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            state = proc.stdout.strip().split("\n")[-1]  # Get last line
            if state in ("success", "failed"):
                return state
        print(f"    ⏳ Đang chờ DAG hoàn tất... ")
        time.sleep(10)
    return "timeout"


def run_idempotency_test():
    print("\n" + "═" * 65)
    print("🧪 IDEMPOTENCY TEST — Gold Pipeline")
    print(f"   Thời gian bắt đầu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 65)

    # ── Bước 1: Lấy số dòng ban đầu ──────────────────────────────────────────
    print("\n📊 Bước 1: Đếm số dòng ban đầu...")
    baseline = get_table_row_counts()
    print(tabulate(
        [[t, c] for t, c in baseline.items()],
        headers=["Bảng Gold", "Số dòng (baseline)"],
        tablefmt="rounded_outline"
    ))

    results = {
        "baseline": baseline,
        "runs": [],
        "pass": True,
    }

    # ── Bước 2: Trigger DAG 2 lần liên tiếp ──────────────────────────────────
    for i in range(1, 3):
        print(f"\n🚀 Bước {i + 1}: Trigger lần {i}...")
        try:
            run_id = trigger_dag()
            print(f"   Run ID: {run_id}")
            state = wait_for_dag(run_id)
            print(f"   Kết quả: {state.upper()}")

            print(f"\n📊 Đếm số dòng sau lần trigger {i}...")
            after_counts = get_table_row_counts()
            print(tabulate(
                [[t, baseline[t], after_counts[t], "✅ PASS" if after_counts[t] == baseline[t] else "❌ FAIL"]
                 for t in baseline],
                headers=["Bảng Gold", "Baseline", "Sau trigger", "Kết quả"],
                tablefmt="rounded_outline"
            ))

            run_pass = all(after_counts[t] == baseline[t] for t in baseline)
            results["runs"].append({
                "trigger": i,
                "state": state,
                "counts": after_counts,
                "pass": run_pass,
            })
            if not run_pass:
                results["pass"] = False

        except Exception as e:
            print(f"   ⚠️ Lỗi khi trigger: {e}")
            print("   Bỏ qua bước này và tiếp tục...")
            results["runs"].append({"trigger": i, "error": str(e), "pass": False})
            results["pass"] = False

    # ── Báo cáo tổng kết ─────────────────────────────────────────────────────
    print("\n" + "═" * 65)
    if results["pass"]:
        print("✅ IDEMPOTENCY TEST: PASSED")
        print("   Tất cả bảng Gold không bị duplicate sau khi trigger lại nhiều lần.")
    else:
        print("❌ IDEMPOTENCY TEST: FAILED")
        print("   Phát hiện dữ liệu bị nhân đôi! Cần kiểm tra lại pipeline.")
    print("═" * 65)
    print(f"   Thời gian kết thúc: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return results["pass"]


if __name__ == "__main__":
    success = run_idempotency_test()
    sys.exit(0 if success else 1)
