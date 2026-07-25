"""
init_trino_schemas.py
─────────────────────
Chạy 1 lần sau khi Trino container start để tạo các schema trong catalog 'minio'.
Schemas này map trực tiếp đến bucket/prefix trên MinIO.

Usage (từ project root):
    python scripts/setup/init_trino_schemas.py

Hoặc chạy trong Airflow container:
    docker exec retailflow_airflow_webserver python /opt/airflow/scripts/setup/init_trino_schemas.py
"""

import sys
import time
import logging
# pyrefly: ignore [missing-import]
import trino

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Kết nối Trino ─────────────────────────────────────────
TRINO_HOST = "trino"          # Docker service name
TRINO_PORT = 8080
TRINO_USER = "admin"
TRINO_CATALOG = "minio"


def get_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
    )


def wait_for_trino(max_retries: int = 30, delay: int = 10):
    """Đợi Trino sẵn sàng trước khi chạy DDL."""
    log.info(f"Đang đợi Trino sẵn sàng tại {TRINO_HOST}:{TRINO_PORT} ...")
    for attempt in range(1, max_retries + 1):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            conn.close()
            log.info("✅ Trino đã sẵn sàng.")
            return True
        except Exception as e:
            log.warning(f"Lần thử {attempt}/{max_retries}: Trino chưa ready — {e}")
            time.sleep(delay)
    log.error("❌ Trino không phản hồi sau nhiều lần thử. Kiểm tra container.")
    return False


# ── Schema definitions ────────────────────────────────────
# location: đường dẫn S3 trên MinIO
# Lưu ý: bucket names phải đã tồn tại trên MinIO (tạo bởi setup-init container)
SCHEMAS = [
    {
        "name": "silver",
        "location": "s3://silver-zone/",
        "description": "Silver Layer — Cleaned Delta Lake tables (CDC + Exchange Rates + Clickstream)",
    },
    {
        "name": "gold",
        "location": "s3://gold-zone/",
        "description": "Gold Layer — Business-ready Data Marts (dbt models)",
    },
]


def create_schemas():
    """Tạo schema trong Trino catalog 'minio' nếu chưa tồn tại."""
    conn = get_connection()
    cursor = conn.cursor()

    for schema in SCHEMAS:
        ddl = (
            f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{schema['name']} "
            f"WITH (location = '{schema['location']}')"
        )
        try:
            log.info(f"Tạo schema: {TRINO_CATALOG}.{schema['name']} → {schema['location']}")
            cursor.execute(ddl)
            log.info(f"  ✅ {TRINO_CATALOG}.{schema['name']} — OK")
        except Exception as e:
            log.error(f"  ❌ Lỗi tạo schema {schema['name']}: {e}")
            raise

    conn.close()


def verify_schemas():
    """Kiểm tra các schema đã được tạo đúng."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SHOW SCHEMAS FROM {TRINO_CATALOG}")
    existing = {row[0] for row in cursor.fetchall()}
    conn.close()

    log.info(f"Schemas hiện có trong '{TRINO_CATALOG}': {sorted(existing)}")

    missing = []
    for schema in SCHEMAS:
        if schema["name"] in existing:
            log.info(f"  ✅ {schema['name']}")
        else:
            log.warning(f"  ❌ THIẾU: {schema['name']}")
            missing.append(schema["name"])

    return missing


def main():
    log.info("=" * 60)
    log.info("  RetailFlow — Trino Schema Initialization")
    log.info("=" * 60)

    # 1. Đợi Trino sẵn sàng
    if not wait_for_trino():
        sys.exit(1)

    # 2. Tạo schemas
    create_schemas()

    # 3. Verify
    missing = verify_schemas()
    if missing:
        log.error(f"Một số schema chưa được tạo: {missing}")
        sys.exit(1)

    log.info("=" * 60)
    log.info("  ✅ Khởi tạo hoàn tất! Tiếp theo: chạy 'dbt debug'")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
