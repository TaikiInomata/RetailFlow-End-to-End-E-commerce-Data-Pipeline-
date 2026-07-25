"""
register_delta_tables.py
────────────────────────
Đăng ký các Delta Tables đã tạo từ Spark vào Trino.
"""

import os
import sys
import logging
# pyrefly: ignore [missing-import]
import trino

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TRINO_HOST = "trino"
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = "minio"

TABLES = [
    {"schema": "silver", "name": "users", "location": "s3://silver-zone/cdc_data/users"},
    {"schema": "silver", "name": "products", "location": "s3://silver-zone/cdc_data/products"},
    {"schema": "silver", "name": "orders", "location": "s3://silver-zone/cdc_data/orders"},
    {"schema": "silver", "name": "exchange_rates", "location": "s3://silver-zone/exchange_rates"},
    {"schema": "silver", "name": "clickstream", "location": "s3://silver-zone/clickstream"},
]

def main():
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG)
    cursor = conn.cursor()

    for table in TABLES:
        # Kiểm tra bảng đã đăng ký chưa
        cursor.execute(f"SHOW TABLES IN {table['schema']} LIKE '{table['name']}'")
        if not cursor.fetchone():
            log.info(f"Registering Delta table: {table['schema']}.{table['name']} at {table['location']}")
            try:
                cursor.execute(f"CALL system.register_table('{table['schema']}', '{table['name']}', '{table['location']}')")
                log.info(f"  ✅ Registered")
            except Exception as e:
                log.error(f"  ❌ Failed: {e}")
        else:
            log.info(f"✅ Table {table['schema']}.{table['name']} is already registered.")

    conn.close()

if __name__ == "__main__":
    main()
