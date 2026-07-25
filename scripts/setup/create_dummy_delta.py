import sys
import logging
# pyrefly: ignore [missing-import]
import trino

logging.basicConfig(level=logging.INFO, format="%(message)s")

TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_USER = "admin"
TRINO_CATALOG = "minio"

DDL = [
    "CREATE TABLE IF NOT EXISTS minio.silver.users (id BIGINT, name VARCHAR, email VARCHAR, phone VARCHAR, address VARCHAR, created_at TIMESTAMP(6), updated_at TIMESTAMP(6), is_active BOOLEAN)",
    "CREATE TABLE IF NOT EXISTS minio.silver.products (id BIGINT, name VARCHAR, category VARCHAR, base_price DOUBLE, created_at TIMESTAMP(6), updated_at TIMESTAMP(6), is_active BOOLEAN)",
    "CREATE TABLE IF NOT EXISTS minio.silver.orders (id BIGINT, user_id BIGINT, product_id BIGINT, quantity INT, total_amount DOUBLE, payment_method VARCHAR, currency VARCHAR, status VARCHAR, created_at TIMESTAMP(6), updated_at TIMESTAMP(6))"
]

conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG)
cursor = conn.cursor()

for stmt in DDL:
    print(f"Executing: {stmt}")
    cursor.execute(stmt)
    print("DONE")
