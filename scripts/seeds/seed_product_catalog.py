"""
seed_product_catalog.py
=======================
Bước khởi tạo nền tảng (Track 1): Parse CSV Kaggle → PostgreSQL products table.

Chức năng:
  - Stream đọc CSV theo chunk (an toàn với file 13GB+ nhờ không load vào RAM)
  - Trích xuất unique products: product_id, category_id, category_code, brand, avg_price
  - Fill NULL category_code từ category_id lookup table (built on-the-fly)
  - Bulk insert vào PostgreSQL với ON CONFLICT DO NOTHING → idempotent (chạy lại an toàn)
  - Thiết lập REPLICA IDENTITY FULL để Debezium theo dõi thay đổi

Chạy lệnh:
  python scripts/seeds/seed_product_catalog.py

Biến môi trường (từ .env):
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
  SEED_PRODUCT_LIMIT   (optional) — giới hạn unique products, dùng khi dev/test. 0 = không giới hạn
"""
import os
import sys
import time
import logging
from pathlib import Path

import psycopg2
import psycopg2.extras
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / '.env')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent.parent.parent
CSV_DIR      = BASE_DIR / "scripts" / "ingestion" / "batch" / "datasets"
CSV_FILES    = sorted(CSV_DIR.glob("*.csv"))

CHUNK_SIZE   = 100_000   # Số rows đọc mỗi lần (cân bằng giữa RAM và tốc độ)
BATCH_SIZE   = 5_000     # Số rows mỗi lần INSERT vào PostgreSQL
SEED_LIMIT   = int(os.getenv("SEED_PRODUCT_LIMIT", "0"))   # 0 = không giới hạn
LOG_INTERVAL = 10        # Log progress mỗi N chunks


# ── SQL ────────────────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS products (
        product_id    VARCHAR(20)     PRIMARY KEY,
        category_id   VARCHAR(30)     NOT NULL,
        category_code VARCHAR(100),                -- Raw từ CSV (có thể NULL)
        category_fill VARCHAR(100)    NOT NULL,     -- Đã fill NULL — KHÔNG BAO GIỜ NULL
        brand         VARCHAR(100)    NOT NULL,
        avg_price     DECIMAL(10, 2)  NOT NULL,
        created_at    TIMESTAMPTZ     DEFAULT CURRENT_TIMESTAMP
    );
"""

# Bật REPLICA IDENTITY FULL: Debezium nhận toàn bộ row khi UPDATE/DELETE products
ALTER_REPLICA_SQL = "ALTER TABLE products REPLICA IDENTITY FULL;"

# ON CONFLICT DO NOTHING: chạy lại nhiều lần vẫn không tạo duplicate
INSERT_SQL = """
    INSERT INTO products
        (product_id, category_id, category_code, category_fill, brand, avg_price)
    VALUES %s
    ON CONFLICT (product_id) DO NOTHING;
"""


# ── Database ────────────────────────────────────────────────────────────────────
def connect_db() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5433"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    conn.autocommit = True
    return conn


def setup_products_table(cursor) -> None:
    logger.info("[Seed] Đang tạo bảng 'products'...")
    cursor.execute(CREATE_TABLE_SQL)
    cursor.execute(ALTER_REPLICA_SQL)
    logger.info("[Seed] Bảng 'products' đã sẵn sàng (REPLICA IDENTITY FULL).")


# ── Parsing ─────────────────────────────────────────────────────────────────────
def parse_csv_files() -> tuple[dict, dict]:
    """
    Stream đọc tất cả CSV file theo CHUNK_SIZE rows/lần.
    Trả về:
      products   : dict[product_id → {category_id, category_code, brand, prices[]}]
      cat_to_code: dict[category_id → best known category_code]  ← dùng fill NULL
    """
    products: dict     = {}
    cat_to_code: dict  = {}   # Lookup: category_id → category_code tốt nhất đã gặp
    total_rows         = 0
    total_chunks       = 0
    stop_flag          = False

    for csv_path in CSV_FILES:
        if stop_flag:
            break
        logger.info("[Seed] ── Đang đọc: %s (%.2f GB)",
                    csv_path.name, csv_path.stat().st_size / 1e9)

        reader = pd.read_csv(
            csv_path,
            chunksize   = CHUNK_SIZE,
            usecols     = ["product_id", "category_id", "category_code", "brand", "price"],
            dtype       = {"product_id": str, "category_id": str},
            on_bad_lines = "skip",
        )

        for chunk in reader:
            total_rows   += len(chunk)
            total_chunks += 1

            for row in chunk.itertuples(index=False):
                pid   = row.product_id   if not pd.isna(row.product_id)   else None
                cid   = row.category_id  if not pd.isna(row.category_id)  else None
                code  = row.category_code if not pd.isna(row.category_code) else None
                brand = row.brand        if not pd.isna(row.brand)        else None
                price = float(row.price) if not pd.isna(row.price) and row.price > 0 else None

                if not pid or not cid:
                    continue

                # Cập nhật lookup: giữ category_code đầy đủ nhất đã gặp
                if code:
                    cat_to_code[cid] = code

                if pid not in products:
                    products[pid] = {
                        "category_id"   : cid,
                        "category_code" : code,
                        "brand"         : brand,
                        "prices"        : [price] if price else [],
                    }
                else:
                    entry = products[pid]
                    # Cập nhật category_code nếu tìm được giá trị (thay NULL)
                    if code and not entry["category_code"]:
                        entry["category_code"] = code
                    if brand and not entry["brand"]:
                        entry["brand"] = brand
                    if price:
                        entry["prices"].append(price)

            if total_chunks % LOG_INTERVAL == 0:
                logger.info("[Seed] Chunks: %d | Rows: %s | Unique products: %d",
                            total_chunks, f"{total_rows:,}", len(products))

            # Dừng sớm nếu đạt giới hạn dev/test
            if SEED_LIMIT > 0 and len(products) >= SEED_LIMIT:
                logger.info("[Seed] SEED_PRODUCT_LIMIT=%d đạt. Dừng đọc CSV.", SEED_LIMIT)
                stop_flag = True
                break

    logger.info("[Seed] Hoàn tất đọc: %s rows | %d unique products | cat_to_code: %d entries",
                f"{total_rows:,}", len(products), len(cat_to_code))
    return products, cat_to_code


def build_insert_rows(products: dict, cat_to_code: dict) -> list[tuple]:
    """
    Chuyển products dict sang list tuples chuẩn bị INSERT.
    Fill NULL category_code từ cat_to_code lookup (2-level fallback).
    """
    rows = []
    null_count = 0

    for pid, data in products.items():
        cid    = data["category_id"]
        code   = data["category_code"]
        brand  = data["brand"] or "unknown"
        prices = data["prices"]

        avg_price = round(sum(prices) / len(prices), 2) if prices else 0.01

        # 2-level fill: raw code → lookup by category_id → fallback từ cid prefix
        cat_fill = code or cat_to_code.get(cid) or f"category_{cid[:8]}"
        if not code:
            null_count += 1

        rows.append((pid, cid, code, cat_fill, brand, avg_price))

    logger.info("[Seed] Products với NULL category_code: %d / %d (%.1f%%) → đã fill bằng cat_to_code lookup",
                null_count, len(rows), null_count / len(rows) * 100 if rows else 0)
    return rows


# ── Bulk Insert ─────────────────────────────────────────────────────────────────
def bulk_insert(cursor, rows: list[tuple]) -> int:
    """Batch insert theo BATCH_SIZE, trả về tổng rows đã xử lý."""
    total = len(rows)
    done  = 0
    for i in range(0, total, BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        psycopg2.extras.execute_values(cursor, INSERT_SQL, batch)
        done += len(batch)
        if done % (BATCH_SIZE * 5) == 0 or done == total:
            logger.info("[Seed] Insert: %d / %d (%.0f%%)",
                        done, total, done / total * 100)
    return done


# ── Main ────────────────────────────────────────────────────────────────────────
def main() -> None:
    start = time.time()
    logger.info("[Seed] ════════════════════════════════════════")
    logger.info("[Seed]   Bắt đầu seeding Product Catalog")
    logger.info("[Seed] ════════════════════════════════════════")
    logger.info("[Seed] SEED_LIMIT=%s | CHUNK_SIZE=%d | BATCH_SIZE=%d",
                SEED_LIMIT or "không giới hạn", CHUNK_SIZE, BATCH_SIZE)

    if not CSV_FILES:
        logger.error("[Seed] Không tìm thấy file CSV trong: %s", CSV_DIR)
        sys.exit(1)

    logger.info("[Seed] Files: %s", [f.name for f in CSV_FILES])

    # 1. Kết nối DB
    try:
        conn   = connect_db()
        cursor = conn.cursor()
        logger.info("[Seed] Kết nối PostgreSQL thành công tại %s:%s/%s",
                    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME"))
    except Exception as e:
        logger.critical("[Seed] Kết nối PostgreSQL thất bại: %s", e)
        sys.exit(1)

    try:
        # 2. Setup bảng
        setup_products_table(cursor)

        # 3. Parse CSV (streaming — không OOM với 13GB+)
        products, cat_to_code = parse_csv_files()

        if not products:
            logger.warning("[Seed] Không có product nào được trích xuất. Kiểm tra lại CSV.")
            return

        # 4. Build insert data + fill NULLs
        logger.info("[Seed] Đang chuẩn bị %d rows để insert...", len(products))
        rows = build_insert_rows(products, cat_to_code)

        # 5. Bulk insert
        logger.info("[Seed] Bắt đầu bulk insert vào PostgreSQL...")
        total_inserted = bulk_insert(cursor, rows)

        # 6. Verify
        cursor.execute("SELECT COUNT(*) FROM products;")
        db_count = cursor.fetchone()[0]

        elapsed = time.time() - start
        logger.info("[Seed] ════════════════════════════════════════")
        logger.info("[Seed]   Hoàn tất! Thời gian: %.1fs", elapsed)
        logger.info("[Seed]   Rows xử lý : %d", total_inserted)
        logger.info("[Seed]   Trong DB   : %d products", db_count)
        logger.info("[Seed] ════════════════════════════════════════")

    except Exception as e:
        logger.error("[Seed] Lỗi trong quá trình seeding: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()
        logger.info("[Seed] Đã đóng kết nối PostgreSQL.")


if __name__ == "__main__":
    main()
