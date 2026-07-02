import os
import sys
import time
import uuid
import random
import logging
from pathlib import Path

import psycopg2
# pyrefly: ignore [missing-import]
from faker import Faker
# pyrefly: ignore [missing-import]
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / '.env')

# SharedCatalog nằm ở scripts/utils/ — thêm đúng thư mục vào sys.path
sys.path.insert(0, str(Path(__file__).parent.parent / 'utils'))
# pyrefly: ignore [missing-import]
from shared_catalog import SharedCatalog  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ── Hằng số ───────────────────────────────────────────────────────────────────
MAX_TRACKED_ORDERS  = 200
SIMULATION_INTERVAL = float(os.getenv("SIMULATION_INTERVAL_SECONDS", "1.0"))

# Phương thức thanh toán + trọng số (Statista VN 2024)
PAYMENT_METHODS = ["credit_card", "bank_transfer", "momo", "vnpay", "paypal", "cash_on_delivery"]
PAYMENT_WEIGHTS = [20,            20,              22,     18,      5,        15]
# credit_card 20% | bank_transfer 20% | MoMo 22% (ví điện tử #1 VN)
# VNPay 18% | PayPal 5% (quốc tế) | COD 15% (thanh toán khi nhận hàng)

# Số lượng sản phẩm trong 1 đơn hàng (lệch phải mạnh — Euromonitor B2C 2023)
QUANTITY_VALUES  = [1,   2,   3,   4,   5]
QUANTITY_WEIGHTS = [52,  25,  12,  7,   4]  # avg ≈1.86 items/order


class RandomizedOrderSet:
    """
    Cấu trúc dữ liệu hỗ trợ ba thao tác với độ phức tạp O(1) trung bình:
      - add(id)       : thêm order vào tập
      - remove(id)    : xóa một order cụ thể
      - pick_random() : lấy ngẫu nhiên 1 phần tử

    Nguyên lý: kết hợp List (random access O(1)) + Dict (lookup O(1)).
    Xóa dùng kỹ thuật "swap-and-pop" để tránh dịch chuyển phần tử.
    Đây là pattern "RandomizedSet" kinh điển, khắc phục nhược điểm O(n) của deque.
    """

    def __init__(self, maxsize: int = 200) -> None:
        self._ids: list[int]       = []      # List để random access O(1)
        self._pos: dict[int, int]  = {}      # Dict: id → vị trí trong list
        self._maxsize = maxsize

    def add(self, order_id: int) -> None:
        if order_id in self._pos:
            return
        # Giới hạn kích thước: xóa phần tử cũ nhất (đầu list) nếu đầy
        if len(self._ids) >= self._maxsize:
            oldest = self._ids[0]
            self.remove(oldest)
        self._pos[order_id] = len(self._ids)
        self._ids.append(order_id)

    def remove(self, order_id: int) -> None:
        if order_id not in self._pos:
            return
        idx      = self._pos[order_id]
        last_id  = self._ids[-1]
        # Swap phần tử cần xóa với phần tử cuối → pop O(1)
        self._ids[idx]     = last_id
        self._pos[last_id] = idx
        self._ids.pop()
        del self._pos[order_id]

    def pick_random(self) -> int:
        return random.choice(self._ids)     # O(1) — List có random access

    def __len__(self) -> int:
        return len(self._ids)


class TransactionSimulator:

    def __init__(self) -> None:
        self.fake = Faker()
        try:
            self.conn = psycopg2.connect(
                host     = os.getenv("DB_HOST", "localhost"),
                port     = os.getenv("DB_PORT", "5433"),
                database = os.getenv("DB_NAME"),
                user     = os.getenv("DB_USER"),
                password = os.getenv("DB_PASSWORD"),
            )
            # Autocommit: mỗi lệnh SQL commit ngay → WAL → Debezium đọc được CDC
            self.conn.autocommit = True
            self.cursor          = self.conn.cursor()
            logger.info("[Simulator] Kết nối PostgreSQL thành công tại %s:%s/%s",
                        os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME"))
        except Exception as e:
            logger.error("[Simulator] Kết nối PostgreSQL thất bại: %s", e)
            raise

        # SharedCatalog: load product_id và user pool từ DB
        # Fallback-safe: tự dùng FALLBACK_PRODUCTS nếu bảng products chưa được seed
        self.catalog = SharedCatalog(self.conn)

    def setup_tables(self) -> None:
        """
        Khởi tạo schema CDC-ready với đầy đủ columns.
        Idempotent: dùng IF NOT EXISTS + ADD COLUMN IF NOT EXISTS → chạy lại an toàn.
        """
        logger.info("[Simulator] Đang thiết lập schema...")

        # ── TRIGGER FUNCTION: auto-update updated_at khi có UPDATE ───────────
        self.cursor.execute("""
            CREATE OR REPLACE FUNCTION set_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)

        # ── BẢNG USERS ────────────────────────────────────────────────────────
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id         SERIAL        PRIMARY KEY,
                name       VARCHAR(255)  NOT NULL,
                email      VARCHAR(255),             -- PII: demo masking trong Silver
                phone      VARCHAR(20),              -- PII: demo masking trong Silver
                city       VARCHAR(100),
                country    VARCHAR(100)  DEFAULT 'VN',
                created_at TIMESTAMPTZ  DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ  DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Schema migration: thêm columns mới vào bảng đã tồn tại (idempotent)
        for col_sql in [
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS email   VARCHAR(255);",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS phone   VARCHAR(20);",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS city    VARCHAR(100);",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS country VARCHAR(100) DEFAULT 'VN';",
        ]:
            self.cursor.execute(col_sql)

        # REPLICA IDENTITY FULL: Debezium nhận full row trong before-image khi UPDATE/DELETE
        self.cursor.execute("ALTER TABLE users REPLICA IDENTITY FULL;")
        self.cursor.execute("""
            CREATE OR REPLACE TRIGGER trg_users_updated_at
                BEFORE UPDATE ON users
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        """)

        # ── BẢNG ORDERS ───────────────────────────────────────────────────────
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id             SERIAL          PRIMARY KEY,
                user_id        INT             REFERENCES users(id) ON DELETE CASCADE,
                product_id     VARCHAR(20),                -- FK mềm tới products table (từ seed CSV)
                quantity       INT             NOT NULL DEFAULT 1,
                unit_price     DECIMAL(10, 2)  NOT NULL DEFAULT 0.01,
                total_amount   DECIMAL(10, 2)  NOT NULL DEFAULT 0.01, -- unit_price × quantity
                amount         DECIMAL(10, 2)  NOT NULL DEFAULT 0.01, -- Legacy: giữ tương thích ngược
                currency       VARCHAR(3)      NOT NULL DEFAULT 'USD',
                payment_method VARCHAR(50)     NOT NULL DEFAULT 'credit_card',
                status         VARCHAR(50)     NOT NULL
                                               CHECK (status IN ('PENDING', 'COMPLETED', 'CANCELLED')),
                created_at     TIMESTAMPTZ     DEFAULT CURRENT_TIMESTAMP,
                updated_at     TIMESTAMPTZ     DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Schema migration cho bảng orders đã tồn tại
        for col_sql in [
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS product_id     VARCHAR(20);",
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS quantity        INT            DEFAULT 1;",
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS unit_price      DECIMAL(10,2)  DEFAULT 0.01;",
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_amount    DECIMAL(10,2)  DEFAULT 0.01;",
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS currency        VARCHAR(3)     DEFAULT 'USD';",
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_method  VARCHAR(50)    DEFAULT 'credit_card';",
        ]:
            self.cursor.execute(col_sql)

        self.cursor.execute("ALTER TABLE orders REPLICA IDENTITY FULL;")
        self.cursor.execute("""
            CREATE OR REPLACE TRIGGER trg_orders_updated_at
                BEFORE UPDATE ON orders
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        """)

        logger.info("[Simulator] Schema sẵn sàng: users + orders (CDC-ready, REPLICA IDENTITY FULL).")

    def run_simulation(self) -> None:
        """Vòng lặp sinh dữ liệu CDC liên tục: INSERT / UPDATE / DELETE."""
        logger.info("[Simulator] === Bắt đầu mô phỏng giao dịch. Bấm Ctrl+C để dừng. ===")
        logger.info("[Simulator] Catalog: %d products | Interval: %.1fs",
                    self.catalog.product_count(), SIMULATION_INTERVAL)

        active_orders = RandomizedOrderSet(maxsize=MAX_TRACKED_ORDERS)

        try:
            while True:
                # ── 1. TẠO USER MỚI (INSERT) ────────────────────────────────
                name    = self.fake.name()
                # Email có UUID prefix để tránh duplicate key constraint
                email   = f"{uuid.uuid4().hex[:8]}@{self.fake.domain_name()}"
                phone   = self.fake.phone_number()[:20]
                city    = self.fake.city()

                self.cursor.execute(
                    "INSERT INTO users (name, email, phone, city) VALUES (%s, %s, %s, %s) RETURNING id;",
                    (name, email, phone, city)
                )
                user_id = self.cursor.fetchone()[0]

                # ── 2. TẠO ĐƠN HÀNG (INSERT — mặc định PENDING) ─────────────
                # product_id lấy từ SharedCatalog → trỏ vào products table thực
                product_id, base_price = self.catalog.pick_product()
                unit_price     = round(base_price * random.uniform(0.8, 1.2), 2)   # ±20% variation
                # Lệch phải mạnh: 52% khách hàng chỉ mua 1 món (Euromonitor B2C)
                quantity       = random.choices(QUANTITY_VALUES, weights=QUANTITY_WEIGHTS, k=1)[0]
                total_amount   = round(unit_price * quantity, 2)
                # Weighted theo thị phần thực tế VN: MoMo 22%, bank_transfer 20%, card 20%...
                payment_method = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]

                self.cursor.execute(
                    """INSERT INTO orders
                       (user_id, product_id, quantity, unit_price, total_amount, amount,
                        currency, payment_method, status)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       RETURNING id;""",
                    (user_id, product_id, quantity, unit_price, total_amount,
                     total_amount,  # amount = total_amount (legacy compat)
                     'USD', payment_method, 'PENDING')
                )
                order_id = self.cursor.fetchone()[0]
                active_orders.add(order_id)
                logger.info("[INSERT] Đơn hàng #%d | User '%s' | Product %s | x%d | $%.2f | %s",
                            order_id, name, product_id, quantity, total_amount, payment_method)

                # ── 3. CHUYỂN TRẠNG THÁI (UPDATE — xác suất 30%) ────────────
                if random.random() < 0.3 and len(active_orders) > 1:
                    update_id  = active_orders.pick_random()          # O(1)
                    # Thực tế: ~82% đơn hoàn thành, ~18% bị huỷ (eCommerce SEA report)
                    new_status = random.choices(
                        ['COMPLETED', 'CANCELLED'],
                        weights=[82, 18],
                        k=1
                    )[0]
                    self.cursor.execute(
                        # updated_at tự cập nhật bởi trigger trg_orders_updated_at
                        "UPDATE orders SET status = %s WHERE id = %s;",
                        (new_status, update_id)
                    )
                    active_orders.remove(update_id)                    # O(1)
                    logger.info("[UPDATE] Đơn hàng #%d → %s", update_id, new_status)

                # ── 4. XÓA ĐỚN HÀNG (DELETE — xác suất 7%) ─────────────────
                # Hard delete hiếm gặp trong production (fraud, test orders, admin)
                # Giữ ở 7% để đảm bảo CDC DELETE events xuất hiện trong pipeline
                if random.random() < 0.07 and len(active_orders) > 1:
                    delete_id = active_orders.pick_random()            # O(1)
                    self.cursor.execute("DELETE FROM orders WHERE id = %s;", (delete_id,))
                    active_orders.remove(delete_id)                    # O(1)
                    logger.warning("[DELETE] Đơn hàng #%d đã bị xóa.", delete_id)

                time.sleep(SIMULATION_INTERVAL)

        except KeyboardInterrupt:
            logger.info("[Simulator] Nhận tín hiệu dừng — đang tắt an toàn...")
        finally:
            self.cursor.close()
            self.conn.close()
            logger.info("[Simulator] Đã đóng kết nối PostgreSQL.")


if __name__ == "__main__":
    try:
        simulator = TransactionSimulator()
        simulator.setup_tables()
        simulator.run_simulation()
    except Exception as e:
        logger.critical("[Simulator] Không thể khởi động: %s", e)
        sys.exit(1)