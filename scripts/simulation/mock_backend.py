import os
import sys
import time
import random
import logging
from pathlib import Path

import psycopg2
# pyrefly: ignore [missing-import]
from faker import Faker
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / '.env')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

MAX_TRACKED_ORDERS = 200
SIMULATION_INTERVAL = 1.0


class RandomizedOrderSet:
    """
    Cấu trúc dữ liệu hỗ trợ ba thao tác với độ phức tạp O(1) trung bình:
      - add(id)       : thêm order vào tập
      - remove(id)    : xóa một order cụ thể
      - pick_random() : lấy ngẫu nhiên một order

    Nguyên lý:
      - List  → truy cập ngẫu nhiên bằng integer index trong O(1)
      - Dict  → tra cứu vị trí phần tử trong O(1)
      - "Swap-and-pop" → xóa phần tử bất kỳ trong O(1) mà không dịch chuyển toàn bộ mảng

    Ví dụ swap-and-pop khi xóa id=30 khỏi [10, 20, 30, 40, 50]:
      1. Tìm vị trí 30 qua dict  → idx = 2             (O(1))
      2. Lấy phần tử cuối        → last = 50            (O(1))
      3. Đặt 50 vào vị trí idx=2 → [10, 20, 50, 40, 50] (O(1))
      4. Cập nhật dict: 50 → idx=2                      (O(1))
      5. Pop phần tử cuối        → [10, 20, 50, 40]     (O(1))
    Không cần dịch chuyển 40, 50 như list.remove() thông thường.
    """
    def __init__(self, maxsize: int = 200):
        self._ids: list[int] = []        # List cho phép random.choice O(1) qua integer index
        self._index: dict[int, int] = {} # Dict: order_id → vị trí trong _ids  (tra cứu O(1))
        self._maxsize = maxsize

    def add(self, order_id: int) -> None:
        """Thêm order_id vào tập. Nếu đầy, xóa bớt 1 phần tử trước."""
        if order_id in self._index:
            return
        # Khi đầy, loại bỏ phần tử ở vị trí 0 (đã tồn tại lâu nhất trong list)
        if len(self._ids) >= self._maxsize:
            self.remove(self._ids[0])
        self._ids.append(order_id)
        self._index[order_id] = len(self._ids) - 1

    def remove(self, order_id: int) -> None:
        """Xóa order_id bằng kỹ thuật swap-and-pop — O(1)."""
        if order_id not in self._index:
            return
        idx = self._index.pop(order_id)     # O(1): lấy vị trí, đồng thời xóa khỏi dict
        last_id = self._ids[-1]             # O(1): lấy phần tử cuối
        if last_id != order_id:             # Edge case: target đã là phần tử cuối rồi
            self._ids[idx] = last_id        # O(1): swap phần tử cuối vào vị trí cần xóa
            self._index[last_id] = idx      # O(1): cập nhật vị trí mới của last_id trong dict
        self._ids.pop()                     # O(1): xóa phần tử cuối (đã duplicate hoặc là target)

    def pick_random(self) -> int | None:
        """Lấy ngẫu nhiên 1 order_id — O(1) nhờ integer index trên List."""
        if not self._ids:
            return None
        return random.choice(self._ids)

    def __len__(self) -> int:
        return len(self._ids)


class TransactionSimulator:
    def __init__(self):
        self.fake = Faker()
        try:
            self.conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5433"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD")
            )
            # Bật Autocommit: mỗi lệnh SQL được commit ngay lập tức xuống WAL
            # → Debezium mới đọc được event CDC
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            logger.info("[Simulator] Kết nối tới PostgreSQL thành công tại %s:%s/%s",
                        os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME"))
        except Exception as e:
            logger.error("[Simulator] Kết nối PostgreSQL thất bại: %s", e)
            raise

    def setup_tables(self):
        """Khởi tạo bảng và trigger nếu chưa tồn tại."""
        logger.info("[Simulator] Đang thiết lập lược đồ dữ liệu (Schema)...")

        # ── TRIGGER FUNCTION: tự động cập nhật updated_at khi có UPDATE ──────
        # CREATE OR REPLACE: idempotent, chạy lại nhiều lần vẫn an toàn
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
                id         SERIAL       PRIMARY KEY,
                name       VARCHAR(255) NOT NULL,
                created_at TIMESTAMPTZ  DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ  DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # REPLICA IDENTITY FULL: Debezium nhận toàn bộ row trong before-image
        # khi có UPDATE/DELETE (mặc định chỉ có PRIMARY KEY)
        self.cursor.execute("ALTER TABLE users REPLICA IDENTITY FULL;")
        self.cursor.execute("""
            CREATE OR REPLACE TRIGGER trg_users_updated_at
                BEFORE UPDATE ON users
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        """)

        # ── BẢNG ORDERS ───────────────────────────────────────────────────────
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id         SERIAL         PRIMARY KEY,
                user_id    INT            REFERENCES users(id) ON DELETE CASCADE,
                amount     DECIMAL(10, 2) NOT NULL,
                status     VARCHAR(50)    NOT NULL
                                          CHECK (status IN ('PENDING', 'COMPLETED', 'CANCELLED')),
                created_at TIMESTAMPTZ    DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ    DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.cursor.execute("ALTER TABLE orders REPLICA IDENTITY FULL;")
        self.cursor.execute("""
            CREATE OR REPLACE TRIGGER trg_orders_updated_at
                BEFORE UPDATE ON orders
                FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        """)

        logger.info("[Simulator] Đã thiết lập xong schema, trigger và REPLICA IDENTITY cho 'users' và 'orders'.")


    def run_simulation(self):
        """Vòng lặp sinh dữ liệu CDC liên tục: INSERT, UPDATE, DELETE."""
        logger.info("[Simulator] === Bắt đầu mô phỏng giao dịch. Bấm Ctrl+C để dừng. ===")

        active_orders = RandomizedOrderSet(maxsize=MAX_TRACKED_ORDERS)

        try:
            while True:
                # ── 1. TẠO USER MỚI (INSERT) ────────────────────────────────
                name = self.fake.name()
                self.cursor.execute(
                    "INSERT INTO users (name) VALUES (%s) RETURNING id;", (name,)
                )
                user_id = self.cursor.fetchone()[0]

                # ── 2. TẠO ĐƠN HÀNG CHO USER (INSERT — mặc định PENDING) ───
                amount = round(random.uniform(10.0, 500.0), 2)
                self.cursor.execute(
                    "INSERT INTO orders (user_id, amount, status) VALUES (%s, %s, %s) RETURNING id;",
                    (user_id, amount, 'PENDING')
                )
                order_id = self.cursor.fetchone()[0]
                active_orders.add(order_id)
                logger.info("[INSERT] Tạo đơn hàng #%d cho '%s' — $%.2f", order_id, name, amount)

                # ── 3. CHUYỂN TRẠNG THÁI ĐƠN HÀNG (UPDATE — xác suất 30%) ──
                if random.random() < 0.3 and len(active_orders) > 1:
                    update_id = active_orders.pick_random()     # O(1)
                    new_status = random.choice(['COMPLETED', 'CANCELLED'])
                    self.cursor.execute(
                        # updated_at được tự động cập nhật bởi trigger trg_orders_updated_at
                        "UPDATE orders SET status = %s WHERE id = %s;",
                        (new_status, update_id)
                    )
                    active_orders.remove(update_id)             # O(1)
                    logger.info("[UPDATE] Đơn hàng #%d → %s", update_id, new_status)

                # ── 4. XÓA ĐƠN HÀNG (DELETE — xác suất 10%) ────────────────
                if random.random() < 0.1 and len(active_orders) > 1:
                    delete_id = active_orders.pick_random()     # O(1)
                    self.cursor.execute(
                        "DELETE FROM orders WHERE id = %s;", (delete_id,)
                    )
                    active_orders.remove(delete_id)             # O(1)
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