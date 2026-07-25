"""
shared_catalog.py
=================
Module dùng chung cho mock_backend.py và clickstream_bot.py.

Giải quyết vấn đề liên thông: cả 2 simulation script đều tham chiếu
cùng product_id và user_id từ PostgreSQL → 3 nguồn dữ liệu join được nhau.

Design:
  - Auto-refresh cache sau refresh_interval giây (mặc định 5 phút)
  - Fallback-safe: không crash nếu products chưa được seed
  - Thread-safe cho trường hợp dùng đa luồng trong tương lai
"""
import time
import random
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Fallback dùng khi products table chưa được seed
FALLBACK_PRODUCTS: list[tuple[str, float]] = [
    ("1003461", 489.07),
    ("1005115", 200.00),
    ("1002586", 150.00),
    ("4804056", 79.99),
    ("1004258", 350.00),
]


class SharedCatalog:
    """
    Cache danh sách products và max user_id từ PostgreSQL.

    Sử dụng:
        catalog = SharedCatalog(conn)
        product_id, base_price = catalog.pick_product()
        pool_size = catalog.max_user_id()
    """

    def __init__(
        self,
        conn,
        refresh_interval: int = 300,
        sample_size: int = 10_000,
    ) -> None:
        """
        Args:
            conn            : psycopg2 connection (autocommit mode)
            refresh_interval: Giây giữa 2 lần auto-refresh (default 5 phút)
            sample_size     : Số products load vào RAM (default 10.000)
        """
        self._conn             = conn
        self._refresh_interval = refresh_interval
        self._sample_size      = sample_size

        self._products: list[tuple[str, float]] = []
        self._max_user_id: int                  = 1000      # fallback
        self._order_ids: list[int]              = []        # Real COMPLETED order IDs
        self._last_refresh: float               = 0.0

        self.refresh()

    # ── Public API ────────────────────────────────────────────────────────────

    def pick_product(self) -> tuple[str, float]:
        """
        Lấy ngẫu nhiên 1 sản phẩm từ cache. O(1).
        Returns: (product_id, avg_price)
        """
        self._maybe_refresh()
        return random.choice(self._products)

    def pick_order_id(self) -> Optional[int]:
        """
        Lấy ngẫu nhiên 1 real order_id (status=COMPLETED) từ cache.
        Dùng cho checkout event trong Clickstream Bot để thay thế pseudo_order_id.
        Returns: order_id (int) hoặc None nếu chưa có order nào trong DB.
        """
        self._maybe_refresh()
        if not self._order_ids:
            return None
        return random.choice(self._order_ids)

    def max_user_id(self) -> int:
        """
        Trả về MAX(id) hiện tại trong bảng users.
        clickstream_bot dùng để sinh user_id trong range thực của DB.
        """
        self._maybe_refresh()
        return self._max_user_id

    def product_count(self) -> int:
        """Số products đang cache trong RAM."""
        return len(self._products)

    def refresh(self) -> None:
        """Load fresh data từ PostgreSQL vào cache."""
        try:
            with self._conn.cursor() as cur:
                # ── Products sample ──────────────────────────────────────────
                cur.execute("""
                    SELECT product_id, avg_price::float
                    FROM   products
                    ORDER  BY RANDOM()
                    LIMIT  %s;
                """, (self._sample_size,))

                rows = cur.fetchall()
                if rows:
                    self._products = [(r[0], float(r[1])) for r in rows]
                else:
                    self._products = FALLBACK_PRODUCTS
                    logger.warning(
                        "[Catalog] Bảng 'products' trống hoặc chưa tồn tại "
                        "→ dùng fallback. Hãy chạy seed_product_catalog.py trước."
                    )

                # ── Max user_id ──────────────────────────────────────────────
                cur.execute("SELECT COALESCE(MAX(id), 1000) FROM users;")
                self._max_user_id = cur.fetchone()[0]

                # ── Orders sample: chỉ lấy COMPLETED orders (đơn thật đã xong) ─
                cur.execute("""
                    SELECT id FROM orders
                    WHERE  status = 'COMPLETED'
                    ORDER  BY RANDOM()
                    LIMIT  1000;
                """)
                order_rows = cur.fetchall()
                self._order_ids = [r[0] for r in order_rows]

            self._last_refresh = time.time()
            logger.info(
                "[Catalog] Refreshed: %d products | max user_id: %d | %d completed orders",
                len(self._products), self._max_user_id, len(self._order_ids),
            )

        except Exception as exc:
            # Không crash simulation nếu refresh lỗi — giữ cache cũ
            logger.warning("[Catalog] Refresh thất bại (giữ cache cũ): %s", exc)
            if not self._products:
                self._products = FALLBACK_PRODUCTS

    # ── Private ───────────────────────────────────────────────────────────────

    def _maybe_refresh(self) -> None:
        """Tự động refresh nếu cache đã quá hạn."""
        if time.time() - self._last_refresh > self._refresh_interval:
            self.refresh()
