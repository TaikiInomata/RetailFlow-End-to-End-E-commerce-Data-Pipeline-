import os
import sys
import time
import json
import random
import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
# pyrefly: ignore [missing-import]
from confluent_kafka import Producer
# pyrefly: ignore [missing-import]
from confluent_kafka.admin import AdminClient, NewTopic
# pyrefly: ignore [missing-import]
from faker import Faker

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

# ── Config từ .env ────────────────────────────────────────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME      = os.getenv("CLICKSTREAM_TOPIC", "retailflow_clickstream")
USER_POOL_SIZE  = int(os.getenv("CLICKSTREAM_USER_POOL_SIZE", "1000"))
SESSION_TIMEOUT = 1800   # 30 phút — session hết hạn nếu không hoạt động

fake = Faker()

# ── Dữ liệu tham chiếu ────────────────────────────────────────────────────────
DEVICES         = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS  = [55, 38, 7]        # Mobile chiếm đa số trong e-commerce

REFERRERS       = ["direct", "google.com", "facebook.com", "instagram.com", "email", "tiktok.com"]
REFERRER_WEIGHTS = [25, 35, 15, 10, 10, 5]

CATEGORIES      = ["electronics", "fashion", "home-garden", "sports", "beauty", "books"]
CATEGORY_WEIGHTS = [25, 30, 18, 12, 10, 5]  # Fashion & Electronics chiếm đa số (thực tế Statista)

CLICK_TARGETS   = ["product_card", "nav_link", "banner", "promo_badge"]
CLICK_TARGET_WEIGHTS = [50, 25, 15, 10]    # product_card chiếm ~50% clicks (Hotjar data)

CHECKOUT_STEPS  = ["shipping", "payment", "confirmation"]
CHECKOUT_STEP_WEIGHTS = [45, 35, 20]       # Funnel drop-off: 45% vào shipping, 20% hoàn tất

SEARCH_TERMS    = ["laptop",  "sneakers",  "headphones", "dress", "blender",
                   "yoga mat", "phone case", "running shoes", "watch", "backpack"]
# Power law: vài từ khóa đầu chiếm đa số lượt tìm kiếm (long tail phía sau)
# Nguồn tham khảo: Google Trends cho e-commerce Electronics (VN/global)
SEARCH_TERM_WEIGHTS = [18,     8,          20,           4,      2,
                        3,       15,          8,              13,     9]

# Phân phối thực tế của funnel e-commerce (tổng = 100)
EVENT_TYPES     = ["scroll", "view_item", "click", "search", "add_to_cart", "checkout"]
EVENT_WEIGHTS   = [30,       28,          20,      10,       8,             4]
BASE_URL        = "https://retailflow.example.com"


class SessionManager:
    """
    Quản lý mapping user_id → session_id theo cơ chế timeout thực tế.

    Logic:
      - User truy cập lần đầu hoặc sau khi session hết hạn → tạo session_id mới.
      - User có event trong vòng SESSION_TIMEOUT giây → dùng lại session_id cũ.
      - Giới hạn số session lưu trong RAM bằng LRU-style eviction.

    Đây là điều kiện bắt buộc để downstream có thể tính:
      session duration, bounce rate, events-per-session, conversion funnel.
    """
    def __init__(self, max_sessions: int = 5000):
        # Dict: user_id → (session_id, last_event_unix_timestamp)
        self._sessions: dict[int, tuple[str, float]] = {}
        self._max = max_sessions

    def get_session(self, user_id: int) -> str:
        now = time.time()

        if user_id in self._sessions:
            sid, last_time = self._sessions[user_id]
            if now - last_time < SESSION_TIMEOUT:
                self._sessions[user_id] = (sid, now)   # refresh — kéo dài session
                return sid

        # Session mới: hết hạn timeout hoặc user lần đầu xuất hiện
        if len(self._sessions) >= self._max:
            # LRU eviction: xóa entry cũ nhất
            oldest_uid = min(self._sessions, key=lambda k: self._sessions[k][1])
            del self._sessions[oldest_uid]

        new_sid = f"sess-{uuid.uuid4().hex[:12]}"
        self._sessions[user_id] = (new_sid, now)
        return new_sid

    def active_count(self) -> int:
        return len(self._sessions)


def build_event_payload(
    user_id: int,
    session_id: str,
    event_type: str,
    device: str,
    override_product_id: str | None = None,
    override_base_price: float | None = None,
) -> dict:
    """
    Xây dựng payload event với các trường phù hợp theo từng event_type.
    Phản ánh đúng cấu trúc clickstream thực tế của hệ thống e-commerce.
    """
    # Dùng product_id thực từ SharedCatalog (trỏ vào Kaggle CSV) nếu có
    item_id    = override_product_id or "1002890"
    base_price = override_base_price or 489.07
    # Weighted: Fashion 30%, Electronics 25%, Home 18%, Sports 12%, Beauty 10%, Books 5%
    category   = random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS, k=1)[0]
    base_url   = "https://retailflow.example.com"

    # Trường chung: có mặt trong MỌI event
    payload: dict = {
        "event_id"        : str(uuid.uuid4()),
        "user_id"         : user_id,
        "session_id"      : session_id,
        "event_type"      : event_type,
        "device"          : device,
        "referrer"        : random.choices(REFERRERS, weights=REFERRER_WEIGHTS, k=1)[0],
        "ip_address"      : fake.ipv4_public(),
        "user_agent"      : fake.user_agent(),
        "event_timestamp" : datetime.now(timezone.utc).isoformat(),
    }

    # Trường đặc thù theo event_type
    if event_type == "view_item":
        payload["item_id"]     = item_id
        payload["page_url"]    = f"{BASE_URL}/products/{item_id.lower()}"
        # Lệch phải: đa số page view ngắn (mode 20s), rất ít người ờ hơn 2 phút
        payload["duration_ms"] = int(random.triangular(5_000, 180_000, 20_000))

    elif event_type == "scroll":
        payload["page_url"] = random.choices(
            [
                f"{BASE_URL}/",
                f"{BASE_URL}/category/{category}",
                f"{BASE_URL}/products/{item_id.lower()}",
            ],
            weights=[30, 45, 25],   # Category pages được scroll nhiều nhất
            k=1
        )[0]
        # Lệch phải: ~40% user chỉ scroll đến 25% trang (mode 20%)
        payload["scroll_depth_pct"] = int(random.triangular(5, 100, 20))
        # Lệch phải: scroll thường ngắn (mode 12s), ít ai scroll 2 phút
        payload["duration_ms"]      = int(random.triangular(3_000, 120_000, 12_000))

    elif event_type == "search":
        # Power law: headphones 20%, laptop 18%, phone case 15% chiếm phần lớn
        query = random.choices(SEARCH_TERMS, weights=SEARCH_TERM_WEIGHTS, k=1)[0]
        payload["search_query"] = query
        payload["page_url"]     = f"{BASE_URL}/search?q={query.replace(' ', '+')}"
        # Lệch phải: phần lớn query có 30–80 kết quả (mode 45); ~8% có 0 kết quả
        payload["result_count"] = int(random.triangular(0, 200, 45))

    elif event_type == "click":
        payload["page_url"]     = f"{BASE_URL}/category/{category}"
        # Weighted: product_card 50%, nav_link 25%, banner 15%, promo_badge 10%
        payload["click_target"] = random.choices(CLICK_TARGETS, weights=CLICK_TARGET_WEIGHTS, k=1)[0]

    elif event_type == "add_to_cart":
        payload["item_id"]  = item_id
        payload["page_url"] = f"{base_url}/products/{item_id}"
        # Lệch phải mạnh: 60% user chỉ mua 1 item, rất ít mua 5+
        payload["quantity"] = random.choices(
            [1, 2, 3, 4, 5, 6],
            weights=[60, 22, 10, 5, 2, 1],
            k=1
        )[0]

    elif event_type == "checkout":
        # Weighted funnel drop-off: shipping 45% → payment 35% → confirmation 20%
        step     = random.choices(CHECKOUT_STEPS, weights=CHECKOUT_STEP_WEIGHTS, k=1)[0]
        # Lệch phải: đa số mua 1-2 item, hiếm khi checkout cả giỏ 5+ món
        quantity = random.choices(
            [1, 2, 3, 4, 5],
            weights=[55, 25, 12, 5, 3],
            k=1
        )[0]
        cart_total = round(base_price * quantity * random.uniform(0.8, 1.2), 2)
        payload["item_id"]    = item_id
        payload["page_url"]   = f"{base_url}/checkout"
        payload["cart_total"] = cart_total
        payload["step"]       = step
        # pseudo_order_id: Gold layer dùng để fuzzy join với CDC orders
        # (match trên user_id + product_id + timestamp window ±60s)
        if step == "confirmation":
            payload["pseudo_order_id"]  = str(uuid.uuid4())
            payload["purchase_amount"]  = cart_total

    return payload


def delivery_report(err, msg):
    """
    Callback khi Kafka xác nhận message đã được gửi hoặc thất bại.
    Bỏ qua log success để tránh nhiễu (tốc độ gửi cao).
    """
    if err is not None:
        logger.error("[Bot] Gửi event thất bại: %s", err)


def connect_db():
    """Kết nối PostgreSQL read-only để load product/user pool."""
    return psycopg2.connect(
        host     = os.getenv("DB_HOST", "localhost"),
        port     = os.getenv("DB_PORT", "5433"),
        database = os.getenv("DB_NAME"),
        user     = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
    )


def run_bot():
    """Khởi chạy Clickstream Bot — bơm events vào Kafka liên tục."""
    # ── Đảm bảo Topic Tồn Tại (Auto-Create) ──────────────────────────────────
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    topic_metadata = admin_client.list_topics(timeout=10)
    
    if TOPIC_NAME not in topic_metadata.topics:
        logger.info("[Bot] Topic '%s' chưa tồn tại. Đang tạo mới...", TOPIC_NAME)
        # Tạo topic với 3 partitions, replication factor = 1 (do đang chạy 1 broker)
        new_topic = NewTopic(TOPIC_NAME, num_partitions=3, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        # Chờ quá trình tạo hoàn tất
        for topic, f in fs.items():
            try:
                f.result()  # Sẽ throw exception nếu fail
                logger.info("[Bot] Đã tạo thành công topic '%s'.", topic)
            except Exception as e:
                logger.error("[Bot] Lỗi khi tạo topic '%s': %s", topic, e)
                sys.exit(1)
    else:
        logger.info("[Bot] Topic '%s' đã tồn tại. Sẵn sàng gửi data.", TOPIC_NAME)

    # ── Kafka Producer ───────────────────────────────────────────────────────
    conf = {
        "bootstrap.servers" : KAFKA_BROKER,
        "client.id"         : "clickstream-bot-1",
        "linger.ms"         : 5,
        "compression.type"  : "lz4",
        "acks"              : "1",
    }
    try:
        producer = Producer(conf)
    except Exception as e:
        logger.critical("[Bot] Không thể khởi tạo Kafka Producer tại %s: %s", KAFKA_BROKER, e)
        sys.exit(1)

    # ── SharedCatalog: load real product_ids + user_ids từ PostgreSQL ────────
    catalog = None
    try:
        pg_conn = connect_db()
        pg_conn.autocommit = True
        catalog = SharedCatalog(pg_conn, refresh_interval=300, sample_size=10_000)
        logger.info("[Bot] Catalog: %d products | max user_id: %d",
                    catalog.product_count(), catalog.max_user_id())
    except Exception as e:
        logger.warning("[Bot] Không thể kết nối PostgreSQL, dùng fallback user_id pool: %s", e)
        pg_conn = None

    session_mgr = SessionManager(max_sessions=USER_POOL_SIZE * 2)

    logger.info("[Bot] === Bắt đầu bơm Clickstream vào topic '%s' (broker: %s) ===",
                TOPIC_NAME, KAFKA_BROKER)
    logger.info("[Bot] Session timeout: %ds | Nhấn Ctrl+C để dừng.", SESSION_TIMEOUT)

    msg_count = 0
    try:
        while True:
            # user_id: dùng real IDs từ PostgreSQL nếu có, fallback về pool cố định
            if catalog:
                max_uid = catalog.max_user_id()
                user_id = random.randint(1, max(max_uid, 1))
            else:
                user_id = random.randint(1, USER_POOL_SIZE)

            session_id = session_mgr.get_session(user_id)
            device     = random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]
            event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

            # product_id và base_price: chỉ cần cho events có sản phẩm cụ thể.
            # scroll/click/search KHÔNG cần → bỏ qua, tiết kiệm 1 lần lookup.
            PRODUCT_EVENTS = ("view_item", "add_to_cart", "checkout")
            if event_type in PRODUCT_EVENTS:
                if catalog:
                    product_id, base_price = catalog.pick_product()  # Real ID từ Kaggle
                else:
                    # Fallback khi PostgreSQL không kết nối được
                    product_id, base_price = ("1003461", 489.07)     # ID thực từ dataset
            else:
                product_id, base_price = (None, None)  # scroll/click/search không có product

            event = build_event_payload(
                user_id, session_id, event_type, device,
                override_product_id=product_id,
                override_base_price=base_price,
            )

            producer.produce(
                topic    = TOPIC_NAME,
                key      = str(user_id).encode("utf-8"),
                value    = json.dumps(event).encode("utf-8"),
                callback = delivery_report,
            )
            producer.poll(0)

            msg_count += 1
            if msg_count % 500 == 0:
                logger.info("[Bot] Đã bơm %d events | Sessions active: %d",
                            msg_count, session_mgr.active_count())

            time.sleep(random.uniform(0.01, 0.1))

    except KeyboardInterrupt:
        logger.info("[Bot] Nhận tín hiệu dừng — đang flush buffer...")
    finally:
        producer.flush(timeout=5.0)
        if pg_conn:
            pg_conn.close()
        logger.info("[Bot] Đã đóng Clickstream Bot. Tổng events đã sinh: %d", msg_count)


if __name__ == "__main__":
    run_bot()