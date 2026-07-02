# Tài liệu: Issues & Giải pháp — 3 Nguồn Dữ liệu Bronze Layer

**Dự án:** CartStream (RetailFlow Data Platform)  
**Ngày lập:** 01/07/2026  
**Phạm vi:** Bronze Layer — 3 nguồn dữ liệu đầu vào  

---

## 1. Tổng quan 3 Nguồn Dữ liệu

| Nguồn | Loại | File/Tool | Đích đến |
|-------|------|-----------|----------|
| **Nguồn 1** | Batch / Historical | `2019-09.csv` + `2019-10.csv` (13.67 GB) | MinIO `bronze-zone/batch/historical/` |
| **Nguồn 2** | CDC / Transactional | `scripts/simulation/mock_backend.py` | PostgreSQL → Debezium → Kafka → MinIO |
| **Nguồn 3** | Streaming / Clickstream | `scripts/simulation/clickstream_bot.py` | Kafka → MinIO `bronze-zone/clickstream/` |

---

## 2. Issues — Nguồn 1: Batch CSV (Kaggle)

**Schema gốc:**
```
event_time | event_type | product_id | category_id | category_code | brand | price | user_id | user_session
```

### 2.1 Issues Nội bộ

| # | Issue | Mức độ | Chi tiết |
|---|-------|--------|----------|
| 1.1 | **Chỉ 2 tháng dữ liệu** | 🟡 Trung bình | Sep–Oct 2019. Không test được seasonality, YoY comparison |
| 1.2 | **Không có `quantity`** | 🟡 Trung bình | Event `purchase` chỉ có `price` của 1 item, không có số lượng hay cart total |
| 1.3 | **`category_code` sparse** | 🟡 Trung bình | 57.1% rows có NULL `category_code` (đặc điểm nổi tiếng của dataset này) |
| 1.4 | **`event_type` chỉ 3 loại** | 🟢 Thấp | Chỉ có `view/cart/purchase` — đơn giản hơn Nguồn 3, naming khác nhau |

### 2.2 Issues Liên thông

| # | Issue | Hậu quả |
|---|-------|---------|
| 1.5 | **`user_id` format 8–9 chữ số** (vd: `520088904`) | Không join được với CDC `user_id` (sequential: 1, 2, 3…) |
| 1.6 | **`product_id` format số thuần** (vd: `1003461`) | Không khớp với Nguồn 3 (`item_id = "PROD-123"`) |

---

## 3. Issues — Nguồn 2: CDC `mock_backend.py`

**Schema gốc:**
```sql
users (id, name, created_at, updated_at)
orders (id, user_id, amount, status, created_at, updated_at)
```

### 3.1 Issues Schema

| # | Issue | Mức độ | Hậu quả |
|---|-------|--------|---------|
| 2.1 | **`users` chỉ có `name`** | 🔴 Cao | Không demo được PII masking (email, phone) — kỹ năng quan trọng trong Data Eng |
| 2.2 | **`orders` không có `product_id`** | 🔴 Cao | Không join được với Nguồn 1 CSV (có product_id, brand, price) |
| 2.3 | **`orders` thiếu `quantity`, `payment_method`, `currency`** | 🟡 Trung bình | Thiếu dimension quan trọng cho phân tích doanh thu |
| 2.4 | **Chỉ 2 bảng** | 🟡 Trung bình | Thực tế OLTP có thêm: `products`, `inventory`, `payments`, `shipping` |

### 3.2 Issues Liên thông

| # | Issue | Hậu quả |
|---|-------|---------|
| 2.5 | **`user_id` sequential (1,2,3…) không đồng bộ với Nguồn 3** | Join `CDC.user_id ↔ Clickstream.user_id` không phản ánh đúng hành vi cùng 1 user |
| 2.6 | **Không có bảng `products` tham chiếu** | Không có nguồn chân lý cho product catalog — 3 nguồn dùng 3 format product_id khác nhau |

---

## 4. Issues — Nguồn 3: Clickstream `clickstream_bot.py`

**Schema gốc:**
```json
{ "event_id", "user_id", "session_id", "event_type", "item_id", "device", "event_timestamp" }
```

### 4.1 Issues Logic (Critical)

| # | Issue | Mức độ | Chi tiết |
|---|-------|--------|----------|
| 3.1 | **`session_id` random mỗi event** | 🔴 Cao | `f"sess-{random.randint(1000, 9999)}"` — mỗi click = 1 session mới. Phá vỡ toàn bộ session analysis |
| 3.2 | **`event_type` phân phối bằng nhau** | 🔴 Cao | `random.choice()` → mỗi loại 16.7%. Thực tế: `scroll` 30%, `checkout` chỉ 4% |
| 3.3 | **`item_id` gán cho mọi event** | 🟡 Trung bình | Event `search`, `scroll` không có item. `search` phải có `search_query` |

### 4.2 Issues Liên thông

| # | Issue | Mức độ | Hậu quả |
|---|-------|--------|---------|
| 3.4 | **`item_id = "PROD-123"` không khớp Nguồn 1** | 🔴 Cao | Không join được với CSV (product_id = "1003461") |
| 3.5 | **`user_id = random.randint(1, 1000)`** | 🔴 Cao | Không đồng bộ với CDC users — join không có ý nghĩa thực tế |
| 3.6 | **Không có `purchase` event** | 🟡 Trung bình | Không tính được Clickstream-to-Order conversion rate |
| 3.7 | **`event_type` naming khác CSV** | 🟢 Thấp | `view_item` vs `view`, `add_to_cart` vs `cart` — cần chuẩn hóa ở Silver |

### 4.3 Issues Code Quality

| # | Issue | Mức độ | Chi tiết |
|---|-------|--------|----------|
| 3.8 | **Config hardcoded** | 🟡 Trung bình | `KAFKA_BROKER = 'localhost:9092'` thay vì đọc từ `.env` |
| 3.9 | **Thiếu trường quan trọng** | 🟡 Trung bình | Không có `ip_address`, `user_agent`, `referrer`, `page_url`, `duration_ms` |
| 3.10 | **`snappy` compression** | 🟢 Thấp | Yêu cầu native lib ngoài, không bundled trong `confluent-kafka` trên Windows |
| 3.11 | **`return` khi Producer init fail** | 🟢 Thấp | Nên dùng `sys.exit(1)` để báo lỗi cho caller |
| 3.12 | **Logging format không nhất quán** | 🟢 Thấp | Khác với format của `mock_backend.py` trong cùng project |

---

## 5. Issues Cross-Source — Liên thông 3 Nguồn

```
Nguồn 1 CSV          Nguồn 2 CDC            Nguồn 3 Clickstream
────────────────     ──────────────────     ──────────────────────
user_id: 8-9 số      user_id: 1, 2, 3…      user_id: 1–1000 (random)
product_id: 7 số     orders: không có PID    item_id: "PROD-123"
event: view/cart      2 bảng đơn giản        event: scroll/search…
Năm 2019             Năm 2026               Năm 2026
```

**Hệ quả:** Không thể thực hiện bất kỳ cross-source join nào trong Gold layer —
vi phạm nguyên tắc *Single Source of Truth* của dự án.

---

## 6. Giải pháp Đã Triển khai

### 6.1 Track 1 — Seed Infrastructure: `scripts/seeds/seed_product_catalog.py`

**Mục tiêu:** Tạo bảng `products` trong PostgreSQL từ Kaggle CSV — nguồn chân lý cho `product_id`.

**Kết quả thực tế:**
- Đọc streaming 109,950,743 rows từ 2 file CSV (an toàn nhờ chunk 100K rows/lần)
- Trích xuất **206,876 unique products**
- Fill 57.1% NULL `category_code` bằng `category_id` lookup (`category_fill`)
- Tính `avg_price` theo từng product
- Bulk insert `ON CONFLICT DO NOTHING` → idempotent
- Thời gian chạy: **762 giây (~12.7 phút)**

**Schema:**
```sql
CREATE TABLE products (
    product_id    VARCHAR(20)    PRIMARY KEY,   -- "1003461" từ Kaggle CSV
    category_id   VARCHAR(30)    NOT NULL,
    category_code VARCHAR(100),                  -- Raw (có thể NULL)
    category_fill VARCHAR(100)   NOT NULL,       -- Đã fill — không bao giờ NULL
    brand         VARCHAR(100)   NOT NULL,
    avg_price     DECIMAL(10,2)  NOT NULL,
    created_at    TIMESTAMPTZ    DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE products REPLICA IDENTITY FULL;
```

**Fixes giải quyết:** 1.3, 1.6, 2.4, 2.6, 3.4

---

### 6.2 Track 2 — Shared Module: `scripts/simulation/shared_catalog.py`

**Mục tiêu:** Module dùng chung giữa `mock_backend.py` và `clickstream_bot.py` — đảm bảo cả 2 dùng cùng `product_id` và `user_id` từ PostgreSQL.

**Cơ chế:**
- Cache 10,000 `(product_id, avg_price)` từ bảng `products` (ngẫu nhiên)
- Cache `MAX(user_id)` từ bảng `users`
- Auto-refresh sau 300 giây (5 phút)
- Fallback-safe: không crash nếu `products` chưa được seed

**API:**
```python
catalog = SharedCatalog(conn)
product_id, price = catalog.pick_product()   # O(1)
max_uid = catalog.max_user_id()              # Để clickstream bot dùng real user IDs
```

**Fixes giải quyết:** 2.5, 3.4, 3.5

---

### 6.3 Track 3 — CDC Source Enhancement: `mock_backend.py`

**Schema users mới:**
```sql
ALTER TABLE users ADD COLUMN IF NOT EXISTS email   VARCHAR(255);  -- PII masking demo
ALTER TABLE users ADD COLUMN IF NOT EXISTS phone   VARCHAR(20);   -- PII masking demo
ALTER TABLE users ADD COLUMN IF NOT EXISTS city    VARCHAR(100);
ALTER TABLE users ADD COLUMN IF NOT EXISTS country VARCHAR(100) DEFAULT 'VN';
```

**Schema orders mới:**
```sql
ALTER TABLE orders ADD COLUMN IF NOT EXISTS product_id      VARCHAR(20);   -- FK mềm → products
ALTER TABLE orders ADD COLUMN IF NOT EXISTS quantity         INT DEFAULT 1;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS unit_price       DECIMAL(10,2) DEFAULT 0.01;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_amount     DECIMAL(10,2) DEFAULT 0.01;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS currency         VARCHAR(3)    DEFAULT 'USD';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_method   VARCHAR(50)   DEFAULT 'credit_card';
```

**Logic mới trong run_simulation():**
- `product_id` lấy từ `SharedCatalog.pick_product()` → ID thực từ Kaggle CSV
- `unit_price = base_price × random.uniform(0.8, 1.2)` — giá thực tế có biến động ±20%
- `total_amount = unit_price × quantity`
- `payment_method` = random trong `['credit_card', 'paypal', 'bank_transfer', 'momo', 'vnpay']`
- `email` = `f"{uuid.uuid4().hex[:8]}@{fake.domain_name()}"` — UUID prefix tránh duplicate

**Fixes giải quyết:** 2.1, 2.2, 2.3, 2.5

---

### 6.4 Track 4 — Clickstream Enhancement: `clickstream_bot.py`

#### Fix 3.1 — SessionManager
```python
class SessionManager:
    """Duy trì mapping user_id → session_id với timeout 30 phút."""
    def get_session(self, user_id: int) -> str:
        now = time.time()
        if user_id in self._sessions:
            sid, last_time = self._sessions[user_id]
            if now - last_time < SESSION_TIMEOUT:
                self._sessions[user_id] = (sid, now)   # refresh
                return sid
        # Hết hạn → tạo session mới
        new_sid = f"sess-{uuid.uuid4().hex[:12]}"
        self._sessions[user_id] = (new_sid, now)
        return new_sid
```

#### Fix 3.2 — Realistic Distribution
```python
EVENT_TYPES   = ["scroll", "view_item", "click", "search", "add_to_cart", "checkout"]
EVENT_WEIGHTS = [30,       28,          20,      10,       8,             4]
```

#### Fix 3.3 — Event-specific Payload
```python
if event_type == "search":
    payload["search_query"] = query        # Không có item_id
    payload["result_count"] = random.randint(0, 200)
elif event_type == "checkout" and step == "confirmation":
    payload["pseudo_order_id"] = str(uuid.uuid4())  # Gold fuzzy join
```

#### Fix 3.4, 3.5 — SharedCatalog Integration
```python
catalog = SharedCatalog(pg_conn)
user_id    = random.randint(1, catalog.max_user_id())     # Real user IDs từ DB
product_id, base_price = catalog.pick_product()           # Real product IDs từ Kaggle
```

#### Fix 3.9 — Thêm trường thực tế
```python
payload["ip_address"]  = fake.ipv4_public()
payload["user_agent"]  = fake.user_agent()
payload["referrer"]    = random.choices(REFERRERS, weights=REFERRER_WEIGHTS, k=1)[0]
payload["page_url"]    = f"https://retailflow.example.com/..."
payload["duration_ms"] = random.randint(5000, 180000)
```

**Fixes giải quyết:** 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.8, 3.9, 3.10, 3.11, 3.12

---

## 7. Ma trận Giải quyết — Tổng kết

| Issue | File giải quyết | Trạng thái |
|-------|----------------|-----------|
| 1.1 Chỉ 2 tháng CSV | Silver Spark (planned) | 🕐 Chờ Silver layer |
| 1.2 Không có quantity | Silver Spark (planned) | 🕐 Chờ Silver layer |
| 1.3 category_code NULL | `seed_product_catalog.py` (category_fill) | ✅ |
| 1.4 event_type 3 loại | Silver taxonomy mapping (planned) | 🕐 Chờ Silver layer |
| 1.5 user_id mismatch CSV | Silver surrogate key (planned) | 🕐 Chờ Silver layer |
| 1.6 product_id bridge | `seed_product_catalog.py` | ✅ 206,876 products |
| 2.1 users schema đơn giản | `mock_backend.py` | ✅ email/phone/city |
| 2.2 orders không có product_id | `mock_backend.py` + `shared_catalog.py` | ✅ |
| 2.3 orders thiếu fields | `mock_backend.py` | ✅ quantity/payment/currency |
| 2.4 Không có products table | `seed_product_catalog.py` | ✅ |
| 2.5 user_id không sync | `shared_catalog.py` + `clickstream_bot.py` | ✅ |
| 2.6 Không có product catalog | `seed_product_catalog.py` | ✅ |
| 3.1 session_id broken | `clickstream_bot.py` — SessionManager | ✅ |
| 3.2 event distribution sai | `clickstream_bot.py` — weighted random | ✅ |
| 3.3 item_id gán bừa | `clickstream_bot.py` — event-specific payload | ✅ |
| 3.4 item_id format sai | `shared_catalog.py` + `clickstream_bot.py` | ✅ |
| 3.5 user_id pool 1-1000 | `shared_catalog.py` + `clickstream_bot.py` | ✅ |
| 3.6 Không có purchase event | `clickstream_bot.py` — pseudo_order_id | ✅ |
| 3.7 event_type naming | Silver taxonomy mapping (planned) | 🕐 Chờ Silver layer |
| 3.8 Config hardcoded | `.env` + `clickstream_bot.py` | ✅ |
| 3.9 Thiếu trường | `clickstream_bot.py` | ✅ ip/ua/referrer/url |
| 3.10 snappy → lz4 | `clickstream_bot.py` | ✅ |
| 3.11 return → sys.exit | `clickstream_bot.py` | ✅ |
| 3.12 Logging format | `clickstream_bot.py` | ✅ |

**Tổng: ✅ 18/22 đã giải quyết | 🕐 4/22 chờ Silver layer**

---

## 8. Files Đã Tạo / Cập nhật

| File | Thay đổi | Mô tả |
|------|----------|-------|
| `scripts/seeds/seed_product_catalog.py` | 🆕 Mới | Parse 13.67GB CSV → 206,876 products vào PostgreSQL |
| `scripts/simulation/shared_catalog.py` | 🆕 Mới | Cache products + user_ids, dùng chung 2 bots |
| `scripts/simulation/mock_backend.py` | ✏️ Cập nhật | Schema đầy đủ: email, phone, product_id, quantity, payment_method |
| `scripts/simulation/clickstream_bot.py` | ✏️ Cập nhật | Rewrite: SessionManager, real IDs, realistic payload |
| `requirements.txt` | ✏️ Cập nhật | Thêm `pandas>=2.0.0` |
| `.env` | ✏️ Cập nhật | Thêm `KAFKA_BROKER`, `CLICKSTREAM_TOPIC`, `CLICKSTREAM_USER_POOL_SIZE`, `SEED_PRODUCT_LIMIT` |

---

## 9. Kiến trúc Liên thông Sau Fix

```
PostgreSQL (Source of Truth)
┌─────────┐  ┌────────────────────────────┐  ┌──────────┐
│  users  │  │          orders            │  │ products │ ← Seed từ CSV
│ id      │  │ user_id → users.id         │  │ 206,876  │
│ name    │  │ product_id → products      │  │ products │
│ email   │  │ quantity, unit_price       │  │          │
│ phone   │  │ payment_method, currency   │  └────┬─────┘
│ city    │  └────────────────────────────┘       │
└────┬────┘           │ (Debezium CDC)             │ SharedCatalog
     │                ▼                            │ (auto-refresh 5 phút)
     │         Kafka Topics                        │
     │   retailflow_server.*                  ┌────┴────────────────┐
     │                │                       │   clickstream_bot   │
     │                │                       │   user_id: real DB  │
     │                │                       │   product_id: real  │
     │                │                       └────────┬────────────┘
     │                │                               │ Kafka
     │                │                       retailflow_clickstream
     │                │                               │
     └────────────────┴───────────────────────────────┘
                      ▼
              MinIO bronze-zone/
        ┌──────────────────────────────────┐
        │ cdc_postgres/                    │
        │ ├── .../public.orders/           │
        │ └── .../public.users/            │
        │ clickstream/                     │
        │ └── retailflow_clickstream/      │
        │ batch/historical/               │
        │ ├── 2019-09.csv                 │
        │ └── 2019-10.csv                 │
        └──────────────────────────────────┘
```

---

## 10. Công việc Còn lại — Bronze Layer

| Task | Mô tả | Ưu tiên |
|------|-------|---------|
| Clickstream S3 Sink | Thêm Kafka Connect S3 Sink cho topic `retailflow_clickstream` | 🔴 Cao |
| Batch Upload | Chạy `batch_ingestion_job.py` để upload 2 CSV lên MinIO | 🔴 Cao |
| Re-register Debezium Connector | Đăng ký lại CDC connector sau Docker restart | 🟡 Trung bình |
| Re-register S3 CDC Sink | Đăng ký lại `ecommerce-s3-sink` sau Docker restart | 🟡 Trung bình |
