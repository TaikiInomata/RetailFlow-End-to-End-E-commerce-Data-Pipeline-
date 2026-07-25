# 🏆 Gold Layer — Master Implementation Plan (v3 — Bug Fixed)
> **Domain:** E-commerce | **Stack:** Trino + dbt-trino + Airflow + Delta Lake on MinIO  
> **Changelog v3:** Fix Window Function bug · Fix Fan-out JOIN · Add Staging Layer · Add SLA · Add PENDING metric

---

## 1. Tổng quan kiến trúc hệ thống

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│                               RETAIL FLOW LAKEHOUSE                                │
├──────────────────┬──────────────────────────┬──────────────────────────────────────┤
│   INGESTION      │   TRANSFORMATION          │   SERVING                            │
│                  │                           │                                      │
│  Kafka ───────► Silver (PySpark)  ─────────► Gold (Trino + dbt)  ──────────► BI   │
│  CDC Debezium    │  silver/orders             │  gold_daily_sales_summary            │
│  Exchange API    │  silver/products           │  gold_product_performance            │
│  Clickstream     │  silver/users             │  gold_customer_snapshot              │
│                  │  silver/exchange_rates     │  gold_daily_funnel                   │
│                  │  silver/clickstream        │  gold_product_engagement             │
├──────────────────┴──────────────────────────┴──────────────────────────────────────┤
│  STORAGE: MinIO (Delta Lake)  │  ORCHESTRATION: Airflow  │  COMPUTE: Trino          │
└────────────────────────────────────────────────────────────────────────────────────┘
```

### Lý do chọn Trino + dbt (không phải PySpark thuần)

| Tiêu chí | PySpark cho Gold | Trino + dbt ✅ |
|:---|:---|:---|
| Tốc độ truy vấn Interactive | Chậm (spill to disk) | Nhanh (in-memory MPP) |
| Business Logic (SQL) | Python dài dòng | SQL ngắn gọn, dễ đọc |
| Data Testing tích hợp | Phải tự viết | dbt test: unique, not_null, etc. |
| Documentation tự động | Không có | `dbt docs generate` |
| Analytics Engineer tự làm | Phải nhờ Data Engineer | Tự viết SQL được |
| Industry Standard 2024 | Legacy | Modern Data Stack ✅ |

---

## 2. Các thay đổi thiết kế đã áp dụng vào Simulation

| # | Quyết định | Code đã thay đổi |
|:--|:---|:---|
| Q1 | **Đa tiền tệ** | `mock_backend.py`: sinh orders 5 tiền tệ — USD (45%), VND (35%), SGD (10%), EUR (6%), JPY (4%). Gold **bắt buộc JOIN** `exchange_rates` để quy USD. |
| Q2 | **SCD Type 2** | `gold_customer_snapshot` lưu lịch sử theo ngày, cho phép track tốc độ tăng trưởng CLV, thay đổi segment. |
| Q3 | **Real Order FK** | `clickstream_bot.py` gắn `real_order_id` (INTEGER FK thật sang `orders.id`). Mart 3 giờ JOIN chính xác Clickstream ↔ Orders. |

---

## 3. Hạ tầng — Docker Compose

### 3.1 Service Trino (mới)

```yaml
# docker/docker-compose.yml
  trino:
    image: trinodb/trino:435
    container_name: retailflow_trino
    restart: unless-stopped
    ports:
      - "8082:8080"           # Trino Web UI
    volumes:
      - ../docker/trino/etc:/etc/trino
      - retailflow_trino_metastore:/var/trino/metastore
    env_file:
      - ../.env
      - ../.env.docker
    networks:
      - retailflow_network
    depends_on:
      minio:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/v1/info"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
```

### 3.2 Thêm named volume cho Trino metastore

```yaml
volumes:
  retailflow_trino_metastore:
    driver: local
```

### 3.3 Airflow — Bổ sung dbt-trino

```yaml
# Trong x-airflow-common → environment
_PIP_ADDITIONAL_REQUIREMENTS: requests boto3 dbt-trino
# Bind mount thư mục dbt vào Airflow container
volumes:
  - ../dbt:/opt/airflow/dbt   # để DAG gọi dbt run
```

---

## 4. Cấu hình Trino

### 4.1 `docker/trino/etc/catalog/minio.properties`

```properties
# Delta Lake connector — đọc trực tiếp file Delta trên MinIO
connector.name=delta_lake

# File-based Metastore: không cần Hive HMS container — lightweight!
hive.metastore=file
hive.metastore.catalog.dir=/var/trino/metastore

# MinIO S3-compatible config
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.aws-access-key=${ENV:MINIO_ROOT_USER}
s3.aws-secret-key=${ENV:MINIO_ROOT_PASSWORD}
s3.region=us-east-1
s3.path-style-access=true

# Delta Lake
delta.enable-non-concurrent-writes=true
```

> [!WARNING]
> **File-based metastore không hỗ trợ concurrent writes.** Nếu 2 dbt models cùng ghi vào schema, có thể bị conflict. Bắt buộc giữ `threads: 1` trong `profiles.yml` cho đến khi nâng cấp lên Hive/Glue metastore.

### 4.2 `docker/trino/etc/config.properties`

```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=2GB
query.max-memory-per-node=1GB
discovery.uri=http://localhost:8080
```

### 4.3 Script init (chạy 1 lần) — Tạo Schema trong Trino

```python
# scripts/setup/init_trino_schemas.py
# Kết nối Trino qua trino-python-client và chạy:
# CREATE SCHEMA IF NOT EXISTS minio.silver WITH (location = 's3a://silver-zone/')
# CREATE SCHEMA IF NOT EXISTS minio.gold   WITH (location = 's3a://gold-zone/')
```

---

## 5. dbt Project Structure

> [!IMPORTANT]
> **Bắt buộc có tầng `staging/`** — đây là nguyên tắc cốt lõi của dbt. Staging làm nhiệm vụ: rename cột, cast type, lọc `is_deleted`. Nếu schema Silver thay đổi, chỉ cần sửa 1 file staging, không phải sửa tất cả mart.

```
dbt/
├── dbt_project.yml              ← Cấu hình project, materialization mặc định
├── profiles.yml                 ← Kết nối Trino (host, port, catalog, schema)
├── packages.yml                 ← dbt-utils
│
├── models/
│   ├── sources.yml              ← Khai báo nguồn raw (minio.silver.*)
│   │
│   ├── staging/                 ← TẦNG 1: Chuẩn hóa từ Silver (light transform)
│   │   ├── stg_orders.sql       — Rename, cast, lọc is_deleted=FALSE
│   │   ├── stg_products.sql     — Chọn cột cần thiết từ products
│   │   ├── stg_users.sql        — Rename, lọc is_deleted=FALSE
│   │   ├── stg_exchange_rates.sql — Điền NULL rate bằng LAST_VALUE IGNORE NULLS
│   │   └── stg_clickstream.sql  — Parse referrer → referrer_category
│   │
│   ├── marts/                   ← TẦNG 2: Business Logic (JOIN staging models)
│   │   ├── sales_analytics/
│   │   │   ├── schema.yml
│   │   │   ├── gold_daily_sales_summary.sql
│   │   │   └── gold_product_performance.sql
│   │   ├── customer_360/
│   │   │   ├── schema.yml
│   │   │   └── gold_customer_snapshot.sql
│   │   └── funnel_analytics/
│   │       ├── schema.yml
│   │       ├── gold_daily_funnel.sql
│   │       └── gold_product_engagement.sql
│   │
└── macros/
    └── rfm_segment.sql          ← Macro RFM tái dùng trong nhiều model
```

### `dbt/profiles.yml`

```yaml
retailflow_dbt:
  target: dev
  outputs:
    dev:
      type: trino
      method: none
      user: airflow
      host: trino             # Docker service name
      port: 8080
      catalog: minio
      schema: gold
      threads: 1              # BẮT BUỘC = 1 do File-based metastore không hỗ trợ concurrent writes
```

---

## 6. Staging Layer (models/staging/)

### `stg_orders.sql`
```sql
{{ config(materialized='view') }}

SELECT
    id                              AS order_id,
    user_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    currency,
    payment_method,
    status,
    CAST(created_at AS TIMESTAMP)   AS created_at,
    CAST(updated_at AS TIMESTAMP)   AS updated_at,
    DATE(created_at)                AS order_date
FROM {{ source('silver', 'orders') }}
WHERE is_deleted = FALSE
```

### `stg_exchange_rates.sql`
```sql
-- Điền NULL exchange_rate bằng tỷ giá ngày gần nhất (LAST_VALUE IGNORE NULLS)
-- Xử lý trường hợp API tỷ giá bị lỗi 1 ngày
{{ config(materialized='view') }}

SELECT
    exchange_date,
    base_currency,
    target_currency,
    LAST_VALUE(exchange_rate IGNORE NULLS) OVER (
        PARTITION BY base_currency, target_currency
        ORDER BY exchange_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS exchange_rate
FROM {{ source('silver', 'exchange_rates') }}
```

### `stg_clickstream.sql`
```sql
-- Parse referrer URL → referrer_category (logic tập trung 1 chỗ, dùng lại nhiều mart)
{{ config(materialized='view') }}

SELECT
    event_id, user_id, session_id,
    event_type, device, step, real_order_id,
    item_id, duration_ms, scroll_depth_pct,
    search_query, result_count, click_target,
    quantity, cart_total, purchase_amount,
    CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
    DATE(event_timestamp)              AS event_date,
    CASE
        WHEN referrer LIKE '%google%' OR referrer LIKE '%bing%' THEN 'organic_search'
        WHEN referrer LIKE '%facebook%' OR referrer LIKE '%instagram%'
             OR referrer LIKE '%tiktok%'                        THEN 'social'
        WHEN referrer = 'email'                                 THEN 'email'
        WHEN referrer = 'direct' OR referrer IS NULL            THEN 'direct'
        ELSE 'other'
    END AS referrer_category
FROM {{ source('silver', 'clickstream') }}
```

---

## 7. Data Freshness SLA

| Mart | SLA Freshness | Lý do | Airflow Schedule |
|:---|:---|:---|:---|
| `gold_daily_sales_summary` | ≤ 1 giờ | CEO xem GMV buổi sáng, real-time | `@hourly` sau CDC done |
| `gold_product_performance` | ≤ 4 giờ | Weekly report, ít nhạy cảm hơn | `0 */4 * * *` |
| `gold_customer_snapshot` | 1 lần/ngày | Không cần realtime | `0 2 * * *` (2AM) |
| `gold_daily_funnel` | ≤ 30 phút | Product team cần nhanh nhất | Sau mỗi Clickstream batch |
| `gold_product_engagement` | ≤ 1 giờ | Cùng cadence với funnel | Sau `gold_daily_funnel` |

---

## 8. Thiết kế Data Mart Chi Tiết

### 8.1 Mart 1 — `gold_sales_analytics`

> **Câu hỏi trả lời:** *"GMV thực tế theo ngày? Sản phẩm nào bán chạy nhất tuần này? Tỷ lệ hủy đơn theo tiền tệ? Doanh thu tiềm năng (PENDING) là bao nhiêu?"*

#### `gold_daily_sales_summary.sql` (Fixed)
```sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['order_date', 'category', 'payment_method', 'currency_group']
) }}

WITH orders_enriched AS (
    SELECT
        o.order_date,
        p.category_fill     AS category,
        p.brand,
        o.payment_method,
        o.currency          AS currency_group,
        o.status,
        o.quantity,
        -- Quy đổi về USD qua staging đã điền NULL sẵn
        o.total_amount / COALESCE(r.exchange_rate, 1.0) AS revenue_usd
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id
    LEFT JOIN {{ ref('stg_exchange_rates') }} r
        ON o.order_date = r.exchange_date
        AND o.currency  = r.target_currency
        AND r.base_currency = 'USD'
    {% if is_incremental() %}
      WHERE o.order_date >= DATE_ADD('day', -3, CURRENT_DATE)
    {% endif %}
)

SELECT
    order_date, category, brand, payment_method, currency_group,
    -- COMPLETED metrics (Revenue thật)
    COUNT_IF(status = 'COMPLETED')                                           AS order_count,
    SUM(CASE WHEN status = 'COMPLETED' THEN revenue_usd ELSE 0 END)          AS total_revenue_usd,
    AVG(CASE WHEN status = 'COMPLETED' THEN revenue_usd END)                 AS avg_order_value_usd,
    SUM(CASE WHEN status = 'COMPLETED' THEN quantity ELSE 0 END)             AS total_quantity_sold,
    -- CANCELLED metrics (Chất lượng vận hành)
    COUNT_IF(status = 'CANCELLED')                                           AS cancelled_count,
    ROUND(
        COUNT_IF(status = 'CANCELLED') * 100.0 /
        NULLIF(COUNT_IF(status IN ('COMPLETED', 'CANCELLED')), 0), 2
    )                                                                        AS cancellation_rate_pct,
    -- PENDING metrics (Dự báo doanh thu ngày hôm nay — thêm mới)
    COUNT_IF(status = 'PENDING')                                             AS pending_count,
    SUM(CASE WHEN status = 'PENDING' THEN revenue_usd ELSE 0 END)            AS pending_revenue_usd
FROM orders_enriched
GROUP BY 1, 2, 3, 4, 5
```

#### `gold_product_performance.sql` (Bug Fixed — Window Function tách thành 2 CTE)
```sql
-- ✅ FIX: Tách Window Function ra khỏi GROUP BY bằng 2 tầng CTE
-- Lỗi cũ: RANK() được đặt trong cùng SELECT với GROUP BY → Trino sẽ ném SQL Error
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['week_start', 'product_id']
) }}

-- Tầng 1: Aggregate
WITH weekly_stats AS (
    SELECT
        DATE_TRUNC('week', o.order_date)  AS week_start,
        o.product_id,
        p.category_fill                   AS category,
        p.brand,
        SUM(o.quantity)                   AS units_sold,
        SUM(o.total_amount / COALESCE(r.exchange_rate, 1.0)) AS revenue_usd
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id
    LEFT JOIN {{ ref('stg_exchange_rates') }} r
        ON o.order_date = r.exchange_date AND o.currency = r.target_currency
    WHERE o.status = 'COMPLETED'
    {% if is_incremental() %}
      -- Buffer 2 tuần để đảm bảo đơn hàng muộn vẫn được tính đúng
      AND DATE_TRUNC('week', o.order_date) >= DATE_ADD('week', -2, DATE_TRUNC('week', CURRENT_DATE))
    {% endif %}
    GROUP BY 1, 2, 3, 4
),

-- Tầng 2: Áp dụng Window Function (không có GROUP BY ở đây)
ranked AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY week_start ORDER BY revenue_usd DESC) AS revenue_rank_in_week,
        COUNT(*) OVER (PARTITION BY week_start)                         AS total_products_in_week
    FROM weekly_stats
)

SELECT
    week_start, product_id, category, brand,
    units_sold, revenue_usd,
    revenue_rank_in_week,
    -- Top 10% trong tuần
    revenue_rank_in_week <= CEIL(total_products_in_week * 0.10) AS is_top_10_pct
FROM ranked
```

---

### 8.2 Mart 2 — `gold_customer_360` (SCD Type 2)

> **Câu hỏi trả lời:** *"Phân khúc khách hàng hôm nay vs hôm qua? Khách VIP nào rớt 'At Risk'? CLV cohort tháng 7 tăng trưởng bao nhiêu %?"*

#### Tại sao SCD Type 2 là bắt buộc?

| Câu hỏi | Type 1 (Ghi đè) | Type 2 (Lịch sử) ✅ |
|:---|:---|:---|
| Hôm nay có bao nhiêu Champions? | ✅ | ✅ |
| Tháng trước có bao nhiêu Champions? | ❌ | ✅ |
| User X chuyển sang At Risk ngày nào? | ❌ | ✅ |
| CLV cohort tháng 7 tăng bao nhiêu % sau 30 ngày? | ❌ | ✅ |

#### Schema `gold_customer_snapshot`
```
snapshot_date           DATE      — Partition key: ngày chụp ảnh
user_id                 BIGINT
city, country           STRING
registration_date       DATE

-- RFM
days_since_last_order   INT       — R
total_orders            BIGINT    — F
first_order_date, last_order_date DATE
customer_lifespan_days  INT

-- Monetary
total_revenue_usd       DOUBLE    — Đã quy đổi
avg_order_value_usd     DOUBLE
preferred_currency      STRING    — MODE(currency)
preferred_payment       STRING    — MODE(payment_method)

-- Segment
customer_segment        STRING    — Champions|Loyal|Promising|At Risk|New|Lost|Never Bought
is_high_value           BOOLEAN   — total_revenue_usd > 500
```

#### `gold_customer_snapshot.sql`
```sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    partition_by = {'field': 'snapshot_date', 'data_type': 'date'}
) }}

{% if is_incremental() %}
  -- Idempotent guard: bỏ qua nếu snapshot ngày hôm nay đã có
  {% if execute %}
    {% set already_exists %}
      SELECT COUNT(1) FROM {{ this }} WHERE snapshot_date = CURRENT_DATE
    {% endset %}
    {% if run_query(already_exists).columns[0][0] > 0 %}
      {{ return(none) }}
    {% endif %}
  {% endif %}
{% endif %}

WITH order_stats AS (
    SELECT
        o.user_id,
        COUNT(*)                                              AS total_orders,
        MIN(o.order_date)                                     AS first_order_date,
        MAX(o.order_date)                                     AS last_order_date,
        DATE_DIFF('day', MAX(o.order_date), CURRENT_DATE)     AS days_since_last_order,
        SUM(o.total_amount / COALESCE(r.exchange_rate, 1.0))  AS total_revenue_usd,
        MAX_BY(o.currency, o.created_at)                      AS preferred_currency,
        MAX_BY(o.payment_method, o.created_at)                AS preferred_payment
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_exchange_rates') }} r
        ON o.order_date = r.exchange_date AND o.currency = r.target_currency
    WHERE o.status = 'COMPLETED'
    GROUP BY 1
)

SELECT
    CURRENT_DATE                                              AS snapshot_date,
    u.user_id, u.city, u.country, u.registration_date,
    s.days_since_last_order,
    COALESCE(s.total_orders, 0)                               AS total_orders,
    s.first_order_date, s.last_order_date,
    DATE_DIFF('day', s.first_order_date, s.last_order_date)   AS customer_lifespan_days,
    COALESCE(s.total_revenue_usd, 0)                          AS total_revenue_usd,
    COALESCE(s.total_revenue_usd / NULLIF(s.total_orders, 0), 0) AS avg_order_value_usd,
    s.preferred_currency, s.preferred_payment,
    -- Macro RFM segment tái dùng được
    {{ rfm_segment('s.days_since_last_order', 's.total_orders', 's.total_revenue_usd') }}
        AS customer_segment,
    COALESCE(s.total_revenue_usd, 0) > 500                    AS is_high_value
FROM {{ ref('stg_users') }} u
LEFT JOIN order_stats s ON u.user_id = s.user_id
```

#### `macros/rfm_segment.sql`
```sql
-- Macro tái dùng RFM logic — thay đổi 1 lần, áp dụng toàn bộ project
{% macro rfm_segment(recency_col, frequency_col, monetary_col) %}
CASE
    WHEN {{ frequency_col }} IS NULL OR {{ frequency_col }} = 0   THEN 'Never Bought'
    WHEN {{ recency_col }} > 180                                   THEN 'Lost'
    WHEN {{ recency_col }} > 60 AND {{ monetary_col }} > 100       THEN 'At Risk'
    WHEN {{ recency_col }} <= 30
         AND {{ frequency_col }} >= 5
         AND {{ monetary_col }} >= 200                             THEN 'Champions'
    WHEN {{ recency_col }} <= 60 AND {{ frequency_col }} >= 3      THEN 'Loyal'
    WHEN {{ recency_col }} <= 30 AND {{ frequency_col }} <= 2      THEN 'Promising'
    ELSE 'New'
END
{% endmacro %}
```

---

### 8.3 Mart 3 — `gold_funnel_analytics`

> **Câu hỏi trả lời:** *"Tỷ lệ CVR từng bước? Kênh nào hiệu quả? Session nào confirm nhưng đơn lại bị CANCEL?"*

#### `gold_daily_funnel.sql` (Bug Fixed — Fan-out JOIN)
```sql
-- ✅ FIX: Lỗi cũ JOIN confirmed c ON e.user_id + e.event_date gây fan-out
-- (User có 3 session trong ngày → bị nhân 3 lần → CVR sai)
-- Fix: Tách confirmed thành subquery độc lập ở session-level, không JOIN vào events
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['event_date', 'device', 'referrer_category']
) }}

WITH events AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
    {% if is_incremental() %}
      WHERE event_date >= DATE_ADD('day', -2, CURRENT_DATE)
    {% endif %}
),

-- Tập các user_id đã thực sự mua hàng thành công (status=COMPLETED)
-- JOIN ở đây để tránh fan-out: mỗi user_id chỉ xuất hiện 1 lần
truly_completed_users AS (
    SELECT DISTINCT
        c.event_date,
        c.user_id
    FROM events c
    INNER JOIN {{ ref('stg_orders') }} o ON c.real_order_id = o.order_id
    WHERE c.step = 'confirmation'
      AND c.real_order_id IS NOT NULL
      AND o.status = 'COMPLETED'
)

SELECT
    e.event_date,
    e.device,
    e.referrer_category,
    COUNT(DISTINCT e.session_id)                                                          AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_type = 'view_item'   THEN e.user_id END)            AS users_viewed_product,
    COUNT(DISTINCT CASE WHEN e.event_type = 'add_to_cart' THEN e.user_id END)            AS users_added_to_cart,
    COUNT(DISTINCT CASE WHEN e.event_type = 'checkout'    THEN e.user_id END)            AS users_initiated_checkout,
    COUNT(DISTINCT CASE WHEN e.step = 'confirmation'      THEN e.user_id END)            AS users_confirmed_checkout,
    -- ✅ FIX: Dùng EXISTS-style lookup thay vì JOIN trực tiếp vào events
    COUNT(DISTINCT CASE WHEN t.user_id IS NOT NULL THEN e.user_id END)                   AS users_truly_completed,
    -- Conversion Rates
    ROUND(COUNT(DISTINCT CASE WHEN e.event_type='add_to_cart' THEN e.user_id END) * 100.0
          / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type='view_item' THEN e.user_id END), 0), 2)
                                                                                          AS view_to_cart_rate,
    ROUND(COUNT(DISTINCT CASE WHEN t.user_id IS NOT NULL THEN e.user_id END) * 100.0
          / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type='view_item' THEN e.user_id END), 0), 2)
                                                                                          AS true_purchase_rate,
    -- Engagement
    AVG(e.duration_ms) / 1000.0                                                           AS avg_session_duration_sec,
    AVG(e.scroll_depth_pct)                                                               AS avg_scroll_depth_pct,
    -- Bounce Rate: session chỉ có đúng 1 event
    ROUND(
        COUNT(DISTINCT CASE WHEN s.event_count = 1 THEN e.session_id END) * 100.0
        / NULLIF(COUNT(DISTINCT e.session_id), 0), 2
    )                                                                                     AS bounce_rate_pct
FROM events e
LEFT JOIN truly_completed_users t
    ON e.user_id = t.user_id AND e.event_date = t.event_date
LEFT JOIN (
    SELECT session_id, COUNT(*) AS event_count FROM events GROUP BY 1
) s ON e.session_id = s.session_id
GROUP BY 1, 2, 3
```

---

## 9. Data Tests (dbt schema.yml)

```yaml
# models/marts/sales_analytics/schema.yml
models:
  - name: gold_daily_sales_summary
    description: "Doanh thu hàng ngày theo danh mục và phương thức thanh toán, đã quy đổi USD."
    columns:
      - name: order_date
        tests: [not_null]
      - name: total_revenue_usd
        tests:
          - not_null
          - dbt_utils.accepted_range: {min_value: 0}
      - name: cancellation_rate_pct
        tests:
          - dbt_utils.accepted_range: {min_value: 0, max_value: 100}
      - name: pending_revenue_usd
        tests:
          - dbt_utils.accepted_range: {min_value: 0}

  - name: gold_customer_snapshot
    description: "Snapshot phân khúc khách hàng theo ngày (SCD Type 2)."
    columns:
      - name: user_id
        tests: [not_null]
      - name: snapshot_date
        tests: [not_null]
      - name: customer_segment
        tests:
          - accepted_values:
              values: ['Champions', 'Loyal', 'Promising', 'At Risk', 'New', 'Lost', 'Never Bought']
      - name: total_revenue_usd
        tests:
          - dbt_utils.accepted_range: {min_value: 0}

  - name: gold_daily_funnel
    columns:
      - name: true_purchase_rate
        tests:
          - dbt_utils.accepted_range: {min_value: 0, max_value: 100}
      - name: bounce_rate_pct
        tests:
          - dbt_utils.accepted_range: {min_value: 0, max_value: 100}
```

---

## 10. Airflow Orchestration

### DAG `gold_dbt_pipeline.py`

```python
# dags/gold_dbt_pipeline.py
BASE = "dbt --no-use-colors"
PROJ = "--profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt"

# Task Graph:
# [wait_for_cdc]     → [dbt_run_staging]  (stg_* views — nhanh)
#                    → [dbt_run_sales]    (--select marts/sales_analytics)
#                    → [dbt_run_customer] (--select marts/customer_360)   ← chỉ 2AM
# [wait_for_stream]  → [dbt_run_funnel]  (--select marts/funnel_analytics)
# [dbt_run_sales, dbt_run_funnel] → [dbt_test_all]
```

### Luồng phụ thuộc đầy đủ

```
[daily_exchange_rate_pipeline]  ──┐
[frequent_cdc_pipeline]         ──┤──► [gold_dbt_pipeline → sales + customer]
                                  │
[frequent_clickstream_pipeline] ──┘──► [gold_dbt_pipeline → funnel]
                                          └── [dbt_test_all] ← Chạy sau mỗi mart
```

---

## 11. Kế hoạch triển khai (Phased Roadmap)

| Phase | Nội dung | Files cần tạo/sửa | Ước tính |
|:--|:---|:---|:---|
| **Phase 1** | Hạ tầng Trino | `docker-compose.yml`, `docker/trino/etc/` | 1 buổi |
| **Phase 2** | Init schema + dbt debug | `init_trino_schemas.py`, `dbt_project.yml`, `profiles.yml` | 1 buổi |
| **Phase 3** | Staging layer | 5 file `stg_*.sql` + `sources.yml` | 1 buổi |
| **Phase 4** | Mart 1 — Sales | `gold_daily_sales_summary.sql`, `gold_product_performance.sql` + tests | 1 buổi |
| **Phase 5** | Mart 2 — Customer | `gold_customer_snapshot.sql`, `rfm_segment macro` + tests | 1 buổi |
| **Phase 6** | Mart 3 — Funnel | `gold_daily_funnel.sql`, `gold_product_engagement.sql` + tests | 1 buổi |
| **Phase 7** | Airflow DAG + E2E test | `dags/gold_dbt_pipeline.py` | 1 buổi |

> [!NOTE]
> **Thứ tự quan trọng:** Phase 3 (Staging) PHẢI hoàn thành trước Phase 4-6. Staging là nền tảng, các Mart `{{ ref('stg_*') }}` sẽ fail nếu staging chưa tồn tại.
