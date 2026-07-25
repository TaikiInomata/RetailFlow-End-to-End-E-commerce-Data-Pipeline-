{#
  gold_customer_snapshot.sql
  ──────────────────────────
  Mart: Customer 360 — SCD Type 2 (daily snapshot)
  Câu hỏi trả lời:
    - Phân khúc khách hàng hôm nay vs hôm qua thay đổi như thế nào?
    - Khách VIP nào đang rớt xuống 'At Risk'? (dùng cho re-engagement campaign)
    - CLV cohort tháng 7 tăng trưởng bao nhiêu % sau 30 ngày?
    - Tỷ lệ khách hàng active / dormant / churned theo thời gian?

  Tại sao SCD Type 2 (daily append):
    - Ghi đè (SCD Type 1) mất toàn bộ lịch sử segment → không thể phân tích trend
    - Append theo ngày → có thể query "Champions ngày hôm qua" dễ dàng

  Idempotent guard:
    - Kiểm tra snapshot ngày hôm nay đã tồn tại chưa trước khi append
    - Tránh duplicate khi DAG retry hoặc backfill

  Lưu ý về partition_by:
    - dbt-trino không hỗ trợ partition_by config như BigQuery
    - Partition trong Delta Lake được quản lý ở tầng Delta/Trino, không phải dbt config
    - Nếu cần partition, dùng EXECUTE CREATE TABLE ... WITH (partitioned_by = ARRAY['snapshot_date'])
      trong init_trino_schemas.py thay vì trong dbt config
#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append'
) }}

{% if is_incremental() %}
  {# Idempotent guard: bỏ qua nếu snapshot ngày hôm nay đã có #}
  {%- if execute -%}
    {%- set already_exists_query -%}
      SELECT COUNT(1) AS cnt FROM {{ this }} WHERE snapshot_date = CURRENT_DATE
    {%- endset -%}
    {%- set results = run_query(already_exists_query) -%}
    {%- if results.columns[0].values()[0] > 0 -%}
      {{ log("Snapshot ngày hôm nay đã tồn tại. Bỏ qua.", info=True) }}
      {{ return(none) }}
    {%- endif -%}
  {%- endif -%}
{% endif %}

WITH order_stats AS (
    SELECT
        o.user_id,
        COUNT(DISTINCT o.order_id)                                      AS total_orders,
        MIN(o.order_date)                                               AS first_order_date,
        MAX(o.order_date)                                               AS last_order_date,
        DATE_DIFF('day', MAX(o.order_date), CURRENT_DATE)               AS days_since_last_order,
        -- Tổng doanh thu quy đổi USD (stg_exchange_rates đã forward-fill)
        COALESCE(SUM(o.total_amount / COALESCE(r.exchange_rate, 1.0)), 0)   AS total_revenue_usd,
        -- Phương thức thanh toán ưa thích nhất gần đây
        MAX_BY(o.payment_method, o.created_at)                          AS preferred_payment,
        -- Tiền tệ ưa thích nhất gần đây
        MAX_BY(o.currency, o.created_at)                                AS preferred_currency
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_exchange_rates') }} r
        ON o.order_date     = r.exchange_date
        AND o.currency      = r.target_currency
        AND r.base_currency = 'USD'
    WHERE o.status = 'COMPLETED'
    GROUP BY 1
)

SELECT
    -- Snapshot metadata
    CURRENT_DATE                                                        AS snapshot_date,

    -- User demographics
    u.user_id,
    u.city,
    u.country,
    u.registration_date,

    -- RFM — Recency
    COALESCE(s.days_since_last_order, NULL)                             AS days_since_last_order,

    -- RFM — Frequency
    COALESCE(s.total_orders, 0)                                         AS total_orders,
    s.first_order_date,
    s.last_order_date,
    -- Thời gian là khách hàng (days)
    CASE
        WHEN s.first_order_date IS NOT NULL AND s.last_order_date IS NOT NULL
        THEN DATE_DIFF('day', s.first_order_date, s.last_order_date)
        ELSE NULL
    END                                                                 AS customer_lifespan_days,

    -- RFM — Monetary
    COALESCE(s.total_revenue_usd, 0)                                    AS total_revenue_usd,
    COALESCE(
        s.total_revenue_usd / NULLIF(s.total_orders, 0),
        0
    )                                                                   AS avg_order_value_usd,
    s.preferred_currency,
    s.preferred_payment,

    -- Phân khúc khách hàng (dùng macro rfm_segment tái dùng được)
    {{ rfm_segment('s.days_since_last_order', 's.total_orders', 's.total_revenue_usd') }}
                                                                        AS customer_segment,

    -- Flag VIP (tổng chi tiêu > $500)
    COALESCE(s.total_revenue_usd, 0) > 500                              AS is_high_value,

    -- Metadata
    CURRENT_TIMESTAMP                                                   AS dbt_updated_at

FROM {{ ref('stg_users') }} u
LEFT JOIN order_stats s ON u.user_id = s.user_id
