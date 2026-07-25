{#
  gold_daily_sales_summary.sql
  ────────────────────────────
  Mart: Sales Analytics
  Câu hỏi trả lời:
    - GMV thực tế theo ngày là bao nhiêu?
    - Tỷ lệ hủy đơn theo danh mục/phương thức thanh toán?
    - Doanh thu tiềm năng (đơn PENDING) ngày hôm nay là bao nhiêu?
    - AOV (Average Order Value) đang trending như thế nào?

  Materialization: incremental (delete+insert)
    - Lý do: Đơn hàng cũ có thể được update status (PENDING → COMPLETED)
      trong vòng 3 ngày → cần reprocess 3 ngày gần nhất
    - unique_key: (order_date, category, brand, payment_method, currency_group)
#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['order_date', 'category', 'brand', 'payment_method', 'currency_group']
) }}

WITH orders_enriched AS (
    SELECT
        o.order_date,
        p.category                              AS category,
        p.brand,
        o.payment_method,
        o.currency                              AS currency_group,
        o.status,
        o.quantity,
        -- Quy đổi về USD (stg_exchange_rates đã forward-fill NULL sẵn)
        -- COALESCE(..., 1.0): fallback khi không tìm thấy tỷ giá (USD orders)
        o.total_amount / COALESCE(r.exchange_rate, 1.0) AS revenue_usd
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_products') }} p
        ON o.product_id = p.product_id
    LEFT JOIN {{ ref('stg_exchange_rates') }} r
        ON o.order_date    = r.exchange_date
        AND o.currency     = r.target_currency
        AND r.base_currency = 'USD'
    {% if is_incremental() %}
      -- Reprocess 3 ngày gần nhất để bắt được status updates (PENDING → COMPLETED)
      WHERE o.order_date >= DATE_ADD('day', -3, CURRENT_DATE)
    {% endif %}
)

SELECT
    order_date,
    category,
    brand,
    payment_method,
    currency_group,

    -- ── COMPLETED metrics (doanh thu thực) ───────────────────
    COUNT_IF(status = 'COMPLETED')                                              AS order_count,
    COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN revenue_usd END), 0)       AS total_revenue_usd,
    AVG(CASE WHEN status = 'COMPLETED' THEN revenue_usd END)                    AS avg_order_value_usd,
    COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN quantity END), 0)          AS total_quantity_sold,

    -- ── CANCELLED metrics (chất lượng vận hành) ──────────────
    COUNT_IF(status = 'CANCELLED')                                              AS cancelled_count,
    ROUND(
        COUNT_IF(status = 'CANCELLED') * 100.0
        / NULLIF(COUNT_IF(status IN ('COMPLETED', 'CANCELLED')), 0),
        2
    )                                                                           AS cancellation_rate_pct,

    -- ── PENDING metrics (dự báo doanh thu ngày hôm nay) ──────
    -- Quan trọng với CEO để ước tính GMV cuối ngày
    COUNT_IF(status = 'PENDING')                                                AS pending_count,
    COALESCE(SUM(CASE WHEN status = 'PENDING' THEN revenue_usd END), 0)         AS pending_revenue_usd,

    -- ── Metadata ──────────────────────────────────────────────
    CURRENT_TIMESTAMP                                                           AS dbt_updated_at

FROM orders_enriched
GROUP BY 1, 2, 3, 4, 5
