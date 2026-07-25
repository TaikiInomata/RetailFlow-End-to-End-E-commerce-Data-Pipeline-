{#
  gold_product_performance.sql
  ────────────────────────────
  Mart: Sales Analytics — Product Performance (Weekly)
  Câu hỏi trả lời:
    - Sản phẩm nào bán chạy nhất tuần này theo doanh thu / số lượng?
    - Sản phẩm nào thuộc Top 10% trong tuần?
    - Xu hướng doanh thu từng sản phẩm theo tuần?

  BUG FIX: Window Function PHẢI được tách ra khỏi GROUP BY
  ─────────────────────────────────────────────────────────
  Lỗi cũ: RANK() OVER(...) trong cùng SELECT với GROUP BY → Trino SQL Error
  Fix:    2 tầng CTE — tầng 1 aggregate, tầng 2 apply window function

  Materialization: incremental (delete+insert)
    - Buffer 2 tuần để bắt đơn hàng muộn (PENDING → COMPLETED sau vài ngày)
#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['week_start', 'product_id']
) }}

-- ── Tầng 1: Aggregate theo tuần ──────────────────────────
WITH weekly_stats AS (
    SELECT
        DATE_TRUNC('week', o.order_date)        AS week_start,
        o.product_id,
        p.category,
        p.brand,
        p.product_name,
        SUM(o.quantity)                         AS units_sold,
        COUNT(DISTINCT o.order_id)              AS order_count,
        -- Quy đổi về USD
        SUM(o.total_amount / COALESCE(r.exchange_rate, 1.0))    AS revenue_usd,
        AVG(o.total_amount / COALESCE(r.exchange_rate, 1.0))    AS avg_order_value_usd
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_products') }} p
        ON o.product_id = p.product_id
    LEFT JOIN {{ ref('stg_exchange_rates') }} r
        ON o.order_date     = r.exchange_date
        AND o.currency      = r.target_currency
        AND r.base_currency = 'USD'
    WHERE o.status = 'COMPLETED'
    {% if is_incremental() %}
      -- Buffer 2 tuần: đảm bảo đơn hàng cuối tuần trước vẫn được tính đúng
      AND DATE_TRUNC('week', o.order_date) >= DATE_ADD('week', -2, DATE_TRUNC('week', CURRENT_DATE))
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5
),

-- ── Tầng 2: Window Function (không có GROUP BY ở đây) ────
ranked AS (
    SELECT
        *,
        -- Rank doanh thu trong tuần (1 = bán chạy nhất)
        RANK() OVER (
            PARTITION BY week_start
            ORDER BY revenue_usd DESC
        )                                       AS revenue_rank_in_week,
        -- Rank số lượng trong tuần
        RANK() OVER (
            PARTITION BY week_start
            ORDER BY units_sold DESC
        )                                       AS units_rank_in_week,
        -- Tổng số sản phẩm trong tuần (để tính top 10%)
        COUNT(*) OVER (PARTITION BY week_start) AS total_products_in_week
    FROM weekly_stats
)

SELECT
    week_start,
    product_id,
    product_name,
    category,
    brand,
    units_sold,
    order_count,
    ROUND(revenue_usd, 2)                       AS revenue_usd,
    ROUND(avg_order_value_usd, 2)               AS avg_order_value_usd,
    revenue_rank_in_week,
    units_rank_in_week,
    total_products_in_week,
    -- Flag: sản phẩm nằm trong Top 10% doanh thu tuần này
    revenue_rank_in_week <= CEIL(total_products_in_week * 0.10)  AS is_top_10_pct_revenue,
    -- Flag: sản phẩm nằm trong Top 3 doanh thu tuần này
    revenue_rank_in_week <= 3                   AS is_top_3_revenue,
    CURRENT_TIMESTAMP                           AS dbt_updated_at
FROM ranked
