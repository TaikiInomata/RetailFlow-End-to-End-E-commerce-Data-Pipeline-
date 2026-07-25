{#
  gold_product_engagement.sql
  ───────────────────────────
  Mart: Funnel Analytics — Product-level Engagement
  Câu hỏi trả lời:
    - Sản phẩm nào được xem nhiều nhất nhưng ít được add to cart?
      (Gap lớn → vấn đề về giá / mô tả / hình ảnh)
    - Search query nào phổ biến nhất → hỗ trợ SEO/merchandising
    - Thời gian xem trung bình mỗi sản phẩm (engagement quality)
    - Tỷ lệ chuyển đổi view → cart theo sản phẩm
#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['event_date', 'item_id']
) }}

WITH product_events AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
    WHERE item_id IS NOT NULL      -- Chỉ lấy events có liên quan đến sản phẩm cụ thể
    {% if is_incremental() %}
      AND event_date >= DATE_ADD('day', -2, CURRENT_DATE)
    {% endif %}
)

SELECT
    e.event_date,
    e.item_id                                                                   AS product_id,
    p.product_name,
    p.category,
    p.brand,

    -- ── View metrics ─────────────────────────────────────
    COUNT(DISTINCT CASE WHEN e.event_type = 'view_item' THEN e.session_id END) AS view_sessions,
    COUNT(DISTINCT CASE WHEN e.event_type = 'view_item' THEN e.user_id END)    AS unique_viewers,

    -- ── Cart metrics ─────────────────────────────────────
    COUNT(DISTINCT CASE WHEN e.event_type = 'add_to_cart' THEN e.user_id END)  AS users_added_to_cart,

    -- ── View → Cart conversion per product ───────────────
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_type = 'add_to_cart' THEN e.user_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type = 'view_item' THEN e.user_id END), 0),
        2
    )                                                                           AS view_to_cart_rate,

    -- ── Engagement quality ───────────────────────────────
    ROUND(AVG(CASE WHEN e.event_type = 'view_item' THEN e.duration_ms END) / 1000.0, 2)
                                                                                AS avg_view_duration_sec,
    ROUND(AVG(CASE WHEN e.event_type = 'view_item' THEN e.scroll_depth_pct END), 2)
                                                                                AS avg_scroll_depth_pct,

    -- ── Search signals ───────────────────────────────────
    COUNT(CASE WHEN e.event_type = 'search' THEN 1 END)                        AS search_impressions,

    -- Metadata
    CURRENT_TIMESTAMP                                                           AS dbt_updated_at

FROM product_events e
LEFT JOIN {{ ref('stg_products') }} p
    ON e.item_id = CAST(p.product_id AS VARCHAR)
GROUP BY 1, 2, 3, 4, 5
