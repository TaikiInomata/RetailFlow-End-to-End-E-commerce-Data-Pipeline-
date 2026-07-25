{#
  gold_daily_funnel.sql
  ─────────────────────
  Mart: Funnel Analytics — Daily Conversion Funnel
  Câu hỏi trả lời:
    - CVR (Conversion Rate) từng bước trong funnel là bao nhiêu?
    - Kênh traffic nào (organic/social/email) hiệu quả nhất?
    - Tỷ lệ bounce rate theo device/kênh?
    - Có bao nhiêu user confirm order nhưng đơn thực tế CANCELLED?

  BUG FIX: Fan-out JOIN
  ─────────────────────
  Lỗi cũ: JOIN confirmed orders trực tiếp vào events table theo user_id + event_date
    → User có 3 session trong ngày → mỗi session đều JOIN → đếm nhân 3 lần → CVR sai
  Fix:    Tách truly_completed_users thành subquery riêng (DISTINCT user_id per day)
    → Mỗi user chỉ xuất hiện 1 lần → COUNT(DISTINCT ...) cho kết quả đúng
#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['event_date', 'device', 'referrer_category']
) }}

WITH events AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
    {% if is_incremental() %}
      -- Buffer 2 ngày: đơn hàng có thể update status sau khi event xảy ra
      WHERE event_date >= DATE_ADD('day', -2, CURRENT_DATE)
    {% endif %}
),

-- ── FIX: Tập users đã thực sự mua thành công ─────────────
-- DISTINCT: mỗi user chỉ xuất hiện 1 lần per ngày → tránh fan-out khi LEFT JOIN vào events
truly_completed_users AS (
    SELECT DISTINCT
        c.event_date,
        c.user_id
    FROM events c
    INNER JOIN {{ ref('stg_orders') }} o
        ON c.real_order_id = o.order_id
    WHERE c.step = 'confirmation'
      AND c.real_order_id IS NOT NULL
      AND o.status = 'COMPLETED'
),

-- ── Session size (dùng cho bounce rate) ──────────────────
session_sizes AS (
    SELECT
        session_id,
        COUNT(*) AS event_count
    FROM events
    GROUP BY 1
)

SELECT
    e.event_date,
    e.device,
    e.referrer_category,

    -- ── Funnel steps ─────────────────────────────────────
    COUNT(DISTINCT e.session_id)                                                AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_type = 'view_item'   THEN e.user_id END)  AS users_viewed_product,
    COUNT(DISTINCT CASE WHEN e.event_type = 'add_to_cart' THEN e.user_id END)  AS users_added_to_cart,
    COUNT(DISTINCT CASE WHEN e.event_type = 'checkout'    THEN e.user_id END)  AS users_initiated_checkout,
    COUNT(DISTINCT CASE WHEN e.step = 'confirmation'      THEN e.user_id END)  AS users_confirmed_checkout,

    -- ── FIX: Dùng LEFT JOIN với truly_completed_users (không JOIN vào events) ──
    COUNT(DISTINCT CASE WHEN t.user_id IS NOT NULL THEN e.user_id END)         AS users_truly_completed,

    -- ── Conversion Rates ─────────────────────────────────
    -- View → Cart
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_type = 'add_to_cart' THEN e.user_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type = 'view_item' THEN e.user_id END), 0),
        2
    )                                                                           AS view_to_cart_rate,

    -- Cart → Checkout
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_type = 'checkout' THEN e.user_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type = 'add_to_cart' THEN e.user_id END), 0),
        2
    )                                                                           AS cart_to_checkout_rate,

    -- View → True Purchase (end-to-end CVR — metric quan trọng nhất)
    ROUND(
        COUNT(DISTINCT CASE WHEN t.user_id IS NOT NULL THEN e.user_id END) * 100.0
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type = 'view_item' THEN e.user_id END), 0),
        2
    )                                                                           AS true_purchase_rate,

    -- ── Engagement ───────────────────────────────────────
    ROUND(AVG(e.duration_ms) / 1000.0, 2)                                      AS avg_session_duration_sec,
    ROUND(AVG(e.scroll_depth_pct), 2)                                          AS avg_scroll_depth_pct,

    -- ── Bounce Rate: session chỉ có đúng 1 event ─────────
    ROUND(
        COUNT(DISTINCT CASE WHEN ss.event_count = 1 THEN e.session_id END) * 100.0
        / NULLIF(COUNT(DISTINCT e.session_id), 0),
        2
    )                                                                           AS bounce_rate_pct,

    -- Metadata
    CURRENT_TIMESTAMP                                                           AS dbt_updated_at

FROM events e
-- FIX: LEFT JOIN với subquery đã DISTINCT → không fan-out
LEFT JOIN truly_completed_users t
    ON e.user_id = t.user_id AND e.event_date = t.event_date
LEFT JOIN session_sizes ss
    ON e.session_id = ss.session_id
GROUP BY 1, 2, 3
