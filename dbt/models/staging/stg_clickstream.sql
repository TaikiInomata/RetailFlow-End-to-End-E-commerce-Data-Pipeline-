{#
  stg_clickstream.sql
  ───────────────────
  Staging model cho bảng clickstream từ Silver Zone.
  Nhiệm vụ:
    - Cast event_timestamp → TIMESTAMP + thêm event_date (DATE)
    - Parse referrer URL → referrer_category (logic tập trung 1 chỗ)
      → Dùng lại trong gold_daily_funnel + gold_product_engagement
      → Nếu rules thay đổi, chỉ sửa 1 file staging, không phải sửa tất cả mart
    - Không lọc gì thêm: clickstream giữ raw events, mart tự filter theo event_type/step
#}
{{ config(materialized='view') }}

SELECT
    event_id,
    user_id,
    session_id,
    event_type,
    device,
    step,
    real_order_id,                              -- FK thật sang orders.id (INTEGER)
    item_id,
    duration_ms,
    scroll_depth_pct,
    search_query,
    result_count,
    click_target,
    quantity,
    cart_total,
    purchase_amount,
    CAST(event_timestamp AS TIMESTAMP)          AS event_timestamp,
    DATE(CAST(event_timestamp AS TIMESTAMP))    AS event_date,
    -- Phân loại nguồn traffic (tập trung ở đây, dùng lại trong mọi mart)
    CASE
        WHEN referrer LIKE '%google%'
          OR referrer LIKE '%bing%'
          OR referrer LIKE '%yahoo%'            THEN 'organic_search'
        WHEN referrer LIKE '%facebook%'
          OR referrer LIKE '%instagram%'
          OR referrer LIKE '%tiktok%'
          OR referrer LIKE '%twitter%'
          OR referrer LIKE '%youtube%'          THEN 'social'
        WHEN referrer = 'email'                 THEN 'email'
        WHEN referrer = 'direct'
          OR referrer IS NULL
          OR referrer = ''                      THEN 'direct'
        ELSE 'other'
    END                                         AS referrer_category
FROM {{ source('silver', 'clickstream') }}
