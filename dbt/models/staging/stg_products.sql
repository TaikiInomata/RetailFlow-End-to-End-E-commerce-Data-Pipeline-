{#
  stg_products.sql
  ────────────────
  Staging model cho bảng products từ Silver Zone.
  Nhiệm vụ:
    - Đổi tên id → product_id
    - Giữ các cột cần thiết cho Gold models
    - Lọc is_deleted = FALSE
    - Điền NULL category bằng 'Unknown' (tránh NULL gây GROUP BY sai)
#}
{{ config(materialized='view') }}

SELECT
    product_id                                  AS product_id,
    brand                                       AS product_name,
    COALESCE(category_code, 'Unknown')          AS category,
    COALESCE(brand, 'Unknown')                  AS brand,
    avg_price                                   AS price,
    CAST(from_iso8601_timestamp(created_at) AS TIMESTAMP)       AS created_at,
    CAST(from_iso8601_timestamp(created_at) AS TIMESTAMP)       AS updated_at
FROM {{ source('silver', 'products') }}
WHERE is_deleted = FALSE
