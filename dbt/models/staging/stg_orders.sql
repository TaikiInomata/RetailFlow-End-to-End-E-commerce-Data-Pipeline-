{#
  stg_orders.sql
  ──────────────
  Staging model cho bảng orders từ Silver Zone.
  Nhiệm vụ:
    - Đổi tên cột id → order_id (chuẩn hóa naming convention)
    - Cast timestamp strings → TIMESTAMP type
    - Thêm cột order_date để dùng trong partition/filter
    - Lọc is_deleted = FALSE (loại bỏ tombstone records từ CDC)
#}
{{ config(materialized='view') }}

SELECT
    id                                  AS order_id,
    user_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    currency,
    payment_method,
    status,
    CAST(from_iso8601_timestamp(created_at) AS TIMESTAMP)       AS created_at,
    CAST(from_iso8601_timestamp(updated_at) AS TIMESTAMP)       AS updated_at,
    DATE(CAST(from_iso8601_timestamp(created_at) AS TIMESTAMP)) AS order_date
FROM {{ source('silver', 'orders') }}
WHERE is_deleted = FALSE
