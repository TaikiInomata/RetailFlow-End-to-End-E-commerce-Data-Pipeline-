{#
  stg_users.sql
  ─────────────
  Staging model cho bảng users từ Silver Zone.
  Nhiệm vụ:
    - Đổi tên id → user_id
    - Chọn các cột cần thiết cho Customer 360
    - Lọc is_deleted = FALSE
    - Chuẩn hóa registration_date về DATE type
#}
{{ config(materialized='view') }}

SELECT
    id                                          AS user_id,
    name                                        AS username,
    email,
    COALESCE(city, 'Unknown')                   AS city,
    COALESCE(country, 'Unknown')                AS country,
    DATE(CAST(from_iso8601_timestamp(created_at) AS TIMESTAMP)) AS registration_date,
    CAST(from_iso8601_timestamp(created_at) AS TIMESTAMP)       AS created_at,
    CAST(from_iso8601_timestamp(updated_at) AS TIMESTAMP)       AS updated_at
FROM {{ source('silver', 'users') }}
WHERE is_deleted = FALSE
