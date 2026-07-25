{#
  stg_exchange_rates.sql
  ──────────────────────
  Staging model cho bảng exchange_rates từ Silver Zone.

  Vấn đề cần giải quyết:
    - API tỷ giá có thể bị lỗi → exchange_rate = NULL cho một số ngày
    - Nếu NULL propagate vào Mart → toàn bộ doanh thu ngày đó = NULL → báo cáo sai

  Giải pháp:
    - LAST_VALUE(... IGNORE NULLS) OVER (...): lấy tỷ giá hợp lệ gần nhất
    - Ordered by exchange_date trong cùng cặp (base, target) currency
    - Nếu ngày đầu tiên cũng NULL → vẫn NULL (không có data để forward-fill)
      → Mart dùng COALESCE(..., 1.0) làm fallback cuối cùng
#}
{{ config(materialized='view') }}

SELECT
    exchange_date,
    base_currency,
    target_currency,
    -- Forward-fill: lấy tỷ giá hợp lệ gần nhất trong cùng cặp currency
    LAST_VALUE(exchange_rate) IGNORE NULLS OVER (
        PARTITION BY base_currency, target_currency
        ORDER BY exchange_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS exchange_rate,
    -- Giữ raw value để debug
    exchange_rate                               AS raw_exchange_rate
FROM {{ source('silver', 'exchange_rates') }}
