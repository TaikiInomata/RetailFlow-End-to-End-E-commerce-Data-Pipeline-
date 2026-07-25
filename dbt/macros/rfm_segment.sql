{#
  rfm_segment.sql — Macro phân khúc RFM
  ──────────────────────────────────────
  Tái dùng trong: gold_customer_snapshot, bất kỳ model nào cần segment

  Tham số:
    recency_col   — Số ngày kể từ lần mua cuối (thấp = tốt)
    frequency_col — Tổng số đơn hàng (cao = tốt)
    monetary_col  — Tổng doanh thu USD (cao = tốt)

  Thay đổi rules → sửa 1 lần tại đây, áp dụng toàn project
#}
{% macro rfm_segment(recency_col, frequency_col, monetary_col) %}
CASE
    -- Chưa mua hàng bao giờ
    WHEN {{ frequency_col }} IS NULL
      OR {{ frequency_col }} = 0                                THEN 'Never Bought'
    -- Mua cách đây > 6 tháng → coi như mất
    WHEN {{ recency_col }} > 180                                THEN 'Lost'
    -- Mua gần đây nhưng đang giảm, có giá trị → cần re-engagement
    WHEN {{ recency_col }} > 60
     AND {{ monetary_col }} > 100                               THEN 'At Risk'
    -- Mua thường xuyên, giá trị cao, gần đây → khách VIP
    WHEN {{ recency_col }} <= 30
     AND {{ frequency_col }} >= 5
     AND {{ monetary_col }} >= 200                              THEN 'Champions'
    -- Mua thường xuyên, còn active
    WHEN {{ recency_col }} <= 60
     AND {{ frequency_col }} >= 3                               THEN 'Loyal'
    -- Mới mua lần đầu/lần hai → tiềm năng
    WHEN {{ recency_col }} <= 30
     AND {{ frequency_col }} <= 2                               THEN 'Promising'
    -- Tất cả trường hợp còn lại
    ELSE 'New'
END
{% endmacro %}
