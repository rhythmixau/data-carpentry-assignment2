WITH business_sales AS (
    SELECT * FROM {{ ref('business_sales') }}
),
    sale_profits AS (
        SELECT * FROM {{ ref('sale_profits') }}
    )
SELECT
  b.business_abn,
  b.business_name,
  b.receipt_date,
  b.year,
  b."month",
  b.month_name,
  b.receipt_ref,
  b.receipt_total,
  b.gst,
  p.total_cost,
  p.total_price,
  p.total_price_after_discount,
  p.total_profit
FROM business_sales b
JOIN sale_profits p ON b.receipt_ref = p.receipt_ref
ORDER BY b.business_abn, b.business_name, b.year, b.month