WITH business_employees AS (
SELECT DISTINCT b.business_name,
  EXTRACT(YEAR FROM CAST(r.receipt_date AS DATE)) AS year,
  EXTRACT(MONTH FROM CAST(r.receipt_date AS DATE)) AS month,
  strftime(CAST(r.receipt_date AS DATE), '%B') month_name,
  c.cashier_name
  FROM {{ ref('receipt_businesses') }} b
JOIN {{ ref('receipt_cashiers') }} c ON b.receipt_ref = c.receipt_ref
JOIN {{ ref('receipts') }} r ON r.receipt_ref = b.receipt_ref
ORDER BY "year", "month", b.business_name, c.cashier_name
)
SELECT * FROM business_employees