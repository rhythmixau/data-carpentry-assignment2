WITH all_receipts AS (
  SELECT receipt_ref, receipt_date FROM receipts
),
businesses AS (
  SELECT receipt_ref, business_name FROM receipt_businesses
),
products AS (
  SELECT receipt_ref, product_name, product_cost, product_price, product_quantity FROM receipt_products
)
SELECT r.receipt_ref, b.business_name, r.receipt_date,
EXTRACT(MONTH FROM CAST(r.receipt_date AS DATE)) AS month,
EXTRACT(YEAR FROM CAST(r.receipt_date AS DATE)) AS year,
CONCAT("month", '-', "year") AS 'month_year',
  p.* FROM all_receipts r
JOIN businesses b ON r.receipt_ref = b.receipt_ref
JOIN products p ON r.receipt_ref = p.receipt_ref