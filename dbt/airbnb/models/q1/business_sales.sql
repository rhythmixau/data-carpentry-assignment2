WITH business_receipts AS (
  SELECT
    B.business_abn,
    B.business_name,
    R.receipt_ref,
    R.receipt_date,
    R.receipt_total,
    R.gst
  FROM {{ ref('receipts') }} AS R
  JOIN {{ ref('receipt_businesses') }} AS B
    ON R.receipt_ref = B.receipt_ref
)
SELECT
  *,
  EXTRACT(YEAR FROM CAST(receipt_date AS DATE)) AS year,
  EXTRACT(MONTH FROM CAST(receipt_date AS DATE)) AS month,
  strftime(CAST(receipt_date AS DATE), '%B') month_name
FROM business_receipts
ORDER BY
  business_abn,
  business_name,
  receipt_date