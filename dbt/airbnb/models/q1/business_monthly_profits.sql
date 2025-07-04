WITH business_sales AS (
  SELECT * FROM {{ ref('business_sales_profits') }}
)
SELECT
  business_abn,
  business_name,
  "year",
  "month",
  month_name,
  SUM(total_profit) total_monthly_profit,
  COUNT(receipt_ref) AS num_of_sales
FROM business_sales
GROUP BY business_abn, business_name, "year", "month", month_name
ORDER BY business_name, "year", "month"