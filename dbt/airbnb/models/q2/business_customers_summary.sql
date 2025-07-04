WITH all_business_customers AS (
  SELECT * FROM {{ ref('business_customers') }}
)
SELECT business_name, customer_name,
  SUM(amount_spent) amount_spent,
  COUNT(receipt_ref) num_purchases
FROM all_business_customers
GROUP BY business_name, customer_name
ORDER BY business_name, amount_spent DESC