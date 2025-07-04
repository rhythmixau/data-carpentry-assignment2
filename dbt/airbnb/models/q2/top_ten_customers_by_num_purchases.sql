WITH all_business_customers AS (
    SELECT * FROM {{ ref('business_customers_summary') }}
), customer_ranks AS (
  SELECT business_name, customer_name,
  num_purchases,
  ROUND(amount_spent, 2) AS amount_spent,
  row_number() OVER(partition BY business_name ORDER BY num_purchases DESC, amount_spent DESC) AS ranking
  FROM all_business_customers
ORDER BY business_name, ranking
  ) SELECT *
  FROM customer_ranks
  WHERE ranking <= 10