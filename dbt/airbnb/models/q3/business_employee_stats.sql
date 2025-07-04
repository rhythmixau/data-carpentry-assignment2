WITH business_employee_count AS (
select
  business_name,
  "year",
  "month",
  "month_name",
  COUNT(cashier_name) num_employees
FROM {{ ref('business_employees') }}
GROUP BY business_name, "year", "month", month_name
)
SELECT
  business_name,
"year",
  "month",
  month_name,
  num_employees
FROM business_employee_count
ORDER BY business_name, "year", "month"