WITH distinct_employees AS (
  SELECT DISTINCT business_name, "year", "month", month_name, cashier_name
  FROM {{ ref('business_employees') }}
  ORDER BY business_name, "year", "month", cashier_name
)
select business_name, "year", "month", month_name,
  STRING_AGG(cashier_name, ', ') AS unique_employees
from distinct_employees
-- where business_name = 'Ed''s Barber Supplies'
GROUP BY business_name, "year", "month", month_name
ORDER BY business_name, "year", "month"