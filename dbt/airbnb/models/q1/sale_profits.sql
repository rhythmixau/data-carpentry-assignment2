WITH products AS (
  SELECT receipt_ref, product_name, product_cost, product_price, product_quantity,
  (product_cost * product_quantity) AS total_cost,
  (product_price * product_quantity) AS total_price
  FROM {{ ref('receipt_products') }}
),
  discounts AS (
  SELECT receipt_ref, product_name, discount, per_quantity
  FROM receipt_promotions
  ),
  products_with_profit AS (
SELECT P.receipt_ref, P.product_name, P.product_cost, P.product_price, P.product_quantity,
  P.total_cost, P.total_price, D.discount, D.per_quantity,
  (FLOOR(P.product_quantity/D.per_quantity) * D.per_quantity) AS discount_qlf_qty,
  CASE
    WHEN D.discount NOT NULL AND discount_qlf_qty > 0 THEN P.total_price * (1.0 - D.discount)
    ELSE P.total_price
  END AS price_after_discount,
  (price_after_discount - total_cost) AS profit
  FROM products P
  LEFT JOIN discounts D ON P.receipt_ref = D.receipt_ref
  AND P.product_name = D.product_name
  )
SELECT receipt_ref,
  ROUND(SUM(total_cost), 2) total_cost,
  ROUND(SUM(total_price), 2)  total_price,
  ROUND(SUM(price_after_discount), 2) AS total_price_after_discount,
  ROUND(SUM(profit), 2) AS total_profit
FROM products_with_profit
GROUP BY receipt_ref