WITH products AS (
  SELECT p.receipt_ref, p.product_name, p.product_cost, p.product_price, p.product_quantity,
  CASE
    WHEN m.discount IS NULL THEN 0
    ELSE m.discount
  END AS discount,
  CASE
    WHEN m.per_quantity IS NULL THEN 0
    ELSE m.per_quantity
  END as per_quantity
  FROM {{ ref('receipt_products') }} p
  LEFT JOIN {{ ref('receipt_promotions') }} m ON p.receipt_ref = m.receipt_ref AND p.product_name = m.product_name
)
SELECT * FROM products