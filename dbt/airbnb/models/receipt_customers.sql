WITH receipts AS (
    SELECT Reference AS receipt_ref,
           Customer['Name'] AS customer_name,
           Customer['Points_Earnt'] AS customer_points
    FROM {{ source('receipt_source', 'stg_receipts') }}
)
SELECT * FROM receipts