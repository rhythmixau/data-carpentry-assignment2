WITH receipts AS (
    SELECT Reference AS receipt_ref, Cashier AS cashier_name
    FROM {{ source('receipt_source', 'stg_receipts') }}
)
SELECT * FROM receipts