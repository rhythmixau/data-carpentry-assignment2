WITH receipts AS (
    SELECT Reference AS receipt_ref, Business['ABN'] AS business_abn, Business['Name'] AS business_name
    FROM {{ source('receipt_source', 'stg_receipts') }}
)
SELECT * FROM receipts