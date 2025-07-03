WITH receipts AS (
    SELECT Reference AS receipt_ref,
           Sequence AS sequence_num,
           Date AS receipt_date,
           GST AS gst,
           Terminal AS terminal_num,
           Total AS receipt_total
    FROM {{ source('receipt_source', 'stg_receipts') }}
)
SELECT * FROM receipts