
def get_payment_amount(payment) -> float:
    return payment["Amount"]


def get_payment_method(payment) -> str:
    return payment["Method"]


def model(dbt, session):
    receipts = dbt.source("receipt_source", "stg_receipts").df()
    receipt_payments_df = receipts[["Reference", "Payments"]]
    receipt_payments_df = receipt_payments_df[receipt_payments_df["Payments"].str.len() > 0]
    receipt_payments_df = receipt_payments_df.explode("Payments")
    receipt_payments_df["payment_amount"] = receipt_payments_df["Payments"].apply(get_payment_amount)
    receipt_payments_df["payment_method"] = receipt_payments_df["Payments"].apply(get_payment_method)
    receipt_payments_df.rename(columns={"Reference": "receipt_ref"}, inplace=True)
    receipt_payments_df = receipt_payments_df[["receipt_ref", "payment_method", "payment_amount"]]
    return receipt_payments_df
