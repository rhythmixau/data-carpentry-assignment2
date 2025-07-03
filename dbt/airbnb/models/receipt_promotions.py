def get_promotion_product_name(promotion):
    return promotion["Product"]


def get_promotion_discount(promotion):
    return promotion["Discount"]


def get_promotion_per_quanity(promotion):
    return promotion["Per_Quantity"]


def model(dbt, session):
    receipts = dbt.source("receipt_source", "stg_receipts").df()
    receipt_payments_df = receipts[["Reference", "Promotions"]]
    receipt_payments_df = receipt_payments_df[receipt_payments_df["Promotions"].str.len() > 0]
    receipt_payments_df = receipt_payments_df.explode("Promotions")

    receipt_payments_df["product_name"] = receipt_payments_df["Promotions"].apply(get_promotion_product_name)
    receipt_payments_df["discount"] = receipt_payments_df["Promotions"].apply(get_promotion_discount)
    receipt_payments_df["per_quantity"] = receipt_payments_df["Promotions"].apply(get_promotion_per_quanity)

    receipt_payments_df.rename(columns={"Reference": "receipt_ref"}, inplace=True)
    receipt_payments_df = receipt_payments_df[["receipt_ref", "product_name", "discount", "per_quantity"]]
    return receipt_payments_df
