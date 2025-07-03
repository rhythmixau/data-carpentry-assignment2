def get_product_name(product):
    return product["Name"]


def get_product_price(product):
    return product["Price"]


def get_product_cost(product):
    return product["Cost"]


def get_product_quantity(product):
    return product["Quantity"]


def model(dbt, session):
    receipts = dbt.source("receipt_source", "stg_receipts").df()
    receipt_products_df = receipts[["Reference", "Products"]]
    receipt_products_df = receipt_products_df[receipt_products_df["Products"].str.len() > 0]
    receipt_products_df = receipt_products_df.explode("Products")
    receipt_products_df["product_name"] = receipt_products_df["Products"].apply(get_product_name)
    receipt_products_df["product_cost"] = receipt_products_df["Products"].apply(get_product_cost)
    receipt_products_df["product_price"] = receipt_products_df["Products"].apply(get_product_price)
    receipt_products_df["product_quantity"] = receipt_products_df["Products"].apply(get_product_quantity)
    receipt_products_df.rename(columns={"Reference": "receipt_ref"}, inplace=True)
    receipt_products_df = receipt_products_df[["receipt_ref", "product_name", "product_cost", "product_price",
                                               "product_quantity"]]
    return receipt_products_df
