def model(dbt, session):
    # Get fresh data from the staging receipts table
    receipts_data = dbt.source("receipt_source", "stg_receipts").df()
    # 2. Create a boolean mask for filtering.
    #    - Use the bitwise '&' operator for element-wise 'AND' operations.
    #    - This is much more efficient than chaining filters.
    mask = (
            receipts_data["Customer"].notnull() &
            receipts_data["Business"].notnull() &
            (receipts_data["Total"] > 0)
    )

    # 3. Apply the filter mask to the DataFrame.
    #    Using .loc is explicit and helps avoid potential SettingWithCopyWarning.
    df = receipts_data.loc[mask].copy()

    # 4. Extract the 'Name' value from the dictionary columns.
    #    The .str.get('key') accessor is a vectorized operation that is
    #    highly optimized for this task, replacing the slow row-by-row loop.
    df["Customer"] = df["Customer"].str.get("Name")
    df["Business"] = df["Business"].str.get("Name")

    # 5. Select and return the final columns.
    result = df[["Reference", "Customer", "Business", "Total"]]
    result.rename(columns={"Reference": "receipt_ref",
                           "Business": "business_name",
                           "Customer": "customer_name",
                           "Total": "amount_spent"}, inplace=True)
    return result
