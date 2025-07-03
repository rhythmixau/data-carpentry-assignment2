# businesses - abn: str, business_name: str
CREATE_BUSINESS_QUERY = """
CREATE TABLE IF NOT EXISTS receipt_businesses (
    receipt_ref VARCHAR NOT NULL,
    business_abn VARCHAR NOT NULL,
    business_name VARCHAR NOT NULL,
); 
"""

# cashier - cashier_id: int, cashier_name: str
CREATE_CASHIER_QUERY = """
CREATE TABLE IF NOT EXISTS receipt_cashiers (
    receipt_ref VARCHAR NOT NULL,
    cashier_name VARCHAR NOT NULL
) 
"""

# customers - name: str, point: int, customer_id: int
CREATE_CUSTOMER_QUERY = """
CREATE TABLE receipt_customers (
    receipt_ref VARCHAR NOT NULL,
    customer_name VARCHAR,
    cusomter_points INTEGER
);
"""

# payments - reference: str, amount: float, method: str
CREATE_RECEIPT_PAYMENTS_QUERY = """
CREATE TABLE IF NOT EXISTS receipt_payments (
    receipt_ref VARCHAR NOT NULL,
    payment_amount FLOAT NOT NULL,
    payment_method VARCHAR NOT NULL,
); 
"""

# products - reference: str, product_name: str, cost: float, price: float
CREATE_RECEIPT_PRODUCTS_QUERY = """
CREATE TABLE IF NOT EXISTS receipt_products (
    receipt_ref VARCHAR NOT NULL,
    product_name VARCHAR NOT NULL,
    product_cost FLOAT NOT NULL,
    product_price FLOAT NOT NULL,
    product_quantity FLOAT NOT NULL,
) 
"""

# receipt_promotions - receipt_ref: str, product_id: int, discount: float, per_quantity: int
CREATE_RECEIPT_PROMOTIONS_QUERY = """
CREATE TABLE IF NOT EXISTS receipt_promotions (
    receipt_ref VARCHAR NOT NULL,
    product_name VARCHAR NOT NULL,
    discount FLOAT NOT NULL,
    per_quantity INTEGER NOT NULL  
);"""
# receipts: reference: str, seq: int, date: date, gst: float, terminal: int, total: float, business_id, customer_id: int
CREATE_RECEIPTS_QUERY = """
CREATE TABLE IF NOT EXISTS receipts (
    receipt_ref VARCHAR NOT NULL,
    sequence_num INTEGER NOT NULL,
    receipt_date DATE NOT NULL,
    gst FLOAT NOT NULL,
    terminal INTEGER NOT NULL,
    receipt_total FLOAT NOT NULL,
)
"""
