-- Star schema (reference); actual tables are written by load.py into SQLite
-- Use as guidance to create DDL if migrating to Postgres/Redshift.

CREATE TABLE dim_product (
    product_key INT PRIMARY KEY,
    product_id INT,
    product_name TEXT,
    category TEXT,
    unit_price NUMERIC(10,2)
);

CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id INT,
    first_name TEXT,
    last_name TEXT,
    city TEXT
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    date DATE,
    year INT,
    month INT,
    day INT,
    dow INT
);

CREATE TABLE fact_sales (
    order_id BIGINT PRIMARY KEY,
    order_ts TIMESTAMP,
    store_id INT,
    date_key INT REFERENCES dim_date(date_key),
    product_key INT REFERENCES dim_product(product_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    quantity INT,
    unit_price NUMERIC(10,2),
    revenue NUMERIC(12,2)
);
