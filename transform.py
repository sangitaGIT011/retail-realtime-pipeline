import pandas as pd
from pathlib import Path

PROC_DIR = Path('data/processed')
REF_DIR = Path('data/reference')

def build_dimensions(df_sales, df_products, df_customers):
    # dim_date
    dim_date = (df_sales[['order_ts']]
                .dropna()
                .drop_duplicates()
                .assign(date=lambda d: d['order_ts'].dt.date)
                .assign(date_key=lambda d: d['date'].astype(str).str.replace('-','').astype(int),
                        year=lambda d: d['order_ts'].dt.year,
                        month=lambda d: d['order_ts'].dt.month,
                        day=lambda d: d['order_ts'].dt.day,
                        dow=lambda d: d['order_ts'].dt.dayofweek)
               )
    dim_date = dim_date[['date_key','date','year','month','day','dow']].drop_duplicates()

    # dim_product
    dim_product = df_products.copy()
    dim_product['product_key'] = dim_product['product_id']
    dim_product = dim_product[['product_key','product_id','product_name','category','unit_price']]

    # dim_customer
    dim_customer = df_customers.copy()
    dim_customer['customer_key'] = dim_customer['customer_id']
    dim_customer = dim_customer[['customer_key','customer_id','first_name','last_name','city']]

    return dim_date, dim_product, dim_customer

def build_fact(df_sales):
    fact = df_sales.copy()
    fact['date_key'] = fact['order_ts'].dt.strftime('%Y%m%d').astype(int)
    fact['product_key'] = fact['product_id']
    fact['customer_key'] = fact['customer_id']
    fact['revenue'] = (fact['quantity'] * fact['unit_price']).round(2)
    fact = fact[['order_id','order_ts','store_id','date_key','product_key','customer_key','quantity','unit_price','revenue']]
    return fact

def main():
    df_sales = pd.read_parquet(PROC_DIR / 'sales_clean.parquet')
    df_products = pd.read_csv(REF_DIR / 'products.csv')
    df_customers = pd.read_csv(REF_DIR / 'customers.csv')

    dim_date, dim_product, dim_customer = build_dimensions(df_sales, df_products, df_customers)
    fact_sales = build_fact(df_sales)

    dim_date.to_parquet(PROC_DIR / 'dim_date.parquet', index=False)
    dim_product.to_parquet(PROC_DIR / 'dim_product.parquet', index=False)
    dim_customer.to_parquet(PROC_DIR / 'dim_customer.parquet', index=False)
    fact_sales.to_parquet(PROC_DIR / 'fact_sales.parquet', index=False)
    print('Wrote star schema parquet files.')

if __name__ == '__main__':
    main()
