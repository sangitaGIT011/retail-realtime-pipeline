import pandas as pd
from pathlib import Path

RAW_DIR = Path('data/raw')
REF_DIR = Path('data/reference')
PROC_DIR = Path('data/processed')
PROC_DIR.mkdir(parents=True, exist_ok=True)

def main():
    sales_files = sorted(RAW_DIR.glob('sales_*.csv'))
    if not sales_files:
        raise SystemExit('No raw sales files found.')
    df_sales = pd.concat((pd.read_csv(f) for f in sales_files), ignore_index=True)
    df_products = pd.read_csv(REF_DIR / 'products.csv')
    df_customers = pd.read_csv(REF_DIR / 'customers.csv')

    # Basic quality checks
    df_sales = df_sales.drop_duplicates()
    df_sales = df_sales.dropna(subset=['order_id','order_ts','product_id','customer_id','quantity','unit_price'])

    # Type coercion
    df_sales['order_ts'] = pd.to_datetime(df_sales['order_ts'], errors='coerce')
    df_sales = df_sales[df_sales['order_ts'].notna()]
    df_sales['quantity'] = pd.to_numeric(df_sales['quantity'], errors='coerce').fillna(0).astype(int)
    df_sales['unit_price'] = pd.to_numeric(df_sales['unit_price'], errors='coerce').round(2)

    # Basic business rules
    df_sales = df_sales[df_sales['quantity'] > 0]
    df_sales = df_sales[df_sales['unit_price'] >= 0]

    # Save intermediate parquet
    out_path = PROC_DIR / 'sales_clean.parquet'
    df_sales.to_parquet(out_path, index=False)
    print(f'Wrote {out_path} with {len(df_sales)} rows.')

if __name__ == '__main__':
    main()
