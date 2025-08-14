import sqlite3
import pandas as pd
from pathlib import Path

PROC_DIR = Path('data/processed')
DB_PATH = Path('sql/warehouse.sqlite')
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        for name in ['dim_date','dim_product','dim_customer','fact_sales']:
            df = pd.read_parquet(PROC_DIR / f'{name}.parquet')
            df.to_sql(name, conn, if_exists='replace', index=False)
            print(f'Loaded {name} -> {DB_PATH}')
    finally:
        conn.close()

if __name__ == '__main__':
    main()
