import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path('sql/warehouse.sqlite')

def run_query(sql):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(sql, conn)

if __name__ == '__main__':
    print('Top 5 products by revenue:')
    q1 = '''
    SELECT p.product_name, SUM(f.revenue) AS total_revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 5
    '''
    print(run_query(q1).to_string(index=False))

    print('\nDaily revenue:')
    q2 = '''
    SELECT d.date, SUM(f.revenue) AS revenue
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY 1
    ORDER BY 1
    '''
    print(run_query(q2).head().to_string(index=False))
