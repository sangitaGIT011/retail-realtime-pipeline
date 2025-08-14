import time, random, json
from pathlib import Path

OUT_DIR = Path('kafka_sim/stream')
OUT_DIR.mkdir(parents=True, exist_ok=True)

PRODUCT_IDS = list(range(1001, 1011))
CUSTOMER_IDS = list(range(5001, 5021))

def main():
    i = 0
    while i < 50:
        evt = {
            'order_id': 90000 + i,
            'order_ts': time.strftime('%Y-%m-%d %H:%M:%S'),
            'store_id': random.choice([1,2,3]),
            'product_id': random.choice(PRODUCT_IDS),
            'customer_id': random.choice(CUSTOMER_IDS),
            'quantity': random.randint(1, 3),
            'unit_price': round(random.uniform(5, 250), 2),
        }
        fname = OUT_DIR / f'event_{int(time.time())}_{i}.json'
        fname.write_text(json.dumps(evt))
        print(f'Wrote {fname}')
        time.sleep(0.2)
        i += 1

if __name__ == '__main__':
    main()
