import json, shutil
from pathlib import Path
import pandas as pd

STREAM_DIR = Path('kafka_sim/stream')
RAW_DIR = Path('data/raw')
RAW_DIR.mkdir(parents=True, exist_ok=True)

def main():
    events = []
    for f in sorted(STREAM_DIR.glob('event_*.json')):
        evt = json.loads(f.read_text())
        events.append(evt)
        # move to processed
        dest = f.with_suffix('.done')
        shutil.move(str(f), str(dest))
    if events:
        df = pd.DataFrame(events)
        out = RAW_DIR / 'stream_append.csv'
        header = not out.exists()
        df.to_csv(out, mode='a', header=header, index=False)
        print(f'Appended {len(df)} events to {out}')

if __name__ == '__main__':
    main()
