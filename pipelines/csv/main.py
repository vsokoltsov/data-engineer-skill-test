import os
from pathlib import Path
from pipelines.csv.reader import read_transactions

if __name__ == '__main__':
    ROOT = Path(__file__).parent.parent.parent
    trx_path = os.path.join(ROOT, "data", "transactions_fr.csv")
    for chunk in read_transactions(path=trx_path, chunk_size=1000):
        print(chunk)