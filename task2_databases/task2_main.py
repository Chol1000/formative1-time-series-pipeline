import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sql_database import run_sql_pipeline
from mongodb_implementation import run_mongodb_pipeline

if __name__ == "__main__":
    run_sql_pipeline()
    print()
    run_mongodb_pipeline()
    print()
    print("=" * 72)
    print("TASK 2 COMPLETE — SQL (7 queries) + MongoDB (5 queries)")
    print("=" * 72)