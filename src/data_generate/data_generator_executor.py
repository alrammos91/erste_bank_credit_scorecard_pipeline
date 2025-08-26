import argparse
import sys
from data_generator import EsteDataGenerator
from data_quality_checks import DataQualityChecker
from data_staging import SQLiteStagingLoader

def execute_data_generation() -> None:
    parser = argparse.ArgumentParser(description="Generate daily synthetic Erste Bank data for Credit Score prediction.")
    parser.add_argument("--n-apps", type=int, default=200, help="Number of applications to simulate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (use e.g. 42)")
    parser.add_argument("--schema", default="config/data_quality_schema.json", help="Path to DQ schema JSON file")
    parser.add_argument("--db", default="db/erste_scorecard.db", help="Path to SQLite DB file")
    args = parser.parse_args()

    gen = EsteDataGenerator(n_apps=args.n_apps, seed=args.seed)
    gen.generate_and_save_all()

    # run DQ
    dq = DataQualityChecker(day_dir=gen.day_dir, schema_path=args.schema)
    ok = dq.evaluate_all(write_report=True)
    if not ok:
        print("\n Data quality checks failed. See logs/ and output/ for details.")
        sys.exit(1)
        print("\nData quality passed.")

    loader = SQLiteStagingLoader(args.db)
    loader.connect()
    try:
        loader.load_day_staging(day_dir=gen.day_dir)
    finally:
        loader.close()

if __name__ == "__main__":
    execute_data_generation()
