from pathlib import Path
import sqlite3
import pandas as pd

# Tables from the daily drop
TABLES = ["applications", "accounts", "transactions", "payments", "delinquency"]

DDL = {
    "stg_applications": (
        """
        CREATE TABLE IF NOT EXISTS stg_applications (
          application_id TEXT,
          scorecard_version TEXT,
          decision TEXT,
          bureau_score TEXT,
          product TEXT,
          channel TEXT,
          segment TEXT,
          _run_date TEXT,
          _source_file TEXT,
          _ingested_at TEXT
        );
        """
    ),
    "stg_accounts": (
        """
        CREATE TABLE IF NOT EXISTS stg_accounts (
          account_id TEXT,
          application_id TEXT,
          activation_date TEXT,
          _run_date TEXT,
          _source_file TEXT,
          _ingested_at TEXT
        );
        """
    ),
    "stg_transactions": (
        """
        CREATE TABLE IF NOT EXISTS stg_transactions (
          transaction_id TEXT,
          account_id TEXT,
          transaction_date TEXT,
          amount TEXT,
          _run_date TEXT,
          _source_file TEXT,
          _ingested_at TEXT
        );
        """
    ),
    "stg_payments": (
        """
        CREATE TABLE IF NOT EXISTS stg_payments (
          payment_id TEXT,
          account_id TEXT,
          payment_date TEXT,
          amount TEXT,
          _run_date TEXT,
          _source_file TEXT,
          _ingested_at TEXT
        );
        """
    ),
    "stg_delinquency": (
        """
        CREATE TABLE IF NOT EXISTS stg_delinquency (
          account_id TEXT,
          days_past_due TEXT,
          default_flag TEXT,
          _run_date TEXT,
          _source_file TEXT,
          _ingested_at TEXT
        );
        """
    ),
}


class SQLiteStagingLoader:
    """Simple class wrapper for loading a daily CSV folder into SQLite staging tables.

    Logic is identical to your original functions, just organized as methods.
    No extra features, no stats objects, no pragmas.
    """

    def __init__(self, sqlite_path: str | Path):
        self.sqlite_path = str(sqlite_path)
        self.con: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        Path(self.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = sqlite3.connect(self.sqlite_path)
        return self.con

    def close(self) -> None:
        if self.con is not None:
            self.con.close()
            self.con = None

    def ensure_staging_tables(self) -> None:
        cur = self.con.cursor()
        for ddl in DDL.values():
            cur.executescript(ddl)
        self.con.commit()

    def ensure_columns(self, full_table: str, df):
        # get existing column names
        existing = {row[1] for row in self.con.execute(f"PRAGMA table_info({full_table});")}
        # add any missing columns as TEXT
        to_add = [c for c in df.columns if c not in existing]
        for col in to_add:
            self.con.execute(f'ALTER TABLE {full_table} ADD COLUMN "{col}" TEXT')

    def load_day_staging(self, day_dir: str | Path) -> None:
        """Load a single daily folder of CSVs into SQLite staging tables.

        - Preserves raw values (TEXT) and avoids NA coercion
        - Adds metadata columns: _run_date, _source_file, _ingested_at
        - Uses a single transaction for speed
        """
        day_dir = Path(day_dir)
        if not day_dir.exists():
            raise FileNotFoundError(f"Day directory not found: {day_dir}")

        self.ensure_staging_tables()
        cur = self.con.cursor()
        cur.execute("BEGIN")

        try:
            for name in TABLES:
                fp = day_dir / f"{name}.csv"
                if not fp.exists():
                    continue

                df = pd.read_csv(fp, dtype=str, keep_default_na=False, na_filter=False)
                df["_run_date"] = day_dir.name
                df["_source_file"] = fp.name
                df["_ingested_at"] = pd.Timestamp.utcnow().isoformat()
                self.ensure_columns(f"stg_{name}", df)
                df.to_sql(f"stg_{name}", self.con, if_exists="append", index=False)
                print(f"Loaded {len(df):5d} rows into stg_{name}")

            self.con.commit()
            print(f"SQLite staging load complete → {self.sqlite_path}")
        except Exception:
            self.con.rollback()
            raise


# # ---------------- CLI (optional) ----------------

# def main() -> None:
#     parser = argparse.ArgumentParser(description="Load a daily CSV folder into SQLite staging tables.")
#     parser.add_argument("--day-dir", required=True, help="Folder with daily CSVs, e.g., erste_bank_data/2025-08-25")
#     parser.add_argument("--db", default="db/erste_scorecard.db", help="Path to SQLite DB file")
#     args = parser.parse_args()

#     loader = SQLiteStagingLoader(args.db)
#     loader.connect()
#     try:
#         loader.load_day(args.day_dir)
#     finally:
#         loader.close()


# if __name__ == "__main__":
#     main()
