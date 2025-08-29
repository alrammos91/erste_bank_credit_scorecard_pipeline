from pathlib import Path
import sqlite3
import pandas as pd
from common.audit import AuditRepository

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
          _ingested_at TEXT,
          _batch_id TEXT
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
          _ingested_at TEXT,
          _batch_id TEXT
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
          _ingested_at TEXT,
          _batch_id TEXT
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
          _ingested_at TEXT,
          _batch_id TEXT
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
          _ingested_at TEXT,
          _batch_id TEXT
        );
        """
    ),
}


class SQLiteStagingLoader:
    """ Class for loading a daily CSV folder into SQLite staging tables."""

    def __init__(self, sqlite_path: str | Path, audit: AuditRepository):
        self.sqlite_path = str(sqlite_path)
        self.con: sqlite3.Connection | None = None
        self.audit = audit

    def connect(self) -> sqlite3.Connection:
        if self.con is None:
            Path(self.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
            self.con = sqlite3.connect(self.sqlite_path)
        for sql in DDL.values():
            self.con.execute(sql)
        self.con.commit()
        return self.con

    def close(self) -> None:
        if self.con:
            self.con.close()
            self.con = None
    
    def ensure_columns(self, table: str, df: pd.DataFrame) -> None:
        assert self.con is not None
        cur = self.con.execute(f"PRAGMA table_info({table})")
        existing_cols = [r[1] for r in cur.fetchall()]
        for col in df.columns:
            if col not in existing_cols:
                self.con.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
        self.con.commit()

    def load_day_staging(self, day_dir: str | Path, batch_id: str) -> None:
        """Load a single daily folder of CSVs into SQLite staging tables.

        - Preserves raw values (TEXT) and avoids NA coercion
        - Adds metadata columns: _run_date, _source_file, _ingested_at
        - Uses a single transaction for speed
        """
        assert self.con is not None
        day_dir = Path(day_dir)
        if not day_dir.exists():
            raise FileNotFoundError(f"Day directory not found: {day_dir}")
        try:
            for name in TABLES:
                fp = day_dir / f"{name}.csv"
                if not fp.exists():
                    continue

                df = pd.read_csv(fp)
                df["_run_date"] = day_dir.name
                df["_source_file"] = fp.name
                df["_ingested_at"] = pd.Timestamp.utcnow().isoformat()
                df["_batch_id"] = batch_id
                self.ensure_columns(f"stg_{name}", df)
                df.to_sql(f"stg_{name}", self.con, if_exists="append", index=False)

                self.audit.log_load_stat(batch_id, table_name=f"stg_{name}", inserted_rows=len(df))
                print(f"Loaded {len(df):5d} rows into stg_{name}")

            self.con.commit()
            print(f"SQLite staging load complete in {self.sqlite_path}")
        except Exception as e:
            self.con.rollback()
            raise
