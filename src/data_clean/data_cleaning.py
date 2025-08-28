import sqlite3
from pathlib import Path
from typing import Optional
import datetime as dt


CLEAN_DDL_SQL = """
CREATE TABLE IF NOT EXISTS clean_applications (
  application_id TEXT,
  scorecard_version TEXT,
  decision TEXT,
  bureau_score INTEGER,
  product TEXT,
  channel TEXT,
  segment TEXT,
  _run_date TEXT,
  _ingested_at TEXT
);

CREATE TABLE IF NOT EXISTS clean_accounts (
  account_id TEXT,
  application_id TEXT,
  activation_date TEXT,
  _run_date TEXT,
  _ingested_at TEXT
);

CREATE TABLE IF NOT EXISTS clean_transactions (
  transaction_id TEXT,
  account_id TEXT,
  transaction_date TEXT,
  amount REAL,
  _run_date TEXT,
  _ingested_at TEXT
);

CREATE TABLE IF NOT EXISTS clean_payments (
  payment_id TEXT,
  account_id TEXT,
  payment_date TEXT,
  amount REAL,
  _run_date TEXT,
  _ingested_at TEXT
);

CREATE TABLE IF NOT EXISTS clean_delinquency (
  account_id TEXT,
  days_past_due INTEGER,
  default_flag INTEGER,
  _run_date TEXT,
  _ingested_at TEXT
);

CREATE TABLE IF NOT EXISTS application_performance (
  application_id TEXT,
  account_id TEXT,
  activation_date TEXT,
  scorecard_version TEXT,
  decision TEXT,
  bureau_score INTEGER,
  product TEXT,
  channel TEXT,
  segment TEXT,
  txn_amount_30d REAL,
  pmt_amount_30d REAL,
  days_past_due INTEGER,
  default_flag INTEGER,
  _run_date TEXT
);
"""

class SQLiteCleaner:
    """Build clean tables from staging.
        Dedup strategy (latest record wins):
        - applications: by application_id
        - accounts: by account_id
        - transactions: by transaction_id
        - payments: by payment_id
        - delinquency: by account_id
    """

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.con: Optional[sqlite3.Connection] = None

    def connect(self) -> sqlite3.Connection:
        if self.con is None:
          self.con = sqlite3.connect(self.db_path)
          self.con.executescript(CLEAN_DDL_SQL)
          self.con.commit()
        return self.con

    def close(self) -> None:
        if self.con:
            self.con.close()
            self.con = None
            
    @staticmethod
    def resolve_run_date(run_date: Optional[str]) -> str:
        if run_date:
            return run_date
        return dt.date.today().isoformat()

    def build_clean_tables(self, run_date: str) -> None:
        assert self.con is not None

        # Idempotency: delete existing rows for the run
        for t in [
          "clean_applications",
          "clean_accounts",
          "clean_transactions",
          "clean_payments",
          "clean_delinquency",
        ]:
          self.con.execute(f"DELETE FROM {t} WHERE _run_date = ?", (run_date,))

        # Applications
        self.con.execute(
            """
            INSERT INTO clean_applications
            SELECT application_id,
              scorecard_version,
              decision,
              CAST(bureau_score AS INTEGER) AS bureau_score,
              product,
              channel,
              segment,
              _run_date,
              _ingested_at
            FROM (
              SELECT *,
                ROW_NUMBER() OVER (PARTITION BY application_id ORDER BY _ingested_at DESC, _batch_id DESC) AS rn
              FROM stg_applications
              WHERE _run_date = ?
            ) s
            WHERE rn = 1;
            """,
            (run_date,),
          )
        print("clean_applications has been loaded")

        # Accounts
        self.con.execute(
          """
          INSERT INTO clean_accounts
          SELECT 
            account_id,
            application_id,
            activation_date,
            _run_date,
            _ingested_at
          FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _ingested_at DESC, _batch_id DESC) rn
            FROM stg_accounts
            WHERE _run_date = ?
          ) s
          WHERE rn = 1;
          """,
          (run_date,),
        )
        print("clean_accounts has been loaded")

        # Transactions (keep the ID in clean table)
        self.con.execute(
          """
          INSERT INTO clean_transactions
          SELECT 
            transaction_id,
            account_id,
            transaction_date,
            CAST(amount AS REAL) AS amount,
            _run_date,
            _ingested_at
          FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY _ingested_at DESC, _batch_id DESC) rn
            FROM stg_transactions
            WHERE _run_date = ?
          ) s
          WHERE rn = 1;
          """,
          (run_date,),
        )
        print("clean_transactions has been loaded")

        # Payments (keep the ID)
        self.con.execute(
          """
          INSERT INTO clean_payments
          SELECT 
            payment_id,
            account_id,
            payment_date,
            CAST(amount AS REAL) AS amount,
            _run_date,
            _ingested_at
          FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _ingested_at DESC, _batch_id DESC) rn
            FROM stg_payments
            WHERE _run_date = ?
          ) s
          WHERE rn = 1;
          """,
          (run_date,),
        )
        print("clean_payments has been loaded")

        # Delinquency
        self.con.execute(
          """
          INSERT INTO clean_delinquency
          SELECT 
            account_id,
            CAST(days_past_due AS INTEGER),
            CAST(default_flag AS INTEGER),
            _run_date,
            _ingested_at
          FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _ingested_at DESC, _batch_id DESC) rn
          FROM stg_delinquency
          WHERE _run_date = ?
          ) s
          WHERE rn = 1;
          """,
          (run_date,),
        )
        print("clean_delinquency has been loaded")
        print(f"Built clean tables for run_date={run_date}")
        self.con.commit()


