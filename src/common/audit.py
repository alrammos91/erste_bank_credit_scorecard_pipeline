import sqlite3
from pathlib import Path

ETL_DDL = {
    "etl_runs": (
        """
        CREATE TABLE IF NOT EXISTS etl_runs (
          batch_id   TEXT PRIMARY KEY,
          run_date   TEXT,
          started_at TEXT,
          ended_at   TEXT,
          status     TEXT, 
          message    TEXT
        );
        """
    ),
    "etl_load_stats": (
        """
        CREATE TABLE IF NOT EXISTS etl_load_stats (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id     TEXT,
          table_name   TEXT,
          inserted_rows INTEGER,
          created_at   TEXT
        );
        """
    )
}

class AuditRepository:
    """Audit/lineage helper for SQLite.

    Creates three tables:
      - etl_runs: one row per pipeline run
      - etl_load_stats: row counts inserted per staging table
    """

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.con: sqlite3.Connection

    def connect(self) -> sqlite3.Connection:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = sqlite3.connect(self.db_path)
        for sql in ETL_DDL.values():
            self.con.execute(sql)
            self.con.commit()
        return self.con

    def start_run(self, batch_id: str, run_date: str) -> None:
        self.con.execute(
            "INSERT OR REPLACE INTO etl_runs(batch_id, run_date, started_at, status) VALUES(?, ?, datetime('now'), 'started')",
            (batch_id, run_date),
        )
        self.con.commit()

    def end_run(self, batch_id: str, status: str, message: str = "") -> None:
        assert self.con is not None, "connect() first"
        self.con.execute(
        "UPDATE etl_runs SET ended_at=datetime('now'), status=?, message=? WHERE batch_id=?",
        (status, message, batch_id),
        )
        self.con.commit()

    def log_load_stat(self, batch_id: str, table_name: str, inserted_rows: int) -> None:
        self.con.execute(
            "INSERT INTO etl_load_stats(batch_id, table_name, inserted_rows, created_at) VALUES(?,?,?,datetime('now'))",
            (batch_id, table_name, int(inserted_rows)),
        )
        self.con.commit()

    def close(self) -> None:
        if self.con is not None:
            self.con.close()
            self.con = None
