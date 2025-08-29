import json
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional
import pandas as pd


@dataclass
class DQResult:
    table: str
    check: str
    severity: str
    passed: bool
    n_affected: int
    details: str = ""


class DataQualityChecker:
    """Config-driven DQ executor for the daily drop.
       Schema JSON example is provided under config/.
    """
    def __init__(
            self, 
            day_dir: Path | str, 
            schema_path: Path | str | None = None, 
            log_dir: Optional[Path | str] = None, 
            logger: Optional[logging.Logger] = None
    ):
        self.day_dir = Path(day_dir)
        self.schema_path = Path(schema_path) if schema_path else Path("config/data_quality_schema.json")
        self.schema: Dict[str, Dict] = self.load_schema(self.schema_path)

        self.log_dir = Path(log_dir) if log_dir else Path("logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger or self.build_logger()
        self.results: List[DQResult] = []
        self.tables: Dict[str, pd.DataFrame] = {}

    def load_schema(self, path: Path) -> Dict[str, Dict]:
        if not path.exists():
            raise FileNotFoundError(f"DQ schema JSON not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        # Normalize: convert enum lists to sets for fast membership
        for tbl, cfg in data.items():
            if "enums" in cfg:
                cfg["enums"] = {col: set(vals) for col, vals in cfg["enums"].items()}
        return data

    def build_logger(self) -> logging.Logger:
        log_path = self.log_dir / f"dq_{self.day_dir.name}.log"
        logger = logging.getLogger(f"DQ-{self.day_dir.name}")
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            # File handler only - no console output for clean pipeline execution
            handler = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=3)
            fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
            handler.setFormatter(fmt)
            logger.addHandler(handler)
            # Removed StreamHandler for clean console output
        logger.info("Initialized DQ logger at %s", log_path)
        return logger

    def read_csv(self, name: str) -> pd.DataFrame:
        path = self.day_dir / f"{name}.csv"
        if not path.exists():
            self.record(
                table=name,
                check="file_exists",
                severity="ERROR",
                passed=False,
                n_affected=1,
                details=f"Missing file: {path}",
            )
            return pd.DataFrame()
        df = pd.read_csv(path)
        self.logger.info("Loaded %s (%d rows)", path.name, len(df))
        return df

    def record(
        self,
        table: str,
        check: str,
        severity: str,
        passed: bool,
        n_affected: int = 0,
        details: str = "",
        samples: Optional[pd.DataFrame] = None,
    ) -> None:
        rec = DQResult(
            table=table,
            check=check,
            severity=severity,
            passed=passed,
            n_affected=int(n_affected),
            details=details
        )
        self.results.append(rec)
        level = logging.INFO if severity == "INFO" else (logging.WARNING if severity == "WARNING" else logging.ERROR)
        msg = f"[{table}] {check} | {'PASS' if passed else 'FAIL'} | affected={n_affected}"
        if details:
            msg += f" | {details}"
        self.logger.log(level, msg)

    def check_required_columns(self, name: str, df: pd.DataFrame, required: Iterable[str]) -> None:
        missing = [c for c in required if c not in df.columns]
        extra = [c for c in df.columns if c not in required]
        self.record(name, "required_columns_missing", "ERROR", len(missing) == 0, len(missing), details=str(missing))
        if extra:
            self.record(name, "extra_columns_present", "INFO", True, len(extra), details=str(extra))

    def check_enums(self, name: str, df: pd.DataFrame, enums: Dict[str, set]) -> None:
        for col, allowed in enums.items():
            if col not in df.columns:
                continue
            bad = ~df[col].isin(list(allowed))
            n_bad = int(bad.sum())
            self.record(name, f"invalid_enum({col})", "ERROR", n_bad == 0, n_bad, details=f"allowed={sorted(allowed)}", samples=df.loc[bad, [col]].drop_duplicates())

    def check_ranges(self, name: str, df: pd.DataFrame, ranges: Dict[str, list] | Dict[str, tuple]) -> None:
        for col, bounds in ranges.items():
            if col not in df.columns:
                continue
            lo, hi = bounds
            bad = (~df[col].between(lo, hi)) | (df[col].isna())
            n_bad = int(bad.sum())
            self.record(name, f"out_of_range({col})", "ERROR", n_bad == 0, n_bad, details=f"expected {lo}-{hi}", samples=df.loc[bad, [col]].head(10))

    def check_non_negative(self, name: str, df: pd.DataFrame, cols: Iterable[str]) -> None:
        for col in cols:
            if col not in df.columns:
                continue
            bad = df[col] < 0
            n_bad = int(bad.sum())
            self.record(name, f"negative_values({col})", "ERROR", n_bad == 0, n_bad, samples=df.loc[bad, [col]])

    def check_dates(self, name: str, df: pd.DataFrame, date_cols: Iterable[str]) -> None:
        for col in date_cols:
            if col not in df.columns:
                continue
            parsed = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")
            bad = parsed.isna()
            n_bad = int(bad.sum())
            self.record(name, f"invalid_date({col})", "ERROR", n_bad == 0, n_bad, samples=df.loc[bad, [col]])

    def check_duplicate_ids(self, name: str, df: pd.DataFrame, id_cols: Iterable[str]) -> None:
        """Detect duplicate identifiers per single column (lightweight check, no PK enforcement)."""
        for col in id_cols:
            if col not in df.columns:
                continue
            dup_mask = df[col].duplicated(keep=False)
            n_dups = int(dup_mask.sum())
            self.record(
                name,
                f"duplicate_id({col})",
                "ERROR",
                n_dups == 0,
                n_dups,
                samples=df.loc[dup_mask, [col]].drop_duplicates().head(10),
            )

    def evaluate_table(self, name: str, check_dup_ids: bool = False) -> None:
        if name not in self.schema:
            self.record(name, "table_in_schema", "ERROR", False, 1, details=f"Table '{name}' missing from schema {self.schema_path}")
            return
        cfg = self.schema[name]
        df = self.read_csv(name)
        self.tables[name] = df

        if df.empty and not (self.day_dir / f"{name}.csv").exists():
            return

        self.check_required_columns(name, df, cfg.get("required", []))
        if enums := cfg.get("enums"):
            self.check_enums(name, df, enums)
        if ranges := cfg.get("ranges"):
            self.check_ranges(name, df, ranges)
        if nn := cfg.get("non_negative", []):
            self.check_non_negative(name, df, nn)
        if dcols := cfg.get("date_cols", []):
            self.check_dates(name, df, dcols)
        if check_dup_ids and (id_cols := cfg.get("dup_id_cols", [])):
            self.check_duplicate_ids(name, df, id_cols)

    def evaluate_all(self, write_report: bool = True, check_dup_ids: bool = False) -> bool:
        table_order = ["applications", "accounts", "transactions", "payments", "delinquency"]
        for t in table_order:
            self.evaluate_table(t, check_dup_ids=check_dup_ids)

        passed = all(r.passed or r.severity == "INFO" for r in self.results)

        if write_report:
            self.write_report()
        self.logger.info("DQ summary: %s", "PASS" if passed else "FAIL")
        return passed

    def write_report(self) -> None:
        out_dir = Path("quality_output")
        out_dir.mkdir(parents=True, exist_ok=True)
        report_json = out_dir / f"dq_report_{self.day_dir.name}.json"
        with report_json.open("w", encoding="utf-8") as f:
            json.dump([asdict(r) for r in self.results], f, indent=2)
        rows = [asdict(r) for r in self.results]
        pd.DataFrame(rows).to_csv(out_dir / f"dq_report_{self.day_dir.name}.csv", index=False)
        self.logger.info("Wrote DQ report to %s", report_json)

