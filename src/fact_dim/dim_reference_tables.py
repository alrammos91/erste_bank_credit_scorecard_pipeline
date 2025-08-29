import sqlite3
from pathlib import Path
from typing import Dict, Optional

# Reference tables DDL
REFERENCE_DDL = """
CREATE TABLE IF NOT EXISTS ref_products (
  product_code TEXT PRIMARY KEY,
  product_name TEXT,
  annual_fee REAL,
  interest_rate REAL,
  credit_limit_min INTEGER,
  credit_limit_max INTEGER,
  rewards_rate REAL,
  foreign_transaction_fee REAL,
  target_segment TEXT,
  launch_date TEXT
);

CREATE TABLE IF NOT EXISTS ref_channels (
  channel_code TEXT PRIMARY KEY,
  channel_name TEXT,
  cost_per_acquisition REAL,
  conversion_rate REAL,
  processing_time_days INTEGER,
  customer_service_rating REAL,
  marketing_budget_pct REAL
);

CREATE TABLE IF NOT EXISTS ref_segments (
  segment_code TEXT PRIMARY KEY,
  segment_name TEXT,
  income_min INTEGER,
  income_max INTEGER,
  age_min INTEGER,
  age_max INTEGER,
  risk_profile TEXT,
  marketing_budget_pct REAL,
  target_approval_rate REAL
);

CREATE TABLE IF NOT EXISTS ref_scorecards (
  scorecard_version TEXT PRIMARY KEY,
  scorecard_name TEXT,
  model_type TEXT,
  approval_threshold INTEGER,
  launch_date TEXT,
  target_approval_rate REAL,
  expected_default_rate REAL,
  model_features TEXT
);
"""

# Reference data
REFERENCE_DATA = {
    'ref_products': [
        ('standard', 'Standard Credit Card', 0.00, 18.99, 500, 5000, 1.0, 2.50, 'mass_market', '2020-01-01'),
        ('gold', 'Gold Rewards Card', 95.00, 16.99, 2000, 15000, 1.5, 0.00, 'affluent', '2021-03-15'),
        ('platinum', 'Platinum Elite Card', 450.00, 14.99, 5000, 50000, 2.0, 0.00, 'high_net_worth', '2022-06-01'),
    ],
    'ref_channels': [
        ('online', 'Digital Banking', 50.00, 0.15, 1, 4.2, 0.70),
        ('branch', 'Branch Network', 150.00, 0.25, 0, 4.8, 0.30),
    ],
    'ref_segments': [
        ('retail', 'Retail Banking', 25000, 150000, 18, 65, 'medium', 0.65, 0.70),
        ('student', 'Student Banking', 0, 35000, 18, 25, 'high', 0.35, 0.60),
    ],
    'ref_scorecards': [
        ('existing', 'Legacy Scorecard v2.1', 'logistic_regression', 650, '2019-01-01', 0.70, 0.08, 'bureau_score,income,debt_ratio'),
        ('pilot', 'ML Pilot Scorecard v3.0', 'gradient_boosting', 620, '2025-07-01', 0.75, 0.06, 'bureau_score,income,debt_ratio,transaction_history,social_media'),
    ]
}

class ReferenceTablesManager:
    """Manages reference/lookup tables for business context."""
    
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.con: sqlite3.Connection | None = None
    
    def connect(self) -> sqlite3.Connection:
        if not self.con:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self.con = sqlite3.connect(self.db_path)
        return self.con
    
    def close(self) -> None:
        if self.con:
            self.con.close()
            self.con = None
    
    def setup_reference_tables(self) -> None:
        """Create and populate reference tables."""
        assert self.con is not None
        
        # Create tables
        self.con.executescript(REFERENCE_DDL)
        
        # Insert reference data
        for table_name, data in REFERENCE_DATA.items():
            # Clear existing data
            self.con.execute(f"DELETE FROM {table_name}")
            
            # Insert new data
            placeholders = ','.join(['?' for _ in data[0]])
            self.con.executemany(
                f"INSERT INTO {table_name} VALUES ({placeholders})", 
                data
            )
        
        self.con.commit()
        print("Reference tables created and populated")
    
