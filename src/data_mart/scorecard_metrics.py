import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass
# from config.config_loader import PipelineConfig  # For future YAML integration


@dataclass
class ScorecardMetrics:
    """Data class to hold the 6 core scorecard performance metrics."""
    scorecard_version: str
    run_date: str
    total_applications: int          # Number of applications
    approval_rate: float             # Approval rate (%)
    activation_rate: float           # Activation rate (%)
    avg_credit_score: float          # Average credit score
    default_rate: float              # Default rate (%)
    avg_spend_amount: float          # Spend behavior (30-day avg)
    avg_payment_amount: float        # Payment behavior (30-day avg)


class ScorecardMetricsCalculator:
    """Calculate and compare performance metrics between scorecards."""
    
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.con: sqlite3.Connection | None = None
        # self.config = PipelineConfig(config_path)  # For future YAML integration
    
    def connect(self) -> sqlite3.Connection:
        if self.con is None:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self.con = sqlite3.connect(self.db_path)
        return self.con
    
    def close(self) -> None:
        if self.con:
            self.con.close()
            self.con = None
    
    def calculate_scorecard_metrics(self, run_date: str, scorecard_version: str) -> ScorecardMetrics:
        """Calculate the core metrics for a specific scorecard version."""
        
        metrics_query = f"""
        SELECT 
            -- 1. Number of applications
            COUNT(*) as total_applications,
            
            -- 2. Approval rate (%)
            AVG(CASE WHEN decision = 'approved' THEN 1.0 ELSE 0.0 END) * 100 as approval_rate,
            
            -- 3. Activation rate (% of approved who activated)
            AVG(CASE WHEN decision = 'approved' THEN activated_flag ELSE NULL END) * 100 as activation_rate,
            
            -- 4. Average credit score
            AVG(bureau_score) as avg_credit_score,
            
            -- 5. Default rate (%)
            AVG(default_flag) * 100 as default_rate,
            
            -- 6a. Spend behavior (30-day average for activated accounts)
            AVG(CASE WHEN activated_flag = 1 THEN txn_amount_30d ELSE NULL END) as avg_spend_amount,
            
            -- 6b. Payment behavior (30-day average for activated accounts)
            AVG(CASE WHEN activated_flag = 1 THEN pmt_amount_30d ELSE NULL END) as avg_payment_amount
            
        FROM application_performance 
        WHERE _run_date = '{run_date}' 
        AND scorecard_version = '{scorecard_version}'
        """
        
        try:
            self.connect()
            result = pd.read_sql_query(metrics_query, self.con)
            row = result.iloc[0]
            
            return ScorecardMetrics(
                scorecard_version=scorecard_version,
                run_date=run_date,
                total_applications=int(row['total_applications']),
                approval_rate=round(float(row['approval_rate']), 2),
                activation_rate=round(float(row['activation_rate'] or 0), 2),
                avg_credit_score=round(float(row['avg_credit_score']), 1),
                default_rate=round(float(row['default_rate']), 2),
                avg_spend_amount=round(float(row['avg_spend_amount'] or 0), 2),
                avg_payment_amount=round(float(row['avg_payment_amount'] or 0), 2)
            )
            
        except sqlite3.Error as e:
            print(f"Error calculating metrics for {scorecard_version}: {e}")
            raise
    
    def generate_scorecard_comparison_csv(self, run_date: str, output_dir: str = "output") -> str:
        """Generate CSV with calculated metrics by Product/Channel/Segment for filtering."""
        
        try:
            # Create output directory
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Calculate metrics for each combination using SQL GROUP BY
            self.connect()
            metrics_query = f"""
            SELECT 
                '{run_date}' as run_date,
                scorecard_version,
                product,
                channel,
                segment,
                
                -- 1. Number of applications
                COUNT(*) as total_applications,
                
                -- 2. Approval rate (%)
                ROUND(AVG(CASE WHEN decision = 'approved' THEN 1.0 ELSE 0.0 END) * 100, 2) as approval_rate_pct,
                
                -- 3. Activation rate (% of approved who activated)
                ROUND(AVG(CASE WHEN decision = 'approved' THEN activated_flag ELSE NULL END) * 100, 2) as activation_rate_pct,
                
                -- 4. Average credit score
                ROUND(AVG(bureau_score), 1) as avg_credit_score,
                
                -- 5. Default rate (%)
                ROUND(AVG(default_flag) * 100, 2) as default_rate_pct,
                
                -- 6a. Spend behavior (30-day average for activated accounts)
                ROUND(AVG(CASE WHEN activated_flag = 1 THEN txn_amount_30d ELSE NULL END), 2) as avg_spend_amount_30d,
                
                -- 6b. Payment behavior (30-day average for activated accounts)
                ROUND(AVG(CASE WHEN activated_flag = 1 THEN pmt_amount_30d ELSE NULL END), 2) as avg_payment_amount_30d
                
            FROM application_performance 
            WHERE _run_date = '{run_date}'
            GROUP BY scorecard_version, product, channel, segment
            ORDER BY scorecard_version, product, channel, segment
            """
            
            df = pd.read_sql_query(metrics_query, self.con)
            
            # Save metrics CSV for filtering
            csv_file = output_path / f"scorecard_metrics_{run_date}.csv"
            df.to_csv(csv_file, index=False)
            
            print(f"Scorecard metrics CSV generated: {csv_file}")
            return str(csv_file)
            
        except Exception as e:
            print(f"Error generating metrics CSV: {e}")
            raise
