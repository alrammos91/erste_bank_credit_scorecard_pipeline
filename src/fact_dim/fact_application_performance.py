import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, Any

class ApplicationPerformanceBuilder:
    """Builds the main performance fact table for scorecard analysis."""
    
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.con: sqlite3.Connection | None = None
    
    def connect(self) -> sqlite3.Connection:
        """Connect to database."""
        if self.con is None:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self.con = sqlite3.connect(self.db_path)
        return self.con
    
    def close(self) -> None:
        """Close database connection."""
        if self.con:
            self.con.close()
            self.con = None
    
    def build_performance_table(self, run_date: str) -> pd.DataFrame:
        """
        Build comprehensive performance table by joining all data sources.
        
        This creates one row per application with:
        - Application details (scorecard, decision, scores)
        - Account activation status
        - 30-day transaction/payment behavior
        - Delinquency status
        - Reference table context (product details, channel info, etc.)
        """
        
        performance_query = f"""
        WITH transaction_summary AS (
            SELECT 
                acc.application_id,
                COUNT(txn.transaction_id) as txn_count_30d,
                COALESCE(SUM(txn.amount), 0) as txn_amount_30d,
                COALESCE(AVG(txn.amount), 0) as avg_txn_amount
            FROM clean_accounts acc
            LEFT JOIN clean_transactions txn ON acc.account_id = txn.account_id
            WHERE acc._run_date = '{run_date}'
            GROUP BY acc.application_id
        ),
        
        payment_summary AS (
            SELECT 
                acc.application_id,
                COUNT(pmt.payment_id) as pmt_count_30d,
                COALESCE(SUM(pmt.amount), 0) as pmt_amount_30d,
                COALESCE(AVG(pmt.amount), 0) as avg_pmt_amount
            FROM clean_accounts acc
            LEFT JOIN clean_payments pmt ON acc.account_id = pmt.account_id
            WHERE acc._run_date = '{run_date}'
            GROUP BY acc.application_id
        ),
        
        delinquency_summary AS (
            SELECT 
                acc.application_id,
                COALESCE(delinq.days_past_due, 0) as days_past_due,
                COALESCE(delinq.default_flag, 0) as default_flag
            FROM clean_accounts acc
            LEFT JOIN clean_delinquency delinq ON acc.account_id = delinq.account_id
            WHERE acc._run_date = '{run_date}'
        )
        
        SELECT 
            -- Core Application Data
            app.application_id,
            app.scorecard_version,
            app.decision,
            app.bureau_score,
            app.product,
            app.channel,
            app.segment,
            
            -- Account Activation
            acc.account_id,
            acc.activation_date,
            CASE WHEN acc.account_id IS NOT NULL THEN 1 ELSE 0 END as activated_flag,
            
            -- Transaction Behavior (30 days)
            COALESCE(txn.txn_count_30d, 0) as txn_count_30d,
            COALESCE(txn.txn_amount_30d, 0) as txn_amount_30d,
            COALESCE(txn.avg_txn_amount, 0) as avg_txn_amount,
            
            -- Payment Behavior (30 days)
            COALESCE(pmt.pmt_count_30d, 0) as pmt_count_30d,
            COALESCE(pmt.pmt_amount_30d, 0) as pmt_amount_30d,
            COALESCE(pmt.avg_pmt_amount, 0) as avg_pmt_amount,
            
            -- Risk Metrics
            COALESCE(delinq.days_past_due, 0) as days_past_due,
            COALESCE(delinq.default_flag, 0) as default_flag,
            
            -- Calculated Business Metrics
            CASE 
                WHEN txn.txn_amount_30d > 0 AND pmt.pmt_amount_30d > 0 
                THEN ROUND(pmt.pmt_amount_30d / txn.txn_amount_30d, 3)
                ELSE 0 
            END as payment_ratio,
            
            CASE 
                WHEN app.decision = 'approved' AND acc.account_id IS NOT NULL THEN 1 
                ELSE 0 
            END as activation_success,
            
            -- Audit Fields
            app._run_date
            
        FROM clean_applications app
        LEFT JOIN clean_accounts acc ON app.application_id = acc.application_id 
            AND app._run_date = acc._run_date
        LEFT JOIN transaction_summary txn ON app.application_id = txn.application_id
        LEFT JOIN payment_summary pmt ON app.application_id = pmt.application_id
        LEFT JOIN delinquency_summary delinq ON app.application_id = delinq.application_id
        
        WHERE app._run_date = '{run_date}'
        ORDER BY app.application_id
        """
        
        try:
            self.connect()
            performance_df = pd.read_sql_query(performance_query, self.con)
            print(f"Built performance table with {len(performance_df)} applications")
            return performance_df
            
        except sqlite3.Error as e:
            print(f"Error building performance table: {e}")
            raise
    
    def create_enriched_performance_table(self, run_date: str) -> pd.DataFrame:
        """
        Create enriched performance table with reference data joined.
        
        This adds business context from reference tables:
        - Product details (fees, limits, rewards)
        - Channel information (costs, conversion rates)
        - Segment data (demographics, risk profiles)
        - Scorecard metadata (thresholds, expected performance)
        """
        
        # First get base performance table
        base_performance = self.build_performance_table(run_date)
        
        # Add reference data joins
        enriched_query_simple = f"""
        SELECT 
            perf.*,
            
            -- Product details
            prod.product_name,
            prod.annual_fee,
            prod.interest_rate,
            prod.credit_limit_min,
            prod.credit_limit_max,
            prod.rewards_rate,
            
            -- Channel details
            ch.channel_name,
            ch.cost_per_acquisition,
            ch.conversion_rate as channel_conversion_rate,
            
            -- Segment details
            seg.segment_name,
            seg.risk_profile,
            seg.target_approval_rate as segment_target_approval_rate,
            
            -- Scorecard details
            sc.scorecard_name,
            sc.model_type,
            sc.approval_threshold,
            sc.expected_default_rate
            
        FROM application_performance perf
        LEFT JOIN ref_products prod ON perf.product = prod.product_code
        LEFT JOIN ref_channels ch ON perf.channel = ch.channel_code
        LEFT JOIN ref_segments seg ON perf.segment = seg.segment_code
        LEFT JOIN ref_scorecards sc ON perf.scorecard_version = sc.scorecard_version
        WHERE perf._run_date = '{run_date}'
        ORDER BY perf.application_id
        """
        
        try:
            self.connect()
            
            # First save base performance table
            base_performance.to_sql('application_performance', self.con, if_exists='replace', index=False)
            
            # Then create enriched version
            enriched_df = pd.read_sql_query(enriched_query_simple, self.con)
            print(f"Built enriched performance table with {len(enriched_df)} applications")
            return enriched_df
            
        except sqlite3.Error as e:
            print(f"Error building enriched performance table: {e}")
            return base_performance  # Return base version if enrichment fails
    
    def save_performance_table(self, run_date: str, table_name: str = "application_performance") -> Dict[str, Any]:
        """Build and save the performance table to database."""
        try:
            performance_df = self.build_performance_table(run_date)
            
            self.connect()
            performance_df.to_sql(table_name, self.con, if_exists='replace', index=False)
            self.con.commit()
            
            return {
                "status": "success",
                "message": f"Performance table saved with {len(performance_df)} records",
                "record_count": len(performance_df),
                "columns": list(performance_df.columns)
            }
            
        except sqlite3.Error as e:
            return {
                "status": "error",
                "message": f"Failed to save performance table: {str(e)}"
            }
