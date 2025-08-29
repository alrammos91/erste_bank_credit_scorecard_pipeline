"""
Recovery Manager for step-level pipeline execution tracking and re-run capabilities.
Integrates with existing audit infrastructure.
"""
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional
import logging


class StepStatus(Enum):
    STARTED = "started"
    COMPLETED = "completed" 
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineStep:
    step_name: str
    function: callable
    dependencies: List[str] = None
    cleanup_function: callable = None
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []


@dataclass
class StepExecution:
    batch_id: str
    step_name: str
    status: StepStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None


class RecoveryManager:
    """Manages step-level execution tracking and recovery for the pipeline."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.logger = logging.getLogger(__name__)
        self.setup_execution_tracking()
    
    def connect(self):
        """Connect to database and ensure tracking tables exist."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def setup_execution_tracking(self):
        """Create execution tracking table if it doesn't exist."""
        if not self.conn:
            self.connect()
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_execution_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL,
                step_name TEXT NOT NULL,
                status TEXT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (batch_id) REFERENCES audit_runs(batch_id)
            )
        """)
        self.conn.commit()
    
    def start_step(self, batch_id: str, step_name: str) -> None:
        """Record step start."""
        self.conn.execute("""
            INSERT INTO pipeline_execution_log 
            (batch_id, step_name, status, start_time)
            VALUES (?, ?, ?, ?)
        """, (batch_id, step_name, StepStatus.STARTED.value, datetime.now()))
        self.conn.commit()
        self.logger.info(f"Started step: {step_name}")
    
    def complete_step(self, batch_id: str, step_name: str) -> None:
        """Record step completion."""
        self.conn.execute("""
            UPDATE pipeline_execution_log 
            SET status = ?, end_time = ?
            WHERE batch_id = ? AND step_name = ? AND status = ?
        """, (StepStatus.COMPLETED.value, datetime.now(), batch_id, step_name, StepStatus.STARTED.value))
        self.conn.commit()
        self.logger.info(f"Completed step: {step_name}")
    
    def fail_step(self, batch_id: str, step_name: str, error_message: str) -> None:
        """Record step failure."""
        self.conn.execute("""
            UPDATE pipeline_execution_log 
            SET status = ?, end_time = ?, error_message = ?
            WHERE batch_id = ? AND step_name = ? AND status = ?
        """, (StepStatus.FAILED.value, datetime.now(), error_message, batch_id, step_name, StepStatus.STARTED.value))
        self.conn.commit()
        self.logger.error(f"Failed step: {step_name} - {error_message}")
    
    def get_batch_status(self, batch_id: str) -> List[StepExecution]:
        """Get execution status for all steps in a batch."""
        cursor = self.conn.execute("""
            SELECT batch_id, step_name, status, start_time, end_time, error_message
            FROM pipeline_execution_log
            WHERE batch_id = ?
            ORDER BY start_time
        """, (batch_id,))
        
        results = []
        for row in cursor.fetchall():
            results.append(StepExecution(
                batch_id=row[0],
                step_name=row[1], 
                status=StepStatus(row[2]),
                start_time=datetime.fromisoformat(row[3]),
                end_time=datetime.fromisoformat(row[4]) if row[4] else None,
                error_message=row[5]
            ))
        return results
    
    def get_failed_step(self, batch_id: str) -> Optional[str]:
        """Get the first failed step name for a batch."""
        cursor = self.conn.execute("""
            SELECT step_name FROM pipeline_execution_log
            WHERE batch_id = ? AND status = ?
            ORDER BY start_time
            LIMIT 1
        """, (batch_id, StepStatus.FAILED.value))
        
        result = cursor.fetchone()
        return result[0] if result else None
    
    def get_completed_steps(self, batch_id: str) -> List[str]:
        """Get list of completed step names for a batch."""
        cursor = self.conn.execute("""
            SELECT step_name FROM pipeline_execution_log
            WHERE batch_id = ? AND status = ?
            ORDER BY start_time
        """, (batch_id, StepStatus.COMPLETED.value))
        
        return [row[0] for row in cursor.fetchall()]
    
    def cleanup_partial_execution(self, batch_id: str, from_step: str = None) -> None:
        """Clean up data from failed/partial execution."""
        # This would integrate with your existing cleanup logic
        # For now, log the cleanup action
        if from_step:
            self.logger.info(f"Cleaning up data from step: {from_step} for batch: {batch_id}")
        else:
            self.logger.info(f"Cleaning up all data for batch: {batch_id}")
        
        # Example cleanup - remove Bronze/Silver/Gold data for this batch
        # You'd implement this based on your data model
        tables_to_cleanup = [
            "bronze_applications", "bronze_accounts", "bronze_transactions", 
            "bronze_payments", "bronze_delinquency",
            "silver_applications", "silver_accounts", "silver_transactions",
            "silver_payments", "silver_delinquency", 
            "fact_application_performance"
        ]
        
        for table in tables_to_cleanup:
            try:
                self.conn.execute(f"DELETE FROM {table} WHERE _batch_id = ?", (batch_id,))
                self.logger.info(f"Cleaned up {table} for batch {batch_id}")
            except sqlite3.OperationalError:
                # Table might not exist yet
                pass
        
        self.conn.commit()
