import argparse
import sys
from uuid import uuid4

from common.audit import AuditRepository
from data_generate.data_generator import EsteDataGenerator
from data_quality.data_quality_checks import DataQualityChecker
from data_load.data_staging import SQLiteStagingLoader
from data_clean.data_cleaning import SQLiteCleaner
from fact_dim.dim_reference_tables import ReferenceTablesManager
from fact_dim.fact_application_performance import ApplicationPerformanceBuilder
from data_mart.scorecard_metrics import ScorecardMetricsCalculator


def execute_pipeline() -> None:
    parser = argparse.ArgumentParser(description="Generate daily synthetic Erste Bank data for Credit Score prediction.")
    parser.add_argument("--n-apps", type=int, default=200, help="Number of applications to simulate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (use e.g. 42)")
    parser.add_argument("--schema", default="config/data_quality_schema.json", help="Path to DQ schema JSON file")
    parser.add_argument("--db", default="db/erste_scorecard.db", help="Path to SQLite DB file")
    args = parser.parse_args()

    batch_id = str(uuid4())

    try:

        # Generate synthetic CSVs
        gen = EsteDataGenerator(n_apps=args.n_apps, seed=args.seed)
        gen.generate_and_save_all()

        # Audit
        audit = AuditRepository(args.db)
        audit.connect()
        audit.start_run(batch_id=batch_id, run_date=gen.run_date)

        # run Data Quality
        dq = DataQualityChecker(day_dir=gen.day_dir, schema_path=args.schema)
        dq_passed = dq.evaluate_all(check_dup_ids=True)
        if not dq_passed:
            print("Data quality issues detected - continuing with cleaning")
        else:
            print("Data quality validation passed")

        # Stage to SQLite
        loader = SQLiteStagingLoader(args.db, audit=audit)
        loader.connect()
        loader.load_day_staging(day_dir=gen.day_dir, batch_id=batch_id)
        loader.close()

        # Build clean layer
        cleaner = SQLiteCleaner(args.db)
        cleaner.connect()
        cleaner.build_clean_tables(gen.run_date)
        cleaner.close()

        # Setup reference tables
        ref_manager = ReferenceTablesManager(args.db)
        ref_manager.connect()
        ref_manager.setup_reference_tables()
        ref_manager.close()

        # Build performance fact table
        performance_builder = ApplicationPerformanceBuilder(args.db)
        performance_builder.connect()
        performance_builder.save_performance_table(gen.run_date)
        performance_builder.close()

        # Calculate and compare scorecard metrics
        metrics_calculator = ScorecardMetricsCalculator(args.db)
        metrics_calculator.connect()
        
        # Generate CSV with all dimensions for easy Excel filtering
        metrics_calculator.generate_scorecard_comparison_csv(gen.run_date)
        
        metrics_calculator.close()

        audit.end_run(batch_id, status="success", message="Complete pipeline: data + DQ + staging + clean + fact/dim + metrics")

    except Exception as e:
        audit.end_run(batch_id, status="failed", message=str(e))
    finally:
        audit.close()

if __name__ == "__main__":
    execute_pipeline()
