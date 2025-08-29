# Erste Bank Scorecard — Demo Data Pipeline (OOP, SQLite, Pandas)
A data engineering project that simulates daily banking data, implements basic quality, stages raw files, builds a typed clean layer, and produces a per-application performance table enriched with reference data along with some metrics. These are stored in a daily csv report file.

---

### **Project Structure**

```
erste_bank_credit_scorecard_pipeline/
├── src/
│   ├── common/                     # Shared utilities (audit)
│   │   ├── audit.py                    # AuditRepository class
│   │   └── __init__.py
│   ├── data_generate/              # Synthetic data generation
│   │   ├── data_generator.py           # EsteDataGenerator class
│   │   └── __init__.py
│   ├── data_quality/               # Validation framework
│   │   ├── data_quality_checks.py      # DataQualityChecker class
│   │   └── __init__.py
│   ├── data_load/                  # Staging layer
│   │   ├── data_staging.py             # SQLiteStagingLoader class
│   │   └── __init__.py
│   ├── data_clean/                 # Cleaning layer
│   │   ├── data_cleaning.py            # SQLiteCleaner class
│   │   └── __init__.py
│   ├── fact_dim/                   # Fact + Dimensions layer (business tables)
│   │   ├── dim_reference_tables.py     # ReferenceTablesManager class
│   │   ├── fact_application_performance.py  # ApplicationPerformanceBuilder class
│   │   └── __init__.py
│   ├── data_mart/                  # Metrics calculation
│   │   ├── scorecard_metrics.py        # ScorecardMetricsCalculator class
│   │   └── __init__.py
│   └── pipeline_executor.py        # Main orchestration script
├── config/                         # Configuration files
│   ├── data_quality_schema.json       # DQ validation rules
│   └── pipeline_config.yaml           # Business dimensions config
├── output/                         # Business reports
│   └── scorecard_metrics_YYYY-MM-DD.csv
├── quality_output/                 # Data quality reports
│   └── dq_report_YYYY-MM-DD.json
├── logs/                           # Execution logs
│   └── dq_YYYY-MM-DD.log
├── db/                             # SQLite database
│   └── erste_scorecard.db
├── erste_bank_data/               # Daily raw data (partitioned by run date)
│   └── YYYY-MM-DD/
│       ├── applications.csv
│       ├── accounts.csv
│       ├── transactions.csv
│       ├── payments.csv
│       └── delinquency.csv
├── requirements.txt               # Python dependencies
└── README.md                     # This file
```
---

## Features
- **Daily batch** pipeline, partitioned by `run_date` (`YYYY-MM-DD`)
- **Config-driven DQ gate** (required columns, enums, ranges, non-negative, date formats, duplicate IDs)
- **Schema-drift tolerant staging** on SQLite with dynamic column adds
- **Typed, idempotent clean layer** with window-based dedup (latest record wins)
- **Performance mart**: `application_performance` joins clean tables + reference lookups
- **Audit**: run status + per-table load counts; metadata columns `_run_date`, `_source_file`, `_ingested_at`, `_batch_id`

---

## Prerequisites
- Python 3.10+
- SQLite (bundled with Python via `sqlite3`)

### Install dependencies
pip install -r requirements.txt

### Clone the repository
git clone https://github.com/alrammos91/erste_bank_credit_scorecard_pipeline.git
cd erste_bank_credit_scorecard_pipeline

---

### Run the Pipeline
```bash
# Generate 200 applications (default)
python src/pipeline_executor.py

# Custom run with 500 applications
python src/pipeline_executor.py --n-apps 500 --seed 123

```


## **Output Files**

### **Business Metrics**
- **File**: `output/scorecard_metrics_YYYY-MM-DD.csv`
- **Purpose**: Compare pilot vs. existing scorecard performance
- **Usage**: Import to Excel, filter by dimensions, create pivot tables

### **🔍 Data Quality Reports**
- **File**: `quality_output/dq_report_YYYY-MM-DD.json`
- **Purpose**: Comprehensive data validation results
- **Content**: Schema compliance, data integrity checks, error details

### **📝 Execution Logs**
- **File**: `logs/dq_YYYY-MM-DD.log`
- **Purpose**: Detailed pipeline execution tracking
- **Content**: Data quality checks, processing steps, audit information

---

## **Configuration Management**

### **Business Dimensions** (`config/pipeline_config.yaml`)
```yaml
dimensions:
  products:
    - "standard"
    - "gold" 
    - "platinum"
    
  channels:
    - "online"
    - "branch"
    
  segments:
    - "retail"
    - "student"
```


## **Pipeline Workflow**

1. **Data Generation**: Create realistic synthetic credit applications
2. **Quality Validation**: Schema compliance and business rule checks
3. **Stage Layer**: Raw data staging with audit trails
4. **Clean Layer**: Cleaned and standardized data
5. **Fact-Dim Layer**: Business-optimized fact and dimension tables
6. **Metrics Calculation**: 6 scorecard KPIs with dimensional breakdown
7. **Report Generation**: Business-ready CSV for stakeholder analysis

---




