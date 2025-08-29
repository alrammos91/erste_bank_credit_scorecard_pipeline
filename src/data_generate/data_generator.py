import pandas as pd
import numpy as np
from faker import Faker
import yaml
from datetime import datetime, date, timedelta
from pathlib import Path

class EsteDataGenerator:
    """
    Generates ONE day of synthetic data under erste_bank_data/YYYY-MM-DD/:

    applications.csv : application_id, scorecard_version, decision, bureau_score, product, channel, segment
    accounts.csv     : account_id, application_id, activation_date
    transactions.csv : transaction_id, account_id, transaction_date, amount
    payments.csv     : payment_id, account_id, payment_date, amount
    delinquency.csv  : account_id, days_past_due, default_flag

    Assumptions - Notes:
    - Extra columns in applications (product/channel/segment) are optional but useful for dashboard filters.
    - Accounts are created ONLY for approved applications (a subset of applications).
    - Transactions, payments, and delinquency are generated per active account.
    """
    def __init__(self, n_apps: int = 200, seed: int = 42, overwrite: bool = False):
        self.fake = Faker()
        self.run_date: str = datetime.today().strftime("%Y-%m-%d")
        self.n_apps = n_apps
        self.seed = seed
        self.config_path: str = "config/pipeline_config.yaml"
        self.overwrite = overwrite

        # Create a directory for the given run date under /data
        self.day_dir = Path("erste_bank_data") / self.run_date
        self.day_dir.mkdir(parents=True, exist_ok=True)

        # Load YAML configuration
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
    def random_date_in_range(self, start: date, end: date) -> str:
        delta = (end - start).days
        return (start + timedelta(days=int(np.random.randint(0, max(delta, 0) + 1)))).strftime("%Y-%m-%d")

    def generate_applications(self) -> pd.DataFrame:
        """Generate synthetic credit card applications."""
        products = self.config['dimensions']['products']
        channels = self.config['dimensions']['channels']
        segments = self.config['dimensions']['segments']
        scorecard_versions = self.config['dimensions']['scorecard_versions']
        apps = []
        for _ in range(self.n_apps):
            apps.append({
                "application_id": self.fake.uuid4(), # unique identifier of each application
                "scorecard_version": np.random.choice(scorecard_versions, p=[0.5, 0.5]),
                "decision": np.random.choice(["approved", "declined"], p=[0.7, 0.3]),
                "bureau_score": np.random.randint(300, 850),
                "product": np.random.choice(products, p=[0.5, 0.35, 0.15]),
                "channel": np.random.choice(channels, p=[0.7, 0.3]),
                "segment": np.random.choice(segments, p=[0.85, 0.15])
            })

        applications = pd.DataFrame(apps)
        return applications

    def generate_accounts(self, applications: pd.DataFrame = None) -> pd.DataFrame:
        """Generate accounts only for approved applications."""
        if applications is None:
            applications = self.generate_applications()
        approved = applications[applications["decision"] == "approved"]
        accounts = []
        
        run_dt = datetime.strptime(self.run_date, "%Y-%m-%d").date()
        start_dt = run_dt - timedelta(days=3)  # activation within 3 days before run_date
        for _, row in approved.iterrows():
            # 80% chance that an approved application leads to an active account
            if np.random.rand() < 0.8:
                accounts.append({
                    "account_id": self.fake.uuid4(),
                    "application_id": row["application_id"],
                    "activation_date": self.random_date_in_range(start_dt, run_dt)
                })
                
        accounts = pd.DataFrame(accounts)
        return accounts

    def generate_transactions(self, accounts: pd.DataFrame = None) -> pd.DataFrame:
        """Generate random transactions for active accounts."""
        if accounts is None:
            accounts = self.generate_accounts()
        
        run_dt = datetime.strptime(self.run_date, "%Y-%m-%d").date()
        start_dt = run_dt - timedelta(days=30)  # last 30 days

        transactions = []
        for _, row in accounts.iterrows():
            for _ in range(np.random.randint(1, 5)): # 1–4 transactions per account
                transactions.append({
                    "transaction_id": self.fake.uuid4(),
                    "account_id": row["account_id"],
                    "transaction_date": self.random_date_in_range(start_dt, run_dt),
                    "amount": round(abs(np.random.normal(100, 50)), 2) # normally distributed spend
                })

        transactions = pd.DataFrame(transactions)
        return transactions
    
    def generate_payments(self, accounts: pd.DataFrame = None) -> pd.DataFrame:
        """Generate random payments for active accounts."""
        if accounts is None:
            accounts = self.generate_accounts()

        run_dt = datetime.strptime(self.run_date, "%Y-%m-%d").date()
        start_dt = run_dt - timedelta(days=30)

        payments = []
        for _, row in accounts.iterrows():
            for _ in range(np.random.randint(1, 4)): # 1–3 payments per account
                payments.append({
                    "payment_id": self.fake.uuid4(),
                    "account_id": row["account_id"],
                    "payment_date": self.random_date_in_range(start_dt, run_dt),
                    "amount": round(abs(np.random.normal(80, 40)), 2) # normally distributed payments
                })
        payments = pd.DataFrame(payments)
        return payments
    
    def generate_delinquency(self, accounts: pd.DataFrame = None) -> pd.DataFrame:
        """Generate delinquency info for accounts (days past due + default flag)."""
        if accounts is None:
            accounts = self.generate_accounts()

        delinquency = []
        for _, row in accounts.iterrows():
            delinquency.append({
                "account_id": row["account_id"],
                "days_past_due": np.random.choice([0, 30, 60, 90], p=[0.85, 0.1, 0.04, 0.01]),
                "default_flag": np.random.choice([0, 1], p=[0.9, 0.1]) # 10% chance of default
            })

        delinquency = pd.DataFrame(delinquency)
        return delinquency
    
    def generate_and_save_all(self) -> dict:
        """Generate all data consistently and save to CSV files."""
        # if self.day_dir.exists() and not self.overwrite:
        #     raise FileExistsError(f"{self.day_dir} already exists; pass overwrite=True to replace")
        print(f"Generating synthetic data for {self.run_date} with {self.n_apps} applications...")
        
        # Generate data in proper sequence
        applications = self.generate_applications()
        accounts = self.generate_accounts(applications)
        transactions = self.generate_transactions(accounts)
        payments = self.generate_payments(accounts)
        delinquency = self.generate_delinquency(accounts)
        
        # Print summary stats
        print(f"Generated: ")
        print(f" {len(applications)} applications ({len(applications[applications['decision']=='approved'])} approved)")
        print(f" {len(accounts)} active accounts")
        print(f" {len(transactions)} transactions")
        print(f" {len(payments)} payments")
        
        # Save all data to CSV files
        applications.to_csv(self.day_dir / "applications.csv", index=False)
        accounts.to_csv(self.day_dir / "accounts.csv", index=False)
        transactions.to_csv(self.day_dir / "transactions.csv", index=False)
        payments.to_csv(self.day_dir / "payments.csv", index=False)
        delinquency.to_csv(self.day_dir / "delinquency.csv", index=False)
        print(f"Saved CSVs under {self.day_dir}")