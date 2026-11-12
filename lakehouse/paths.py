import os
from pathlib import Path

PROJECT_DIR = Path(os.environ["LAKE_PROJECT_DIR"])
DATA_DIR = Path(os.environ["LAKE_DATA_DIR"])
RAW_DIR = DATA_DIR / "raw"
LAKE_DIR = DATA_DIR / "lake"
BRONZE = LAKE_DIR / "bronze_complaints"
SILVER = LAKE_DIR / "silver_complaints"
QUARANTINE = LAKE_DIR / "quarantine_complaints"
GOLD_DIR = LAKE_DIR / "gold"
SAMPLE_DIR = PROJECT_DIR / "sample"
