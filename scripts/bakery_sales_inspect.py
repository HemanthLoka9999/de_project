import pandas as pd
import logging
from pathlib import Path

# ---------------------------------------------------------
#  SETUP
# ---------------------------------------------------------

RAW_PATH = Path("../raw/day1_bakery_sales.csv")
CLEAN_PATH = Path("../clean/day1_clean_bakery_sales.csv")
LOG_PATH = Path("../logs/day1_pipeline.log")

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s - %(lineno)d"
)

# ---------------------------------------------------------
#  CLEANING FUNCTIONS
# ---------------------------------------------------------

def trim_whitespace(df):
    """Trim whitespace from all object columns"""
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())
    return df

def standardize_case(df, title_case_cols=None):
    """
    Convert columns to title case.
    title_case_cols: list of column names to apply title case
    """
    if title_case_cols:
        for col in title_case_cols:
            if col in df.columns:
                df[col] = df[col].str.title()
    return df

def convert_columns(df, date_cols=None, numeric_cols=None):
    """Convert specified columns to datetime or numeric"""
    if date_cols:
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
    if numeric_cols:
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def handle_duplicates(df, subset_cols=None, keep="first"):
    """Drop duplicates based on subset of columns"""
    if subset_cols:
        df = df.drop_duplicates(subset=subset_cols, keep=keep)
    else:
        df = df.drop_duplicates(keep=keep)
    return df

def flag_invalid_rows(df, invalid_rules):
    """
    Add invalid_flag based on rules.
    invalid_rules: dict of {column_name: condition_function}
    """
    df["invalid_flag"] = 0
    for col, func in invalid_rules.items():
        if col in df.columns:
            df.loc[func(df[col]), "invalid_flag"] = 1
    return df

# ---------------------------------------------------------
#  VALIDATION FUNCTION
# ---------------------------------------------------------

def run_validations(df, id_col=None):
    results = {}

    if id_col and id_col in df.columns:
        # Missing IDs
        results["missing_ids"] = df[df[id_col].isna()]

        # Duplicate IDs
        dup_ids = df[id_col].value_counts()
        results["duplicate_ids"] = dup_ids[dup_ids > 1]

        # Duplicate rows
        results["duplicate_rows"] = df[df.duplicated(keep=False)]

    # Invalid rows
    if "invalid_flag" in df.columns:
        results["invalid_rows"] = df[df["invalid_flag"] == 1]

    return results

# ---------------------------------------------------------
#  MAIN PIPELINE
# ---------------------------------------------------------

def run_pipeline(
    raw_path,
    clean_path,
    title_case_cols=None,
    date_cols=None,
    numeric_cols=None,
    dup_subset_cols=None,
    invalid_rules=None,
    id_col=None
):
    try:
        logging.info(f"Pipeline started for {raw_path.name}")

        # Load raw data
        df = pd.read_csv(raw_path)

        # Clean data
        df = trim_whitespace(df)
        df = standardize_case(df, title_case_cols)
        df = convert_columns(df, date_cols, numeric_cols)
        df = handle_duplicates(df, dup_subset_cols)
        if invalid_rules:
            df = flag_invalid_rows(df, invalid_rules)

        # Run validations
        checks = run_validations(df, id_col=id_col)

        # Save clean file
        clean_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(clean_path, index=False)

        logging.info(f"Pipeline completed successfully for {raw_path.name}")
        print(f"ETL Pipeline Successful for {raw_path.name}!")

        return df, checks

    except Exception as e:
        logging.error(f"Pipeline failed for {raw_path.name}: {e}")
        print(f"Pipeline failed. Check logs for {raw_path.name}.")

# ---------------------------------------------------------
#  EXECUTE
# ---------------------------------------------------------
if __name__ == "__main__":
    df, checks = run_pipeline(
        raw_path=RAW_PATH,
        clean_path=CLEAN_PATH,
        title_case_cols=["Item"],
        date_cols=["date_time"],
        numeric_cols=None,
        dup_subset_cols=["Transaction"],
        invalid_rules={
            "Transaction": lambda col: col.isna(),
            "period_day": lambda col: col.isna()
        },
        id_col="Transaction"
    )
