import pandas as pd
import logging
from pathlib import Path
# ---------------------------------------------------------
#  SETUP
# ---------------------------------------------------------

RAW_PATH = Path("..raw/day1_bakery_sales.csv")
CLEAN_PATH = Path("../clean/day1_clean_bakery_sales.csv")
LOG_PATH = Path("../logs/day1_pipeline.log")

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s - %(lineno)s"
)

# ---------------------------------------------------------
#  CLEANING FUNCTIONS
# ---------------------------------------------------------

def basic_cleaning(df):

    # Strip whitespace from string columns
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # Standardize Item column (Title Case)
    if "Item" in df.columns:
        df["Item"] = df["Item"].str.title()

    # Convert date column
    if "date_time" in df.columns:
        df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce")

    # Add invalid_flag
    df["invalid_flag"] = df.apply(
        lambda row: 1 if (
            pd.isna(row["Transaction"])        # missing ID
            or pd.isna(row["period_day"])      # missing period
        ) else 0,
        axis=1
    )

    return df

print("Starting ETL...")
print("Cleaning done.")
print("Saving file...")
print("ETL Completed Successfully!")



# ---------------------------------------------------------
#  VALIDATION FUNCTION
# ---------------------------------------------------------

def run_validations(df):
    results = {}

    # Missing Transaction IDs
    results["missing_transaction"] = df[df["Transaction"].isna()]

    # Duplicate Transaction values
    dup_ids = df["Transaction"].value_counts()
    results["duplicate_ids"] = dup_ids[dup_ids > 1]

    # Duplicate rows
    results["duplicate_rows"] = df[df["Transaction"].duplicated(keep=False)]

    # invalid_flag = 1 rows
    results["invalid_rows"] = df[df["invalid_flag"] == 1]

    return results


# ---------------------------------------------------------
#  MAIN PIPELINE
# ---------------------------------------------------------

def run_pipeline():
    try:
        logging.info("Pipeline started.")

        # Load raw data
        df = pd.read_csv(RAW_PATH)

        # Clean
        df = basic_cleaning(df)

        # Validations
        checks = run_validations(df)

        # Save clean file
        CLEAN_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(CLEAN_PATH, index=False)

        logging.info("Pipeline completed successfully.")
        print("ETL Pipeline Successful!")

        return df, checks

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        print("Pipeline failed. Check logs/day1_pipeline.log.")


# ---------------------------------------------------------
#  EXECUTE
# ---------------------------------------------------------
## It is a Python entry-point that ensures the pipeline runs only when the file is executed directly and not when it is imported.
# This makes your ETL scripts modular, reusable, and prevents accidental execution.
if __name__ == "__main__":
    run_pipeline()