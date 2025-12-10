import pandas as pd
import logging
from pathlib import Path
import sqlite3

# ---------------------------------------------------------
#  SETUP
# ---------------------------------------------------------
RAW_PATH = Path("../raw/day2_online_retail_II.xlsx")
CLEAN_PATH = Path("../clean/day2_clean_online_retail_II.csv")
LOG_PATH = Path("../logs/day2_pipeline.log")

# Ensure folders exist
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
CLEAN_PATH.parent.mkdir(parents=True, exist_ok=True)

# Logging config
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s - %(lineno)d"
)

# ---------------------------------------------------------
#  CLEANING FUNCTION
# ---------------------------------------------------------
def cleaning(df):
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    if "Country" in df.columns:
        df["Country"] = df["Country"].str.upper().str.strip()
    if "Description" in df.columns:
        df["Description"] = df["Description"].str.title().str.strip()
    if "StockCode" in df.columns:
        df["StockCode"] = df["StockCode"].str.upper().str.strip()

    if "InvoiceDate" in df.columns:
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
 
    df["invalid_flag"] = df["Customer ID"].isnull().astype(int) if "Customer ID" in df.columns else 0
    df = df.drop_duplicates(subset=['Invoice', 'StockCode'], keep='first')
 
    return df

# ---------------------------------------------------------
#  PYTHON VALIDATION FUNCTION
# ---------------------------------------------------------
def run_validations(df):
    results = {}
    results["missing_Customer_IDs"] = df[df["Customer ID"].isna()]
    results["key_dupes"] = df[df.duplicated(subset=['Invoice', 'StockCode'], keep=False)]
    results["invalid_rows"] = df[df["invalid_flag"] == 1]
    return results

# ---------------------------------------------------------
#  SQL VALIDATION FUNCTION
# ---------------------------------------------------------
def run_sql_validations(df):
    conn = sqlite3.connect(":memory:")
    df.to_sql("invoice", conn, index=False, if_exists="replace")
    results = {}

    results["orders_per_country"] = pd.read_sql(
        "SELECT Country, COUNT(*) AS total_orders "
        "FROM invoice " 
        "GROUP BY Country "
        "ORDER BY total_orders DESC", conn
    )

    results["missing_customers"] = pd.read_sql(
        "SELECT * FROM invoice WHERE [Customer ID] IS NULL", conn
    )
    conn.close()
    return results

# ---------------------------------------------------------
#  MAIN PIPELINE
# ---------------------------------------------------------
def run_pipeline():
    try:
        logging.info("Pipeline started.")

        df = pd.read_excel(RAW_PATH)
        df = cleaning(df)
        checks = run_validations(df)
        sql_checks = run_sql_validations(df)
        df.reset_index(drop=True, inplace=True)
        df.to_csv(CLEAN_PATH, index=False)

        # Concise log summary
        logging.info(
            f"Pipeline completed successfully | "
            f"Total rows: {len(df)} | "
            f"Missing Customer IDs: {len(checks['missing_Customer_IDs'])} | "
            f"Duplicate keys: {len(checks['key_dupes'])} | "
            f"Invalid flagged rows: {len(checks['invalid_rows'])}"
        )

        return df, checks, sql_checks

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        print("Pipeline failed. Check logs/day2_pipeline.log.")

# ---------------------------------------------------------
#  EXECUTE
# ---------------------------------------------------------
if __name__ == "__main__":
    df, checks, sql_checks = run_pipeline()

    # Optional: print summaries
    print("Total rows:", len(df))
    print("Missing Customer IDs:", len(checks['missing_Customer_IDs']))
    print("Duplicate keys:", len(checks['key_dupes'])e)
    print("Invalid flagged rows:", len(checks['invalid_rows']))
    print("Top 5 orders per country:\n", sql_checks['orders_per_country'].head())
