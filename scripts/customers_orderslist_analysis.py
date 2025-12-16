import pandas as pd
import logging
from pathlib import Path

# ---------------------------------------------------------
#  FILE PATHS
# ---------------------------------------------------------
data_folder = Path(r"D:\Learnings\de_project\raw\customers_orderslist")
clean_folder = Path(r"D:\Learnings\de_project\clean")
log_folder = Path(r"D:\Learnings\de_project\logs")
dq_folder = Path(r"D:\Learnings\de_project\data_quality")

for folder in [clean_folder, log_folder, dq_folder]:
    folder.mkdir(parents=True, exist_ok=True)

LOG_PATH = log_folder / "auto_cleaning_pipeline.log"

CUSTOMERS_PATH = data_folder / "olist_customers_dataset.csv"
ORDERS_PATH = data_folder / "olist_orders_dataset.csv"
ORDER_ITEMS_PATH = data_folder / "olist_order_items_dataset.csv"
PRODUCTS_PATH = data_folder / "olist_products_dataset.csv"

CLEAN_CUSTOMERS = clean_folder / "clean_customers.csv"
CLEAN_ORDERS = clean_folder / "clean_orders.csv"
CLEAN_ORDER_ITEMS = clean_folder / "clean_order_items.csv"
CLEAN_PRODUCTS = clean_folder / "clean_products.csv"

# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s - Line:%(lineno)d"
)

# ---------------------------------------------------------
# AUTO CLEAN FUNCTION (WITH DATE THRESHOLD)
# ---------------------------------------------------------
def clean_df_auto(
    df: pd.DataFrame,
    df_name: str,
    dup_subset: dict = None,
    date_threshold: float = 0.7
) -> pd.DataFrame:

    try:
        # Standardize column names
        df.columns = df.columns.str.upper()

        # Identify column types
        cat_cols = [c for c in df.select_dtypes(include='object').columns if 'ID' not in c]
        num_cols = df.select_dtypes(include='number').columns.tolist()

        # -----------------------------
        # TEXT CLEANING
        # -----------------------------
        df[cat_cols] = df[cat_cols].apply(
            lambda x: x.str.strip().str.upper()
        )

        # -----------------------------
        # MISSING VALUE HANDLING
        # -----------------------------
        for col in num_cols:
            df[col] = df[col].fillna(0)

        for col in cat_cols:
            df[col] = df[col].fillna('UNKNOWN')

        # -----------------------------
        # SMART DATE DETECTION
        # -----------------------------
        for col in cat_cols.copy():
            parsed = pd.to_datetime(
                df[col],
                errors='coerce',
                infer_datetime_format=True
            )

            valid_ratio = parsed.notna().mean()

            if valid_ratio >= date_threshold:
                df[col] = parsed
                cat_cols.remove(col)
                logging.info(
                    f"{df_name}: Column '{col}' converted to datetime "
                    f"(valid ratio: {valid_ratio:.2f})"
                )

        # -----------------------------
        # DUPLICATE HANDLING
        # -----------------------------
        if dup_subset and df_name in dup_subset:
            subset_cols = [c.upper() for c in dup_subset[df_name]]
        else:
            subset_cols = None

        before = df.shape[0]
        df = df.drop_duplicates(subset=subset_cols, keep='first')
        after = df.shape[0]

        logging.info(
            f"{df_name}: Dropped {before - after} duplicates "
            f"based on {subset_cols or 'all columns'}"
        )

        logging.info(f"{df_name}: Final shape {df.shape}")
        return df

    except Exception as e:
        logging.error(f"{df_name}: Cleaning failed - {e}")
        raise

# ---------------------------------------------------------
# VALIDATION FUNCTION
# ---------------------------------------------------------
def validate_df(df: pd.DataFrame, df_name: str, dup_subset: dict) -> dict:
    results = {}
    keys = dup_subset.get(df_name, [])

    results['row_count'] = df.shape[0]
    results['null_counts'] = df.isna().sum().to_dict()

    dup_info = {}
    for key in keys:
        if key in df.columns:
            dup_info[key] = df[df.duplicated(subset=[key])].shape[0]

    results['duplicate_keys'] = dup_info

    summary_df = pd.DataFrame([results])
    summary_df.to_csv(dq_folder / f"{df_name}_quality.csv", index=False)

    logging.info(f"{df_name}: Validation completed")
    return results

# ---------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------
def run_pipeline():
    logging.info("Pipeline started")

    df_customers = pd.read_csv(CUSTOMERS_PATH)
    df_orders = pd.read_csv(ORDERS_PATH)
    df_order_items = pd.read_csv(ORDER_ITEMS_PATH)
    df_products = pd.read_csv(PRODUCTS_PATH)

    dup_subset = {
        'df_customers': ['CUSTOMER_ID'],
        'df_orders': ['ORDER_ID'],
        'df_order_items': ['ORDER_ITEM_ID'],
        'df_products': ['PRODUCT_ID']
    }

    df_customers = clean_df_auto(df_customers, 'df_customers', dup_subset)
    df_orders = clean_df_auto(df_orders, 'df_orders', dup_subset)
    df_order_items = clean_df_auto(df_order_items, 'df_order_items', dup_subset)
    df_products = clean_df_auto(df_products, 'df_products', dup_subset)

    validate_df(df_customers, 'df_customers', dup_subset)
    validate_df(df_orders, 'df_orders', dup_subset)
    validate_df(df_order_items, 'df_order_items', dup_subset)
    validate_df(df_products, 'df_products', dup_subset)

    df_customers.to_csv(CLEAN_CUSTOMERS, index=False)
    df_orders.to_csv(CLEAN_ORDERS, index=False)
    df_order_items.to_csv(CLEAN_ORDER_ITEMS, index=False)
    df_products.to_csv(CLEAN_PRODUCTS, index=False)

    logging.info("Pipeline completed successfully")
    print("Pipeline executed successfully")

# ---------------------------------------------------------
# EXECUTE
# ---------------------------------------------------------
if __name__ == "__main__":
    run_pipeline()
