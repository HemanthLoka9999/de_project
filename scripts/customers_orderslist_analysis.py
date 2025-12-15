import pandas as pd
import logging
from pathlib import Path

# ---------------------------------------------------------
#  FILE PATHS
# ---------------------------------------------------------
data_folder = Path(r"D:\Learnings\de_project\raw\customers_orderslist")
clean_folder = Path(r"D:\Learnings\de_project\clean")
log_folder = Path(r"D:\Learnings\de_project\logs")
dq_folder = Path(r"D:\Learnings\de_project\data_quality")  # NEW: Data quality reports

# Ensure folders exist
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
# Logging config
# ---------------------------------------------------------
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s - Line:%(lineno)d"
)

# ---------------------------------------------------------
# Fully Automated Cleaning Function
# ---------------------------------------------------------
def clean_df_auto(df: pd.DataFrame, df_name: str = "DataFrame",
                  dup_subset: dict = None,
                  drop_duplicates: bool = True,
                  show_head: bool = True) -> pd.DataFrame:
    try:
        df.columns = df.columns.str.upper()
        cat_cols = [col for col in df.select_dtypes(include='object').columns if 'ID' not in col.upper()]
        num_cols = df.select_dtypes(include='number').columns.tolist()
        date_cols = [col for col in df.columns if 'DATE' in col or 'TIMESTAMP' in col]

        df[cat_cols] = df[cat_cols].apply(lambda x: x.str.strip().str.upper())

        for col in num_cols:
            df[col] = df[col].fillna(0)
        for col in cat_cols:
            df[col] = df[col].fillna('UNKNOWN')
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        if drop_duplicates:
            subset_cols = None
            if dup_subset and df_name in dup_subset:
                subset_cols = [col.upper() for col in dup_subset[df_name] if col.upper() in df.columns]
            before = df.shape[0]
            df = df.drop_duplicates(subset=subset_cols, keep='first')
            after = df.shape[0]
            logging.info(f"{df_name}: Dropped {before - after} duplicates based on {subset_cols or 'all columns'}")

        if show_head:
            print(f"\n--- Cleaned {df_name} (head) ---\n{df.head()}\n")

        logging.info(f"{df_name}: Cleaned DataFrame with shape: {df.shape}, columns: {df.columns.tolist()}")
        return df

    except Exception as e:
        logging.error(f"{df_name}: Error in cleaning DataFrame: {e}")
        raise

# ---------------------------------------------------------
# Validation Function
# ---------------------------------------------------------
def validate_df(df: pd.DataFrame, df_name: str, dup_subset: dict = None) -> dict:
    results = {}
    key_cols = dup_subset.get(df_name, []) if dup_subset else []

    missing_keys = {}
    dup_rows = {}
    for col in key_cols:
        if col in df.columns:
            missing_keys[col] = df[df[col].isna()].shape[0]
            dup_rows[col] = df[df.duplicated(subset=[col], keep=False)].shape[0]

    results['missing_keys'] = missing_keys
    results['duplicate_keys'] = dup_rows
    results['invalid_rows'] = df[df.get('CUSTOMER_ID', 'NA') == 'UNKNOWN'].shape[0] if 'CUSTOMER_ID' in df.columns else 0

    logging.info(f"{df_name} Validation Summary: {results}")
    print(f"{df_name} Validation Summary: {results}")

    # Save to data_quality folder as CSV
    summary_df = pd.DataFrame([results])
    summary_df.to_csv(dq_folder / f"{df_name}_quality.csv", index=False)

    return results

# ---------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------
def run_pipeline():
    try:
        logging.info("Pipeline started.")

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

        df_customers = clean_df_auto(df_customers, df_name='df_customers', dup_subset=dup_subset)
        df_orders = clean_df_auto(df_orders, df_name='df_orders', dup_subset=dup_subset)
        df_order_items = clean_df_auto(df_order_items, df_name='df_order_items', dup_subset=dup_subset)
        df_products = clean_df_auto(df_products, df_name='df_products', dup_subset=dup_subset)

        val_customers = validate_df(df_customers, 'df_customers', dup_subset)
        val_orders = validate_df(df_orders, 'df_orders', dup_subset)
        val_order_items = validate_df(df_order_items, 'df_order_items', dup_subset)
        val_products = validate_df(df_products, 'df_products', dup_subset)

        df_customers.to_csv(CLEAN_CUSTOMERS, index=False)
        df_orders.to_csv(CLEAN_ORDERS, index=False)
        df_order_items.to_csv(CLEAN_ORDER_ITEMS, index=False)
        df_products.to_csv(CLEAN_PRODUCTS, index=False)

        logging.info("Pipeline completed successfully.")
        print("Pipeline executed successfully. Clean files and validation reports are ready.")

        return {
            'df_customers': df_customers, 'df_orders': df_orders,
            'df_order_items': df_order_items, 'df_products': df_products,
            'validations': {
                'customers': val_customers,
                'orders': val_orders,
                'order_items': val_order_items,
                'products': val_products
            }
        }

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        print("Pipeline failed. Check logs for details.")

# ---------------------------------------------------------
# EXECUTE
# ---------------------------------------------------------
if __name__ == "__main__":
    results = run_pipeline()
