# ETL Pipeline Overview

## 1. Extract
- Scripts: etl/scripts/01_bakery_sales_inspect_analysis.py
          etl/scripts/02_online_retail_II_analysis.py
          etl/scripts/03_customers_orderslist_analysis.py
- Reads raw CSV/XLSX files from data/raw/

## 2. Transform
- Cleans data: removes duplicates, handles missing values, normalizes types
- Outputs:
    - data/processed/ → cleaned datasets
    - data_quality/ → flagged rows, duplicates, null summaries

## 3. Load
- Saves cleaned datasets in data/processed/
- Logs ETL runs in etl/logs/
