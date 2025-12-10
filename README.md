# de_project
# Data Engineering & Analysis Project

## Project Overview

This project demonstrates end-to-end **Data Engineering and Analysis** workflows, covering ETL, data cleaning, quality checks, exploratory analysis, and reporting. It showcases the use of **Python, PySpark, SQL, Power BI, and cloud tools** where applicable, to handle real-world datasets.

**Objective:**

* Process raw datasets into clean, structured data
* Perform data quality checks and handle missing or duplicate values
* Conduct exploratory analysis and generate insights
* Build reusable scripts and notebooks for automation
* Integrate cloud tools for data storage, processing, or deployment

---

## Project Structure

```
de_project/
│
├─ raw/                  # Raw datasets
│   ├─ bakery_sales.csv
│   ├─ online_retail_II.xlsx
│   └─ customers_orderslist/
├─ clean/                # Cleaned datasets
│   ├─ clean_bakery_sales.csv
│   └─ online_retail_II.csv
├─ data_quality/         # Nulls, duplicates, and quality reports
│   ├─ online_retail_II_duplicate_rows.csv
│   └─ online_retail_II_nullsum_.csv
├─ notebooks/            # Jupyter notebooks for ETL & analysis
│   ├─ etl_bakery_sales.ipynb
│   ├─ etl_customers_orderslist.ipynb
│   └─ etl_online_retail_II.ipynb
├─ scripts/              # Python scripts for automation & ETL
│   ├─ bakery_sales_inspect_analysis.py
│   ├─ customers_orderslist_analysis.py
│   └─ online_retail_II_analysis.py
├─ logs/                 # Execution logs for scripts and pipelines
│   ├─ clean_bakery_sales.log
│   └─ online_retail_II.log
├─ sql/                  # SQLite databases or SQL scripts
│   └─ bakery_db.db
├─ .vscode/
│   └─ settings.json
├─ README.md             # Project overview and documentation
```

---

## Tools & Technologies

* **Python**: Data processing, scripting
* **PySpark**: Distributed data transformations
* **SQL**: Database operations and queries
* **Power BI**: Visualization and reporting
* **Cloud Platforms**: AWS, Azure, GCP (for storage & ETL)
* **Git & GitHub**: Version control and collaboration

---

## Completed Modules

### Bakery Sales ETL & Analysis

* Extracted raw CSV data from `raw/bakery_sales.csv`
* Cleaned data: removed nulls, standardized formats → saved in `clean/clean_bakery_sales.csv`
* Developed ETL notebook: `notebooks/etl_bakery_sales.ipynb`
* Created reusable script: `scripts/bakery_sales_inspect_analysis.py`
* Logged processing steps → `logs/clean_bakery_sales.log`

**Skills Applied:** Python scripting, Pandas, data cleaning, logging

### Online Retail II ETL & Analysis

* Imported raw Excel file `raw/online_retail_II.xlsx`
* Cleaned and transformed data → `clean/online_retail_II.csv`
* Checked for duplicates and missing values → `data_quality/`
* Built ETL notebook: `notebooks/etl_online_retail_II.ipynb`
* Scripted automated data pipeline: `scripts/online_retail_II_analysis.py`
* Logged execution → `logs/online_retail_II.log`

**Skills Applied:** Python, Pandas, data cleaning, logging, Excel integration

### Customers Orders List ETL & Analysis

* Processed raw datasets in `raw/customers_orderslist/`
* Cleaned and transformed data
* Built ETL notebook: `notebooks/etl_customers_orderslist.ipynb`
* Created reusable script: `scripts/customers_orderslist_analysis.py`

**Skills Applied:** Python, Pandas, data cleaning, ETL automation

---

## How to Run the Project

1. **Clone the repository**

   ```bash
   git clone https://github.com/HemanthLoka9999/de_project.git
   cd de_project
   ```

2. **Install Python dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the ETL/analysis scripts**

   ```bash
   python scripts/bakery_sales_inspect_analysis.py
   python scripts/customers_orderslist_analysis.py
   python scripts/online_retail_II_analysis.py
   ```

4. **Open notebooks for interactive analysis**

   ```bash
   jupyter notebook notebooks/
   ```

---

### Notes

* **Workflow:** `raw → clean → data_quality → scripts → notebooks → reports`
* Future modules will be added progressively, maintaining the same folder structure.
* Focus is on **reproducibility, clarity, and professional presentation**.

