# DE Project Portfolio

## Folder Structure

- **data/** → raw & processed datasets
- **data_quality/** → proof of cleaning: duplicates, nulls, flagged rows
- **etl/** → scripts & logs for ETL pipelines
- **analysis/** → notebooks, figures, notes (includes EDA)
- **sql/** → SQL queries (basic → advanced)
- **dashboard/** → demo dashboards (if any)
- **docs/** → ETL explanation + diagrams
- **mock_s3/** → simulated cloud storage
- **archive/** → backups

## ETL Overview

- **Scripts:** etl/scripts/
- **Logs:** etl/logs/
- **Flow:** Raw data → Extract → Transform → Load (see docs/etl_flow.png)
- **Data Quality:** data_quality/ contains duplicates, null summaries, and cleaned datasets

**Analysis:** analysis/notebooks/ → feature engineering, insights; analysis/notes/ → observations and markdown summaries

**SQL:** sql/ contains queries using joins, window functions, and aggregations

