# EDA Insights — Ecommerce Orders Dataset

## 1. Dataset Overview

- Dataset contains order-level transaction data.
- Columns: `order_id`, `customer_id`, `product_category`, `order_date`, `revenue`, `quantity`, `discount`, `payment_method`, `city`.
- No missing values except `discount` (316 rows).
- Total rows: 1200 (replace with actual)
- Total columns: 9

## 2. Key Findings

### 2.1 Distribution & Skewness

- Revenue distribution is **right-skewed** (few very high orders).
- Quantity is slightly negatively skewed.
- Most orders are small to medium value; a few high-value orders dominate total revenue.

### 2.2 Missing Values

- Only `discount` has missing values; likely indicates no discount applied.

### 2.3 Correlation Analysis

- Revenue shows very weak correlation with `quantity` and `discount`.
- Suggests revenue is influenced more by **product mix** or **customer behavior** than order size or discounts.

### 2.4 Outlier Analysis

- IQR method used for outlier detection.
- High-value orders identified as outliers.
- These outliers contribute significantly to total revenue but are genuine transactions.

### 2.5 Customer Segmentation

- Revenue bins created: Low, Medium, High, Very High.
- Majority of customers fall into Low/Medium segments.
- High/Very High segment is small but drives a large share of revenue.

## 3. Business Implications

- Short-term revenue growth is largely driven by a small number of high-value orders.
- Long-term growth depends on engaging the majority of smaller-order customers.
- Discounts have minimal impact on overall revenue; focus on **customer targeting** and **product prioritization**.
- Understanding revenue concentration helps in **retention strategies** for high-value customers and **upsell opportunities** for others.

