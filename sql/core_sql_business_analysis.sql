/* ============================================================
   File: core_sql_business_analysis.sql
   Purpose:
   Core SQL patterns used in business analysis.
   Focus: clean logic, correct aggregation, interview-ready.
   ============================================================ */


/* ------------------------------------------------------------
   1. Customer-wise Revenue (Avoiding Revenue Inflation)
   Business Question:
   How much revenue has each customer generated?
------------------------------------------------------------ */

SELECT
    o.customer_id,
    SUM(oi.quantity * p.price) AS total_revenue
FROM orders o
JOIN order_items oi
    ON o.order_id = oi.order_id
JOIN products p
    ON oi.product_id = p.product_id
GROUP BY o.customer_id;


/* ------------------------------------------------------------
   2. Revenue with Product-level Discounts
   Business Question:
   What is the actual revenue after applying discounts?
------------------------------------------------------------ */

SELECT
    o.customer_id,
    SUM(
        oi.quantity * p.price * (1 - COALESCE(d.discount_percent, 0) / 100)
    ) AS discounted_revenue
FROM orders o
JOIN order_items oi
    ON o.order_id = oi.order_id
JOIN products p
    ON oi.product_id = p.product_id
LEFT JOIN product_discounts d
    ON p.product_id = d.product_id
GROUP BY o.customer_id;


/* ------------------------------------------------------------
   3. Customers with No Orders
   Business Question:
   Which customers have never placed an order?
------------------------------------------------------------ */

SELECT
    c.customer_id,
    c.name
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;


/* ------------------------------------------------------------
   4. Order-level Revenue Breakdown
   Business Question:
   What is the total value of each order?
------------------------------------------------------------ */

SELECT
    oi.order_id,
    SUM(oi.quantity * p.price) AS order_total
FROM order_items oi
JOIN products p
    ON oi.product_id = p.product_id
GROUP BY oi.order_id;


/* ------------------------------------------------------------
   5. Customers with Revenue Above Average
   Business Question:
   Which customers spend more than the average customer?
------------------------------------------------------------ */

WITH customer_revenue AS (
    SELECT
        o.customer_id,
        SUM(oi.quantity * p.price) AS total_revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY o.customer_id
)
SELECT *
FROM customer_revenue
WHERE total_revenue >
      (SELECT AVG(total_revenue) FROM customer_revenue);


/* ------------------------------------------------------------
   6. Top 3 Customers by Revenue
   Business Question:
   Who are the top revenue-generating customers?
------------------------------------------------------------ */

SELECT
    customer_id,
    total_revenue
FROM (
    SELECT
        o.customer_id,
        SUM(oi.quantity * p.price) AS total_revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY o.customer_id
) t
ORDER BY total_revenue DESC
LIMIT 3;


/* ------------------------------------------------------------
   7. Most Frequently Ordered Products
   Business Question:
   Which products are ordered the most?
------------------------------------------------------------ */

SELECT
    oi.product_id,
    COUNT(*) AS order_count
FROM order_items oi
GROUP BY oi.product_id
ORDER BY order_count DESC;


/* ------------------------------------------------------------
   8. Customers with More Than One Order
   Business Question:
   Identify repeat customers.
------------------------------------------------------------ */

SELECT
    customer_id,
    COUNT(order_id) AS total_orders
FROM orders
GROUP BY customer_id
HAVING COUNT(order_id) > 1;


/* ------------------------------------------------------------
   9. Revenue Contribution Percentage per Customer
   Business Question:
   What % of total revenue does each customer contribute?
------------------------------------------------------------ */

WITH customer_revenue AS (
    SELECT
        o.customer_id,
        SUM(oi.quantity * p.price) AS total_revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY o.customer_id
)
SELECT
    customer_id,
    total_revenue,
    ROUND(
        total_revenue * 100.0 /
        SUM(total_revenue) OVER (),
        2
    ) AS revenue_percentage
FROM customer_revenue;


/* ------------------------------------------------------------
   10. Detect Duplicate Product Entries per Order
   Business Question:
   Are there duplicate product lines within the same order?
------------------------------------------------------------ */

SELECT
    order_id,
    product_id,
    COUNT(*) AS duplicate_count
FROM order_items
GROUP BY order_id, product_id
HAVING COUNT(*) > 1;


/* ------------------------------------------------------------
   11. Highest Priced Product per Order
   Business Question:
   What is the most expensive product in each order?
------------------------------------------------------------ */

SELECT
    order_id,
    MAX(p.price) AS max_product_price
FROM order_items oi
JOIN products p
    ON oi.product_id = p.product_id
GROUP BY order_id;


/* ------------------------------------------------------------
   12. Orders Without Order Items (Data Quality Check)
   Business Question:
   Are there any incomplete orders?
------------------------------------------------------------ */

SELECT
    o.order_id
FROM orders o
LEFT JOIN order_items oi
    ON o.order_id = oi.order_id
WHERE oi.order_id IS NULL;


/* ------------------------------------------------------------
   13. Average Order Value per Customer
   Business Question:
   How valuable is each customer's average order?
------------------------------------------------------------ */

WITH order_totals AS (
    SELECT
        o.order_id,
        o.customer_id,
        SUM(oi.quantity * p.price) AS order_value
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY o.order_id, o.customer_id
)
SELECT
    customer_id,
    ROUND(AVG(order_value), 2) AS avg_order_value
FROM order_totals
GROUP BY customer_id;


/* ------------------------------------------------------------
   14. Customers Who Bought Discounted Products
   Business Question:
   Which customers benefited from discounts?
------------------------------------------------------------ */

SELECT DISTINCT
    o.customer_id
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN product_discounts d ON oi.product_id = d.product_id;


/* ------------------------------------------------------------
   15. Rank Customers by Revenue
   Business Question:
   Rank customers based on spending.
------------------------------------------------------------ */

WITH customer_revenue AS (
    SELECT
        o.customer_id,
        SUM(oi.quantity * p.price) AS total_revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY o.customer_id
)
SELECT
    customer_id,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM customer_revenue;
