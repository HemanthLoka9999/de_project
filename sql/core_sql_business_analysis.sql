/* =========================================================
   CORE SQL (BUSINESS SCENARIOS)
   Focus: Aggregation, correctness, business thinking
   ========================================================= */


/* 1. Total revenue (baseline sanity metric) */
SELECT
    SUM(quantity * price) AS total_revenue
FROM order_items;


/* 2. Total revenue per day */
SELECT
    order_date,
    SUM(quantity * price) AS daily_revenue
FROM order_items
GROUP BY order_date
ORDER BY order_date;


/* 3. Average order value (AOV) */
SELECT
    SUM(quantity * price) / COUNT(DISTINCT order_id) AS avg_order_value
FROM order_items;


/* 4. Revenue per customer */
SELECT
    customer_id,
    SUM(quantity * price) AS customer_revenue
FROM order_items
GROUP BY customer_id;


/* 5. Orders count per customer */
SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders
FROM order_items
GROUP BY customer_id;


/* 6. Conditional aggregation — completed vs cancelled revenue */
SELECT
    SUM(CASE WHEN order_status = 'Completed'
             THEN quantity * price ELSE 0 END) AS completed_revenue,
    SUM(CASE WHEN order_status = 'Cancelled'
             THEN quantity * price ELSE 0 END) AS cancelled_revenue
FROM order_items;


/* 7. Revenue from high-value orders only */
SELECT
    SUM(quantity * price) AS high_value_revenue
FROM order_items
WHERE quantity * price >= 1000;


/* 8. Customers with total spend above threshold */
SELECT
    customer_id,
    SUM(quantity * price) AS total_spent
FROM order_items
GROUP BY customer_id
HAVING SUM(quantity * price) > 5000;


/* 9. Handle NULL prices safely */
SELECT
    SUM(quantity * COALESCE(price, 0)) AS safe_revenue
FROM order_items;


/* 10. De-duplication at aggregation level (no DISTINCT abuse) */
SELECT
    order_id,
    SUM(quantity * price) AS order_revenue
FROM order_items
GROUP BY order_id;


/* 11. Monthly revenue trend */
SELECT
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    SUM(quantity * price) AS monthly_revenue
FROM order_items
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;


/* 12. Order bucket classification using CASE */
SELECT
    order_id,
    SUM(quantity * price) AS order_value,
    CASE
        WHEN SUM(quantity * price) >= 5000 THEN 'High'
        WHEN SUM(quantity * price) >= 2000 THEN 'Medium'
        ELSE 'Low'
    END AS order_bucket
FROM order_items
GROUP BY order_id;


/* 13. Top 5 customers by revenue */
SELECT
    customer_id,
    SUM(quantity * price) AS total_revenue
FROM order_items
GROUP BY customer_id
ORDER BY total_revenue DESC
LIMIT 5;


/* 14. Revenue contribution percentage per customer */
SELECT
    customer_id,
    SUM(quantity * price) AS customer_revenue,
    SUM(quantity * price) * 100.0 /
        (SELECT SUM(quantity * price) FROM order_items) AS revenue_pct
FROM order_items
GROUP BY customer_id;


/* 15. Daily order count vs daily revenue (sanity check) */
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity * price) AS total_revenue
FROM order_items
GROUP BY order_date
ORDER BY order_date;


/* 16. Identify zero-revenue or suspicious orders */
SELECT
    order_id,
    SUM(quantity * price) AS order_revenue
FROM order_items
GROUP BY order_id
HAVING SUM(quantity * price) = 0;


/* 17. Average items per order */
SELECT
    AVG(item_count) AS avg_items_per_order
FROM (
    SELECT
        order_id,
        SUM(quantity) AS item_count
    FROM order_items
    GROUP BY order_id
) t;


/* 18. Revenue excluding cancelled orders */
SELECT
    SUM(quantity * price) AS net_revenue
FROM order_items
WHERE order_status <> 'Cancelled';


/* 19. Customers with repeat purchases */
SELECT
    customer_id
FROM order_items
GROUP BY customer_id
HAVING COUNT(DISTINCT order_id) > 1;


/* 20. Revenue check by product (no joins) */
SELECT
    product_id,
    SUM(quantity * price) AS product_revenue
FROM order_items
GROUP BY product_id
ORDER BY product_revenue DESC;
