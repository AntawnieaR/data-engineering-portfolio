-- Sample SQL for Data Engineering Portfolio

-----------------------------------------------------
-- 1. Join orders with customers to build a reporting view

SELECT 
    o.order_id,
    o.order_date,
    c.customer_name,
    c.state,
    o.total_amount
FROM orders o
JOIN customers c 
    ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
ORDER BY o.order_date DESC;

-----------------------------------------------------
-- 2. Use a CTE to calculate monthly revenue

WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        SUM(total_amount) AS revenue
    FROM orders
    GROUP BY DATE_TRUNC('month', order_date)
)
SELECT 
    month,
    revenue
FROM monthly_revenue
ORDER BY month;

-----------------------------------------------------
-- 3. Identify duplicate customers by email

SELECT
    email,
    COUNT(*) AS record_count
FROM customers
GROUP BY email
HAVING COUNT(*) > 1
ORDER BY record_count DESC;

-----------------------------------------------------
-- 4. Basic data quality check: find orders with negative totals

SELECT
    order_id,
    order_date,
    total_amount
FROM orders
WHERE total_amount < 0;
