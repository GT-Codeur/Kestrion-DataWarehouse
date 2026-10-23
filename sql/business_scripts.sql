/*1. Sanity check: row counts*/
SELECT 'dim_customer' t, COUNT(*) n FROM dim_customer UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date UNION ALL
SELECT 'fact_order_lines', COUNT(*) FROM fact_order_lines;

/*2. Revenue by month*/
SELECT d.cal_year, d.cal_month,
       COUNT(DISTINCT f.order_id) AS orders,
       SUM(f.line_amount)         AS revenue
FROM fact_order_lines f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.cal_year, d.cal_month
ORDER BY d.cal_year, d.cal_month;

/*3. Top 10 customers by revenue*/
SELECT c.customer_name, c.city, c.category,
       COUNT(DISTINCT f.order_id) AS orders,
       SUM(f.line_amount)         AS revenue
FROM fact_order_lines f
JOIN dim_customer c ON c.customer_id = f.customer_id
GROUP BY c.customer_name, c.city, c.category
ORDER BY revenue DESC
LIMIT 10;

/*4. Best-selling products by units and revenue*/
SELECT p.sku, p.product_name, p.category,
       SUM(f.quantity)    AS units_sold,
       SUM(f.line_amount) AS revenue
FROM fact_order_lines f
JOIN dim_product p ON p.product_id = f.product_id
GROUP BY p.sku, p.product_name, p.category
ORDER BY revenue DESC
LIMIT 10;

