-- templates/transform/daily_sales_kpis.sql.j2

CREATE OR REPLACE TABLE fct_daily_sales AS
SELECT
    CAST(invoice_date AS DATE) AS sales_date,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue,
    COUNT(DISTINCT invoice_id) AS order_count,
    ROUND(
        SUM(quantity * unit_price) 
        / NULLIF(COUNT(DISTINCT invoice_id), 0), 
        2
    ) AS avg_order_value
FROM stg_orders_cleaned
GROUP BY sales_date
ORDER BY sales_date DESC;