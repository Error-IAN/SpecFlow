-- templates/transform/orders_cleaning.sql.j2

CREATE OR REPLACE TABLE stg_orders_cleaned AS
SELECT
    CAST(Invoice AS VARCHAR) as invoice_id,
    CAST(StockCode AS VARCHAR) as stock_code,
    Description as description,
    CAST(Quantity AS INTEGER) as quantity,
    CAST(InvoiceDate AS TIMESTAMP) as invoice_date,
    CAST(Price AS DOUBLE) as unit_price,
    -- Use double quotes because the Excel source has a space
    CAST("Customer ID" AS VARCHAR) as customer_id,
    Country as country
FROM raw_orders
WHERE "Customer ID" IS NOT NULL  -- Added quotes here
  AND Price > 0
  AND Quantity > 0;