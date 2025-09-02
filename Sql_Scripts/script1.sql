-- Overall sales summary
CREATE OR REPLACE VIEW view_overall_sales_summary AS
SELECT 
    COUNT(DISTINCT InvoiceNo) as total_orders,
    COUNT(DISTINCT CustomerKey) as total_customers,
    COUNT(DISTINCT ProductKey) as total_products,
    SUM(SalesAmount) as total_revenue,
    AVG(SalesAmount) as avg_line_value,
    SUM(Quantity) as total_units_sold
FROM FactSales
WHERE Quantity > 0 AND  unitprice> 0;

-- Monthly sales trends
CREATE OR REPLACE VIEW view_monthly_sales_trends AS
SELECT 
    d.Year,
    d.Month,
    COUNT(DISTINCT f.InvoiceNo) as orders,
    SUM(f.SalesAmount) as revenue,
    SUM(f.Quantity) as units_sold,
    COUNT(DISTINCT f.CustomerKey) as active_customers
FROM FactSales f
JOIN DimDate d ON f.DateKey = d.DateKey
WHERE f.Quantity > 0 AND f.UnitPrice > 0  -- Filter once, consistently
GROUP BY d.Year, d.Month
ORDER BY d.Year, d.Month;