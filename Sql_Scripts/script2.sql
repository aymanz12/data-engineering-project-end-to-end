-- Customer value analysis
CREATE OR REPLACE VIEW view_customer_value_analysis AS
SELECT 
    c.Country,
    COUNT(DISTINCT c.CustomerKey) as customer_count,
    SUM(f.SalesAmount) as total_revenue,
    AVG(f.SalesAmount) as avg_order_value,
    COUNT(DISTINCT f.InvoiceNo) as total_orders
FROM FactSales f
JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
WHERE f.SalesAmount > 0 AND f.Quantity > 0
GROUP BY c.Country
ORDER BY total_revenue DESC;

-- Customer purchase frequency
CREATE OR REPLACE VIEW view_customer_purchase_frequency AS
SELECT 
    customer_orders,
    COUNT(*) as customer_count
FROM (
    SELECT 
        CustomerKey,
        COUNT(DISTINCT InvoiceNo) as customer_orders
    FROM FactSales
    WHERE SalesAmount > 0 AND Quantity > 0
    GROUP BY CustomerKey
) customer_frequency
GROUP BY customer_orders
ORDER BY customer_orders;