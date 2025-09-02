-- Customer segmentation (RFM-style analysis)
CREATE OR REPLACE VIEW view_customer_segmentation AS
WITH customer_metrics AS (
    SELECT 
        f.CustomerKey,
        c.Country,
        MAX(d.FullDate) as last_purchase_date,
        COUNT(DISTINCT f.InvoiceNo) as frequency,
        SUM(CASE WHEN f.SalesAmount > 0 THEN f.SalesAmount ELSE 0 END) as monetary_value,
        AVG(CASE WHEN f.SalesAmount > 0 THEN f.SalesAmount END) as avg_order_value
    FROM FactSales f
    JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
    JOIN DimDate d ON f.DateKey = d.DateKey
    WHERE f.Quantity > 0 
    GROUP BY f.CustomerKey, c.Country
)
SELECT 
    Country,
    COUNT(*) as customers,
    AVG(frequency) as avg_purchase_frequency,
    AVG(monetary_value) as avg_customer_value,
    AVG(avg_order_value) as avg_order_size
FROM customer_metrics
GROUP BY Country
ORDER BY avg_customer_value DESC;