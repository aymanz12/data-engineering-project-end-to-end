-- Seasonal analysis
CREATE OR REPLACE VIEW view_seasonal_analysis AS
SELECT 
    d.Quarter,
    d.Month,
    SUM(f.SalesAmount) as revenue,
    COUNT(DISTINCT f.InvoiceNo) as orders
FROM FactSales f
JOIN DimDate d ON f.DateKey = d.DateKey
WHERE f.Quantity > 0 AND f.SalesAmount > 0
GROUP BY d.Quarter, d.Month
ORDER BY d.Quarter, d.Month;

-- Day of week patterns
CREATE OR REPLACE VIEW view_day_of_week_patterns AS
SELECT 
    d.Weekday,
    COUNT(DISTINCT f.InvoiceNo) as orders,
    SUM(f.SalesAmount) as revenue,
    AVG(f.SalesAmount) as avg_order_value
FROM FactSales f
JOIN DimDate d ON f.DateKey = d.DateKey
WHERE f.Quantity > 0 AND f.SalesAmount > 0
GROUP BY d.Weekday
ORDER BY 
    CASE d.Weekday
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END;