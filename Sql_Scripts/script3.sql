-- Top performing products
CREATE OR REPLACE VIEW view_top_performing_products AS
SELECT 
    p.StockCode,
    p.Description,
    SUM(f.Quantity) as total_quantity,
    SUM(f.SalesAmount) as total_revenue,
    COUNT(DISTINCT f.CustomerKey) as unique_customers,
    AVG(f.UnitPrice) as avg_unit_price
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
WHERE f.Quantity > 0 AND f.SalesAmount > 0
GROUP BY p.StockCode, p.Description
ORDER BY total_revenue DESC
LIMIT 20;

-- Product sales distribution
CREATE OR REPLACE VIEW view_Product_sales_distribution AS
SELECT 
    CASE 
        WHEN total_revenue >= 10000 THEN 'High Value'
        WHEN total_revenue >= 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END as product_tier,
    COUNT(*) as product_count,
    SUM(total_revenue) as tier_revenue
FROM (
    SELECT 
        ProductKey,
        SUM(CASE WHEN SalesAmount > 0 THEN SalesAmount ELSE 0 END) as total_revenue
    FROM FactSales
    WHERE Quantity > 0 
    GROUP BY ProductKey
) product_performance
GROUP BY 
    CASE 
        WHEN total_revenue >= 10000 THEN 'High Value'
        WHEN total_revenue >= 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END
ORDER BY tier_revenue DESC;