-- ============================================================================
-- Native Semantic View Demo - SQL-Based Creation (GA Aug 2025)
-- ============================================================================
-- This demo shows how to create semantic views using native SQL syntax
-- instead of YAML files. SQL-based semantic views offer:
-- - Version control friendly (plain SQL files)
-- - Easier debugging and iteration
-- - Direct integration with Cortex Analyst
-- ============================================================================

USE ROLE AUTOMATED_INTELLIGENCE_ADMIN;
USE DATABASE DASH_AUTOMATED_INTELLIGENCE_DB;
USE SCHEMA SEMANTIC;
USE WAREHOUSE AUTOMATED_INTELLIGENCE_WH;

-- ============================================================================
-- PART 1: Create a Simple Semantic View (Orders Analysis)
-- ============================================================================

-- Create a focused semantic view for order analysis
-- NOTE: Clause order must be: TABLES, RELATIONSHIPS, FACTS, DIMENSIONS, METRICS
CREATE OR REPLACE SEMANTIC VIEW ORDERS_ANALYTICS_SV
TABLES (
    -- Define logical tables with primary keys
    orders AS DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS
        PRIMARY KEY (ORDER_ID)
        WITH SYNONYMS = ('transactions', 'sales')
        COMMENT = 'Individual order transactions',
    
    customers AS DASH_AUTOMATED_INTELLIGENCE_DB.RAW.CUSTOMERS
        PRIMARY KEY (CUSTOMER_ID)
        WITH SYNONYMS = ('buyers', 'clients')
        COMMENT = 'Customer profiles'
)
RELATIONSHIPS (
    -- Define how tables relate
    orders(CUSTOMER_ID) REFERENCES customers
)
FACTS (
    -- Order value facts (FACTS must come before DIMENSIONS)
    orders.total_amount AS total_amount
        WITH SYNONYMS = ('order value', 'gross amount')
        COMMENT = 'Total order value before discount',
    
    orders.discount_percent AS discount_percent
        WITH SYNONYMS = ('discount rate', 'discount')
        COMMENT = 'Discount percentage applied',
    
    orders.shipping_cost AS shipping_cost
        WITH SYNONYMS = ('delivery cost')
        COMMENT = 'Shipping charges'
)
DIMENSIONS (
    -- Order dimensions
    orders.order_date AS order_date
        WITH SYNONYMS = ('purchase date', 'transaction date')
        COMMENT = 'When the order was placed',
    
    orders.order_status AS order_status
        WITH SYNONYMS = ('status')
        COMMENT = 'Order fulfillment status',
    
    -- Customer dimensions
    customers.customer_name AS CONCAT(first_name, ' ', last_name)
        WITH SYNONYMS = ('customer', 'name')
        COMMENT = 'Full customer name',
    
    customers.customer_segment AS customer_segment
        WITH SYNONYMS = ('segment', 'tier')
        COMMENT = 'Customer tier (Premium, Standard, Basic)',
    
    customers.customer_state AS state
        WITH SYNONYMS = ('location', 'region')
        COMMENT = 'Customer state'
)
METRICS (
    -- Revenue metrics
    orders.total_revenue AS SUM(total_amount)
        WITH SYNONYMS = ('gross revenue', 'total sales')
        COMMENT = 'Sum of all order values',
    
    orders.net_revenue AS SUM(total_amount * (1 - discount_percent / 100))
        WITH SYNONYMS = ('net sales')
        COMMENT = 'Revenue after discounts',
    
    orders.order_count AS COUNT(DISTINCT order_id)
        WITH SYNONYMS = ('number of orders', 'transaction count')
        COMMENT = 'Total number of orders',
    
    orders.avg_order_value AS AVG(total_amount)
        WITH SYNONYMS = ('AOV', 'average order')
        COMMENT = 'Average order value',
    
    -- Customer metrics
    customers.customer_count AS COUNT(DISTINCT customers.customer_id)
        WITH SYNONYMS = ('number of customers')
        COMMENT = 'Unique customers'
)
COMMENT = 'Semantic view for order and customer analytics'
AI_SQL_GENERATION = 'When asked about revenue, use net_revenue unless gross is specifically requested. Default time period is last 30 days unless specified.';

-- ============================================================================
-- PART 2: Verify the Semantic View
-- ============================================================================

-- Show the created semantic view
SHOW SEMANTIC VIEWS LIKE 'ORDERS_ANALYTICS_SV';

-- Show dimensions
SHOW SEMANTIC DIMENSIONS IN DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.ORDERS_ANALYTICS_SV;

-- Show metrics
SHOW SEMANTIC METRICS IN DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.ORDERS_ANALYTICS_SV;

-- ============================================================================
-- PART 3: Query the Semantic View with Standard SQL (GA March 2, 2026)
-- ============================================================================

-- Semantic views can be queried like regular views
-- The query optimizer understands the semantic context
SELECT 
    ORDER_YEAR,
    ORDER_MONTH,
    CUSTOMER_SEGMENT,
    TOTAL_REVENUE,
    ORDER_COUNT,
    AVG_ORDER_VALUE
FROM DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.ORDERS_ANALYTICS_SV
WHERE ORDER_YEAR = 2026
GROUP BY ORDER_YEAR, ORDER_MONTH, CUSTOMER_SEGMENT
ORDER BY ORDER_YEAR DESC, ORDER_MONTH DESC;

-- ============================================================================
-- PART 4: Generate YAML from Existing Semantic View
-- ============================================================================

-- You can export semantic views to YAML for documentation or migration
SELECT SYSTEM$GENERATE_SEMANTIC_VIEW_YAML('DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.ORDERS_ANALYTICS_SV');

-- ============================================================================
-- PART 5: Create Semantic View from YAML (Alternative Method)
-- ============================================================================

-- If you have a YAML file, you can create the semantic view from it
-- CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
--     'DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.MY_VIEW',
--     '<yaml_content>'
-- );

-- ============================================================================
-- Key Benefits of SQL-Based Semantic Views
-- ============================================================================

/*
1. VERSION CONTROL: SQL files work naturally with Git
2. CODE REVIEW: Standard PR workflows apply
3. DEBUGGING: SQL errors are easier to trace than YAML
4. ITERATION: Quick changes without YAML syntax concerns
5. DOCUMENTATION: SQL comments integrate with tools
6. TESTING: Can be validated with EXPLAIN or dry-run
*/

-- ============================================================================
-- PART 6: Query Semantic View with Standard SQL FROM Clause (GA March 2026)
-- ============================================================================
-- You can now query semantic views directly in the FROM clause like a regular
-- table, instead of using the SEMANTIC_VIEW() function. This makes semantic
-- views a first-class citizen in SQL.

-- Old syntax (still works):
SELECT * FROM SEMANTIC_VIEW(
    ORDERS_ANALYTICS_SV
    DIMENSIONS orders.order_date
    METRICS orders.total_revenue
)
ORDER BY order_date;

-- New syntax (GA March 2026) — standard SQL:
SELECT
    order_date,
    SUM(total_revenue) AS total_revenue
FROM ORDERS_ANALYTICS_SV
GROUP BY order_date
ORDER BY order_date;

-- You can use all standard SQL clauses: WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
SELECT
    customer_segment,
    SUM(total_revenue) AS segment_revenue,
    COUNT(DISTINCT order_count) AS total_orders
FROM ORDERS_ANALYTICS_SV
WHERE order_status = 'COMPLETED'
GROUP BY customer_segment
HAVING SUM(total_revenue) > 10000
ORDER BY segment_revenue DESC;

-- ============================================================================
-- Demo Complete
-- ============================================================================
SELECT '✅ Semantic View SQL Demo Complete!' AS status;
