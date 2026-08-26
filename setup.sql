-- ============================================================================
-- AUTOMATED INTELLIGENCE: HANDS-ON LAB - Setup Script
-- 
-- Theme: From raw data to AI-powered insights—entirely within Snowflake.
--
-- This script creates all infrastructure for the hands-on lab:
--   Section 1: Ingest & Stage  → Streaming tables, staging, dynamic tables
--   Section 2: Serve & Analyze → Interactive tables, dbt schemas
--   Section 3: Intelligence    → Cortex Search, semantic views, AI functions
--
-- Execution Order:
--   0. PREREQUISITES: Grant account-level privileges (ACCOUNTADMIN only)
--   1. Clean up existing objects (WIPE SLATE)
--   2. Create database, schemas, warehouses
--   3. Create raw tables and stored procedures
--   4. Create dynamic tables pipeline
--   5. Create interactive tables
--   6. Create AI/ML infrastructure
--   7. Create Cortex Search Service
--   8. Create Semantic View
--
-- IMPORTANT: Run PREREQUISITES section first as ACCOUNTADMIN, then the rest
-- ============================================================================
-- ============================================================================
-- PREREQUISITES: ACCOUNTADMIN GRANTS (Run as ACCOUNTADMIN first)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE ROLE AUTOMATED_INTELLIGENCE_ADMIN
  COMMENT = 'Role for managing the Automated Intelligence platform';

SET current_user = (SELECT CURRENT_USER());
GRANT ROLE AUTOMATED_INTELLIGENCE_ADMIN TO USER IDENTIFIER($current_user);
ALTER USER IDENTIFIER($current_user) SET DEFAULT_ROLE = AUTOMATED_INTELLIGENCE_ADMIN;

GRANT CREATE SNOWFLAKE INTELLIGENCE ON ACCOUNT TO ROLE AUTOMATED_INTELLIGENCE_ADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE AUTOMATED_INTELLIGENCE_ADMIN;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE AUTOMATED_INTELLIGENCE_ADMIN;

-- ============================================================================
-- DATA SCALE CONFIGURATION
-- ============================================================================
-- Default: '10M' (~5 min setup)
-- Full:    '50M' (~15 min setup, better Interactive Tables contrast)
--
-- To load 50M rows, change the value below before running.
-- WARNING: 50M takes approximately 3x longer to load and refresh.
-- ============================================================================
SET data_scale = '10M';

-- ============================================================================
-- MAIN SETUP: Continue as ACCOUNTADMIN (PAT sessions cannot switch roles)
-- ============================================================================

-- ============================================================================
-- STEP 0: WIPE SLATE - Drop all existing objects
-- ============================================================================

ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 1800;

DROP DATABASE IF EXISTS dash_automated_intelligence_db CASCADE;
CREATE DATABASE dash_automated_intelligence_db;
USE DATABASE dash_automated_intelligence_db;

-- ============================================================================
-- STEP 1: Infrastructure Foundation
-- Purpose: Database, schemas, and warehouses for all Acts
-- ============================================================================

-- Create database (already exists from step 0)
CREATE DATABASE IF NOT EXISTS dash_automated_intelligence_db;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS dash_automated_intelligence_db.raw;
CREATE SCHEMA IF NOT EXISTS dash_automated_intelligence_db.staging;
CREATE SCHEMA IF NOT EXISTS dash_automated_intelligence_db.dynamic_tables;
CREATE SCHEMA IF NOT EXISTS dash_automated_intelligence_db.interactive;
CREATE SCHEMA IF NOT EXISTS dash_automated_intelligence_db.semantic;
CREATE SCHEMA IF NOT EXISTS dash_automated_intelligence_db.dbt_staging COMMENT = 'Schema for dbt staging models';
CREATE SCHEMA IF NOT EXISTS dash_automated_intelligence_db.dbt_analytics COMMENT = 'Schema for dbt analytical models';
CREATE SCHEMA IF NOT EXISTS dash_automated_intelligence_db.dbt_intermediate COMMENT = 'Schema for dbt intermediate models';

-- dbt grants: AUTOMATED_INTELLIGENCE_ADMIN needs these to run dbt build
-- (dbt_intermediate is pre-created above so the cross-role grants below resolve.)
GRANT CREATE SCHEMA ON DATABASE dash_automated_intelligence_db TO ROLE automated_intelligence_admin;
GRANT USAGE, CREATE VIEW, CREATE TABLE ON SCHEMA dash_automated_intelligence_db.dbt_staging TO ROLE automated_intelligence_admin;
GRANT USAGE, CREATE VIEW, CREATE TABLE ON SCHEMA dash_automated_intelligence_db.dbt_analytics TO ROLE automated_intelligence_admin;
GRANT USAGE, CREATE VIEW, CREATE TABLE ON SCHEMA dash_automated_intelligence_db.dbt_intermediate TO ROLE automated_intelligence_admin;

-- Cross-role FUTURE grants for dbt schemas: dbt creates its models later (run as
-- AUTOMATED_INTELLIGENCE_ADMIN). These FUTURE grants ensure the ACCOUNTADMIN-owned
-- Streamlit dashboard can SELECT them as they are created. (ALL TABLES/ALL VIEWS
-- grants for the existing dbt objects — none yet at this point — are applied in
-- the post-dbt step documented in README / deploy.sh.) AUTOMATED_INTELLIGENCE_ADMIN
-- read grants on the pipeline schemas are applied at the END of this script, once
-- those tables exist.
GRANT USAGE ON SCHEMA dash_automated_intelligence_db.dbt_analytics TO ROLE accountadmin;
GRANT USAGE ON SCHEMA dash_automated_intelligence_db.dbt_staging TO ROLE accountadmin;
GRANT USAGE ON SCHEMA dash_automated_intelligence_db.dbt_intermediate TO ROLE accountadmin;
GRANT SELECT ON FUTURE TABLES IN SCHEMA dash_automated_intelligence_db.dbt_analytics TO ROLE accountadmin;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA dash_automated_intelligence_db.dbt_analytics TO ROLE accountadmin;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA dash_automated_intelligence_db.dbt_staging TO ROLE accountadmin;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA dash_automated_intelligence_db.dbt_intermediate TO ROLE accountadmin;

-- Create warehouse
CREATE OR REPLACE WAREHOUSE hol_wh
  WITH WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for automated intelligence HOL (Medium for 50M row scale)';

-- Set context
USE DATABASE dash_automated_intelligence_db;
USE SCHEMA raw;
USE WAREHOUSE hol_wh;


-- ============================================================================
-- STEP 2: Raw Tables (Act 1 - Ingest & Stage)
-- Purpose: Landing zone for Snowpipe Streaming data
-- ============================================================================

-- Create customers table
CREATE OR REPLACE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    registration_date DATE,
    customer_segment VARCHAR(20)
);

-- Create orders table
-- Note: order_id uses VARCHAR to support UUID from Snowpipe Streaming
CREATE OR REPLACE TABLE orders (
    order_id VARCHAR(36) PRIMARY KEY,
    customer_id INT,
    order_date TIMESTAMP,
    order_status VARCHAR(20),
    total_amount DECIMAL(10, 2),
    discount_percent DECIMAL(5, 2),
    shipping_cost DECIMAL(8, 2)
);

-- Create order_items table
-- Note: order_item_id and order_id use VARCHAR to support UUID from Snowpipe Streaming
CREATE OR REPLACE TABLE order_items (
    order_item_id VARCHAR(36) PRIMARY KEY,
    order_id VARCHAR(36),
    product_id INT,
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10, 2),
    line_total DECIMAL(12, 2)
);

-- ============================================================================
-- STEP 3: Stored Procedures (Act 1 - Ingest & Stage)
-- Purpose: Data generation and staging operations
-- ============================================================================

-- Procedure: generate_customers
-- Purpose: Generate customer records independently
-- Parameters: num_customers - Number of new customers to create
CREATE OR REPLACE PROCEDURE generate_customers(num_customers INT)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    LET next_customer_id INT := (SELECT COALESCE(MAX(customer_id), 0) + 1 FROM customers);
    
    INSERT INTO customers
    SELECT
        :next_customer_id + ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS customer_id,
        CASE UNIFORM(1, 20, RANDOM())
            WHEN 1 THEN 'John' WHEN 2 THEN 'Sarah' WHEN 3 THEN 'Michael' WHEN 4 THEN 'Emily'
            WHEN 5 THEN 'David' WHEN 6 THEN 'Jessica' WHEN 7 THEN 'Chris' WHEN 8 THEN 'Ashley'
            WHEN 9 THEN 'Matt' WHEN 10 THEN 'Amanda' WHEN 11 THEN 'Ryan' WHEN 12 THEN 'Lauren'
            WHEN 13 THEN 'Kevin' WHEN 14 THEN 'Nicole' WHEN 15 THEN 'Brian' WHEN 16 THEN 'Rachel'
            WHEN 17 THEN 'Tyler' WHEN 18 THEN 'Megan' WHEN 19 THEN 'Josh' ELSE 'Katie'
        END AS first_name,
        CASE UNIFORM(1, 20, RANDOM())
            WHEN 1 THEN 'Smith' WHEN 2 THEN 'Johnson' WHEN 3 THEN 'Williams' WHEN 4 THEN 'Brown'
            WHEN 5 THEN 'Jones' WHEN 6 THEN 'Garcia' WHEN 7 THEN 'Miller' WHEN 8 THEN 'Davis'
            WHEN 9 THEN 'Rodriguez' WHEN 10 THEN 'Martinez' WHEN 11 THEN 'Hernandez' WHEN 12 THEN 'Lopez'
            WHEN 13 THEN 'Gonzalez' WHEN 14 THEN 'Wilson' WHEN 15 THEN 'Anderson' WHEN 16 THEN 'Thomas'
            WHEN 17 THEN 'Taylor' WHEN 18 THEN 'Moore' WHEN 19 THEN 'Jackson' ELSE 'Martin'
        END AS last_name,
        'customer' || (:next_customer_id + ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) || '@email.com' AS email,
        '555-' || LPAD(UNIFORM(100, 999, RANDOM())::STRING, 3, '0') || '-' || LPAD(UNIFORM(1000, 9999, RANDOM())::STRING, 4, '0') AS phone,
        UNIFORM(100, 9999, RANDOM()) || ' ' || CASE UNIFORM(1, 10, RANDOM())
            WHEN 1 THEN 'Main St' WHEN 2 THEN 'Oak Ave' WHEN 3 THEN 'Maple Dr' 
            WHEN 4 THEN 'Cedar Ln' WHEN 5 THEN 'Pine Rd' WHEN 6 THEN 'Elm St'
            WHEN 7 THEN 'Washington Blvd' WHEN 8 THEN 'Lake View Dr' WHEN 9 THEN 'Mountain Way'
            ELSE 'Summit Trail'
        END AS address,
        CASE UNIFORM(1, 15, RANDOM())
            WHEN 1 THEN 'Denver' WHEN 2 THEN 'Salt Lake City' WHEN 3 THEN 'Boulder'
            WHEN 4 THEN 'Aspen' WHEN 5 THEN 'Park City' WHEN 6 THEN 'Jackson'
            WHEN 7 THEN 'Telluride' WHEN 8 THEN 'Steamboat Springs' WHEN 9 THEN 'Vail'
            WHEN 10 THEN 'Breckenridge' WHEN 11 THEN 'Mammoth Lakes' WHEN 12 THEN 'Tahoe City'
            WHEN 13 THEN 'Whistler' WHEN 14 THEN 'Banff' ELSE 'Portland'
        END AS city,
        CASE UNIFORM(1, 10, RANDOM())
            WHEN 1 THEN 'CO' WHEN 2 THEN 'UT' WHEN 3 THEN 'WY'
            WHEN 4 THEN 'CA' WHEN 5 THEN 'WA' WHEN 6 THEN 'OR'
            WHEN 7 THEN 'MT' WHEN 8 THEN 'ID' WHEN 9 THEN 'NV' ELSE 'BC'
        END AS state,
        LPAD(UNIFORM(10000, 99999, RANDOM())::STRING, 5, '0') AS zip_code,
        DATEADD(day, -UNIFORM(1, 1825, RANDOM()), CURRENT_DATE()) AS registration_date,
        CASE UNIFORM(1, 3, RANDOM())
            WHEN 1 THEN 'Premium'
            WHEN 2 THEN 'Standard'
            ELSE 'Basic'
        END AS customer_segment
    FROM TABLE(GENERATOR(ROWCOUNT => :num_customers));
    
    RETURN 'Successfully generated ' || :num_customers || ' customers';
END;
$$;

-- Verify procedures created
SHOW PROCEDURES LIKE '%generate%';


-- ============================================================================
-- STEP 3.5: Staging Layer + Gen2 Warehouse (Act 1 - Ingest & Stage)
-- Purpose: Staging tables for MERGE operations, Gen2 for Optima Indexing
-- ============================================================================

USE SCHEMA dash_automated_intelligence_db.staging;

-- Create staging tables for Snowpipe Streaming
CREATE TABLE IF NOT EXISTS orders_staging (
    order_id VARCHAR(36),
    customer_id INTEGER,
    order_date TIMESTAMP_NTZ,
    order_status VARCHAR(20),
    total_amount FLOAT,
    discount_percent FLOAT,
    shipping_cost FLOAT
)
COMMENT = 'Staging table for order data from Snowpipe Streaming';

CREATE TABLE IF NOT EXISTS order_items_staging (
    order_item_id VARCHAR(36),
    order_id VARCHAR(36),
    product_id INTEGER,
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    quantity INTEGER,
    unit_price FLOAT,
    line_total FLOAT
)
COMMENT = 'Staging table for order item data from Snowpipe Streaming';

-- Snowpipe Streaming grants: AUTOMATED_INTELLIGENCE_ADMIN streams into these tables
-- and calls the merge/truncate/count procedures (all caller's rights).
GRANT USAGE     ON SCHEMA dash_automated_intelligence_db.staging                      TO ROLE automated_intelligence_admin;
GRANT INSERT, SELECT, DELETE, TRUNCATE ON TABLE orders_staging                        TO ROLE automated_intelligence_admin;
GRANT INSERT, SELECT, DELETE, TRUNCATE ON TABLE order_items_staging                   TO ROLE automated_intelligence_admin;
GRANT CREATE PIPE ON SCHEMA dash_automated_intelligence_db.staging                    TO ROLE automated_intelligence_admin;

-- Snapshot table for benchmarking
CREATE TABLE IF NOT EXISTS discount_snapshot (
    order_id VARCHAR(36),
    discount_percent FLOAT
);

-- Create Gen2 warehouse
CREATE OR REPLACE WAREHOUSE hol_gen2_wh
WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    GENERATION = '2'
    COMMENT = 'Gen2 warehouse for data transformation';

-- Procedure: merge_staging_to_raw
CREATE OR REPLACE PROCEDURE merge_staging_to_raw(
    return_timing BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    orders_merged INTEGER;
    order_items_merged INTEGER;
    orders_start TIMESTAMP_NTZ;
    orders_end TIMESTAMP_NTZ;
    items_start TIMESTAMP_NTZ;
    items_end TIMESTAMP_NTZ;
BEGIN
    start_time := CURRENT_TIMESTAMP();
    
    orders_start := CURRENT_TIMESTAMP();
    
    MERGE INTO raw.orders tgt
    USING (
        SELECT 
            order_id,
            customer_id,
            order_date,
            order_status,
            total_amount,
            discount_percent,
            shipping_cost
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) as rn
            FROM staging.orders_staging
        )
        WHERE rn = 1
    ) src
    ON tgt.order_id = src.order_id
    WHEN MATCHED THEN UPDATE SET
        customer_id = src.customer_id,
        order_date = src.order_date,
        order_status = src.order_status,
        total_amount = src.total_amount,
        discount_percent = src.discount_percent,
        shipping_cost = src.shipping_cost
    WHEN NOT MATCHED THEN INSERT (
        order_id, customer_id, order_date, order_status, total_amount, discount_percent, shipping_cost
    ) VALUES (
        src.order_id, src.customer_id, src.order_date, src.order_status,
        src.total_amount, src.discount_percent, src.shipping_cost
    );
    
    orders_merged := SQLROWCOUNT;
    orders_end := CURRENT_TIMESTAMP();
    
    items_start := CURRENT_TIMESTAMP();
    
    MERGE INTO raw.order_items tgt
    USING (
        SELECT 
            order_item_id,
            order_id,
            product_id,
            product_name,
            product_category,
            quantity,
            unit_price,
            line_total
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY order_item_id) as rn
            FROM staging.order_items_staging
        )
        WHERE rn = 1
    ) src
    ON tgt.order_item_id = src.order_item_id
    WHEN MATCHED THEN UPDATE SET
        order_id = src.order_id,
        product_id = src.product_id,
        product_name = src.product_name,
        product_category = src.product_category,
        quantity = src.quantity,
        unit_price = src.unit_price,
        line_total = src.line_total
    WHEN NOT MATCHED THEN INSERT (
        order_item_id, order_id, product_id, product_name, product_category,
        quantity, unit_price, line_total
    ) VALUES (
        src.order_item_id, src.order_id, src.product_id, src.product_name,
        src.product_category, src.quantity, src.unit_price,
        src.line_total
    );
    
    order_items_merged := SQLROWCOUNT;
    items_end := CURRENT_TIMESTAMP();
    
    -- Delete only confirmed merged rows from staging
    DELETE FROM staging.order_items_staging
    WHERE order_item_id IN (SELECT order_item_id FROM raw.order_items);
    
    DELETE FROM staging.orders_staging
    WHERE order_id IN (SELECT order_id FROM raw.orders);
    
    end_time := CURRENT_TIMESTAMP();
    
    IF (return_timing) THEN
        RETURN OBJECT_CONSTRUCT(
            'total_duration_ms', DATEDIFF('millisecond', start_time, end_time),
            'orders', OBJECT_CONSTRUCT(
                'records_merged', orders_merged,
                'duration_ms', DATEDIFF('millisecond', orders_start, orders_end)
            ),
            'order_items', OBJECT_CONSTRUCT(
                'records_merged', order_items_merged,
                'duration_ms', DATEDIFF('millisecond', items_start, items_end)
            ),
            'start_time', start_time,
            'end_time', end_time
        );
    ELSE
        RETURN OBJECT_CONSTRUCT('status', 'success');
    END IF;
END;
$$;

-- Procedure: enrich_raw_data
CREATE OR REPLACE PROCEDURE enrich_raw_data(
    return_timing BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    orders_updated INTEGER;
BEGIN
    start_time := CURRENT_TIMESTAMP();
    
    UPDATE raw.orders
    SET discount_percent = CASE 
        WHEN total_amount >= 1000 THEN LEAST(discount_percent + 5.0, 50.0)
        WHEN total_amount >= 500 THEN LEAST(discount_percent + 2.5, 50.0)
        ELSE discount_percent
    END
    WHERE order_date >= DATEADD('day', -30, CURRENT_DATE())
      AND discount_percent < 50.0;
    
    orders_updated := SQLROWCOUNT;
    
    end_time := CURRENT_TIMESTAMP();
    
    IF (return_timing) THEN
        RETURN OBJECT_CONSTRUCT(
            'orders_updated', orders_updated,
            'duration_ms', DATEDIFF('millisecond', start_time, end_time),
            'start_time', start_time,
            'end_time', end_time
        );
    ELSE
        RETURN OBJECT_CONSTRUCT('status', 'success');
    END IF;
END;
$$;

-- Procedure: create_discount_snapshot
CREATE OR REPLACE PROCEDURE create_discount_snapshot()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE staging.discount_snapshot;
    
    INSERT INTO staging.discount_snapshot
    SELECT order_id, discount_percent
    FROM raw.orders
    WHERE order_date >= DATEADD('day', -30, CURRENT_DATE());
    
    RETURN 'Discount snapshot created';
END;
$$;

-- Procedure: restore_discount_snapshot
CREATE OR REPLACE PROCEDURE restore_discount_snapshot()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE raw.orders
    SET discount_percent = snapshot.discount_percent
    FROM staging.discount_snapshot snapshot
    WHERE raw.orders.order_id = snapshot.order_id;
    
    RETURN 'Discount values restored from snapshot';
END;
$$;

-- Procedure: truncate_staging_tables
CREATE OR REPLACE PROCEDURE truncate_staging_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE staging.orders_staging;
    TRUNCATE TABLE staging.order_items_staging;
    
    RETURN 'Staging tables truncated successfully';
END;
$$;

-- Procedure: get_staging_counts
CREATE OR REPLACE PROCEDURE get_staging_counts()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    orders_count INTEGER;
    order_items_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO orders_count FROM staging.orders_staging;
    SELECT COUNT(*) INTO order_items_count FROM staging.order_items_staging;
    
    RETURN OBJECT_CONSTRUCT(
        'orders_staging', orders_count,
        'order_items_staging', order_items_count,
        'total_pending', orders_count + order_items_count
    );
END;
$$;


-- ============================================================================
-- STEP 4: Dynamic Tables Pipeline (Act 1 - Ingest & Stage)
-- Purpose: 3-tier incremental refresh pipeline (Enrichment → Integration → Aggregation)
-- ============================================================================

USE SCHEMA dash_automated_intelligence_db.dynamic_tables;
USE WAREHOUSE hol_wh;

-- Drop existing dynamic tables to avoid schema conflicts when updating
DROP DYNAMIC TABLE IF EXISTS product_performance_metrics;
DROP DYNAMIC TABLE IF EXISTS daily_business_metrics;
DROP DYNAMIC TABLE IF EXISTS fact_orders;
DROP DYNAMIC TABLE IF EXISTS enriched_order_items;
DROP DYNAMIC TABLE IF EXISTS enriched_orders;

-- ----------------------------------------------------------------------------
-- TIER 1: Enrichment Layer
-- Purpose: Add temporal dimensions and calculated fields to raw data
-- Target Lag: 12 hours (time-based scheduling)
-- Refresh Mode: INCREMENTAL
-- ----------------------------------------------------------------------------

-- Enriched Orders: Add temporal dimensions and financial calculations
-- TARGET_LAG = '1 minute' for near real-time analytics (matches Snowpipe Streaming latency)
CREATE OR REPLACE DYNAMIC TABLE enriched_orders
TARGET_LAG = '1 minute'
WAREHOUSE = hol_wh
INITIALIZE = ON_SCHEDULE
REFRESH_MODE = INCREMENTAL
AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    
    -- Temporal dimensions
    DATE(o.order_date) AS order_date_only,
    YEAR(o.order_date) AS order_year,
    QUARTER(o.order_date) AS order_quarter,
    MONTH(o.order_date) AS order_month,
    DAYOFWEEK(o.order_date) AS order_day_of_week,
    DAYNAME(o.order_date) AS order_day_name,
    WEEK(o.order_date) AS order_week,
    
    -- Financial calculations
    o.total_amount,
    o.discount_percent,
    o.shipping_cost,
    ROUND(o.total_amount * (o.discount_percent / 100), 2) AS discount_amount,
    ROUND(o.total_amount - (o.total_amount * (o.discount_percent / 100)), 2) AS net_amount,
    ROUND(o.total_amount - (o.total_amount * (o.discount_percent / 100)) + o.shipping_cost, 2) AS final_amount,
    
    -- Discount flags
    CASE WHEN o.discount_percent > 0 THEN TRUE ELSE FALSE END AS has_discount,
    CASE 
        WHEN o.discount_percent = 0 THEN 'No Discount'
        WHEN o.discount_percent <= 10 THEN 'Low Discount'
        WHEN o.discount_percent <= 20 THEN 'Medium Discount'
        ELSE 'High Discount'
    END AS discount_tier,
    
    -- Order size
    CASE
        WHEN o.total_amount < 100 THEN 'Small'
        WHEN o.total_amount < 500 THEN 'Medium'
        WHEN o.total_amount < 2000 THEN 'Large'
        ELSE 'Extra Large'
    END AS order_size_category
FROM dash_automated_intelligence_db.raw.orders o;

-- Enriched Order Items: Add price analysis and category flags
-- TARGET_LAG = '1 minute' for near real-time analytics (matches Snowpipe Streaming latency)
CREATE OR REPLACE DYNAMIC TABLE enriched_order_items
TARGET_LAG = '1 minute'
WAREHOUSE = hol_wh
INITIALIZE = ON_SCHEDULE
REFRESH_MODE = INCREMENTAL
AS
SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.product_name,
    oi.product_category,
    oi.quantity,
    oi.unit_price,
    oi.line_total,
    
    -- Price analysis
    ROUND(oi.line_total / oi.quantity, 2) AS actual_unit_price,
    ROUND(oi.unit_price - (oi.line_total / oi.quantity), 2) AS unit_price_variance,
    
    -- Category flags
    CASE WHEN oi.product_category = 'Skis' THEN TRUE ELSE FALSE END AS is_skis,
    CASE WHEN oi.product_category = 'Snowboards' THEN TRUE ELSE FALSE END AS is_snowboards,
    
    -- Quantity tiers
    CASE
        WHEN oi.quantity = 1 THEN 'Single'
        WHEN oi.quantity <= 3 THEN 'Few'
        ELSE 'Bulk'
    END AS quantity_tier
FROM dash_automated_intelligence_db.raw.order_items oi;

-- ----------------------------------------------------------------------------
-- TIER 2: Integration Layer
-- Purpose: Join enriched tables to create denormalized fact table
-- Target Lag: DOWNSTREAM (waits for Tier 1 to complete)
-- Refresh Mode: INCREMENTAL
-- ----------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE fact_orders
TARGET_LAG = DOWNSTREAM
WAREHOUSE = hol_wh
INITIALIZE = ON_SCHEDULE
REFRESH_MODE = INCREMENTAL
AS
SELECT
    eo.order_id,
    eo.customer_id,
    eo.order_date,
    eo.order_date_only,
    eo.order_year,
    eo.order_quarter,
    eo.order_month,
    eo.order_day_of_week,
    eo.order_day_name,
    eo.order_week,
    eo.order_status,
    
    -- Order-level metrics
    eo.total_amount,
    eo.discount_percent,
    eo.discount_amount,
    eo.net_amount,
    eo.shipping_cost,
    eo.final_amount,
    eo.has_discount,
    eo.discount_tier,
    eo.order_size_category,
    
    -- Item-level details
    eoi.order_item_id,
    eoi.product_id,
    eoi.product_name,
    eoi.product_category,
    eoi.quantity,
    eoi.unit_price,
    eoi.line_total,
    eoi.actual_unit_price,
    eoi.unit_price_variance,
    eoi.is_skis,
    eoi.is_snowboards,
    eoi.quantity_tier,
    
    -- Calculated metrics
    COUNT(eoi.order_item_id) OVER (PARTITION BY eo.order_id) AS items_per_order,
    SUM(eoi.line_total) OVER (PARTITION BY eo.order_id) AS order_items_total
FROM dash_automated_intelligence_db.dynamic_tables.enriched_orders eo
INNER JOIN dash_automated_intelligence_db.dynamic_tables.enriched_order_items eoi
    ON eo.order_id = eoi.order_id;

-- ----------------------------------------------------------------------------
-- TIER 3: Aggregation Layer
-- Purpose: Pre-compute business metrics for instant query performance
-- Target Lag: DOWNSTREAM (waits for Tier 2 to complete)
-- Refresh Mode: INCREMENTAL
-- ----------------------------------------------------------------------------

-- Daily Business Metrics: Daily aggregations of key business metrics
CREATE OR REPLACE DYNAMIC TABLE daily_business_metrics
TARGET_LAG = DOWNSTREAM
WAREHOUSE = hol_wh
INITIALIZE = ON_SCHEDULE
REFRESH_MODE = INCREMENTAL
AS
SELECT
    order_date_only,
    order_year,
    order_quarter,
    order_month,
    order_week,
    order_day_name,
    
    -- Order metrics
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    AVG(items_per_order) AS avg_items_per_order,
    
    -- Revenue metrics
    SUM(total_amount) AS total_revenue,
    SUM(net_amount) AS total_net_revenue,
    SUM(discount_amount) AS total_discounts,
    SUM(shipping_cost) AS total_shipping,
    SUM(final_amount) AS total_final_revenue,
    AVG(final_amount) AS avg_order_value,
    
    -- Discount analysis
    COUNT(DISTINCT CASE WHEN has_discount THEN order_id END) AS orders_with_discount,
    ROUND(COUNT(DISTINCT CASE WHEN has_discount THEN order_id END)::DECIMAL / COUNT(DISTINCT order_id) * 100, 2) AS discount_penetration_pct,
    AVG(CASE WHEN has_discount THEN discount_percent END) AS avg_discount_percent,
    
    -- Order size distribution
    COUNT(DISTINCT CASE WHEN order_size_category = 'Small' THEN order_id END) AS small_orders,
    COUNT(DISTINCT CASE WHEN order_size_category = 'Medium' THEN order_id END) AS medium_orders,
    COUNT(DISTINCT CASE WHEN order_size_category = 'Large' THEN order_id END) AS large_orders,
    COUNT(DISTINCT CASE WHEN order_size_category = 'Extra Large' THEN order_id END) AS extra_large_orders
FROM dash_automated_intelligence_db.dynamic_tables.fact_orders
GROUP BY 
    order_date_only,
    order_year,
    order_quarter,
    order_month,
    order_week,
    order_day_name;

-- Product Performance Metrics: Product category aggregations
CREATE OR REPLACE DYNAMIC TABLE product_performance_metrics
TARGET_LAG = DOWNSTREAM
WAREHOUSE = hol_wh
INITIALIZE = ON_SCHEDULE
REFRESH_MODE = INCREMENTAL
AS
SELECT
    product_category,
    
    -- Sales metrics
    COUNT(DISTINCT order_id) AS orders_count,
    COUNT(order_item_id) AS items_sold,
    SUM(quantity) AS total_quantity_sold,
    SUM(line_total) AS total_revenue,
    
    -- Averages
    AVG(unit_price) AS avg_unit_price,
    AVG(quantity) AS avg_quantity_per_order,
    AVG(line_total) AS avg_line_total,
    
    -- Price analysis
    MIN(unit_price) AS min_unit_price,
    MAX(unit_price) AS max_unit_price,
    
    -- Category flags
    SUM(CASE WHEN is_skis THEN 1 ELSE 0 END) AS ski_items,
    SUM(CASE WHEN is_snowboards THEN 1 ELSE 0 END) AS snowboard_items,
    
    -- Quantity distribution
    COUNT(CASE WHEN quantity_tier = 'Single' THEN 1 END) AS single_item_orders,
    COUNT(CASE WHEN quantity_tier = 'Few' THEN 1 END) AS few_item_orders,
    COUNT(CASE WHEN quantity_tier = 'Bulk' THEN 1 END) AS bulk_orders
FROM dash_automated_intelligence_db.dynamic_tables.fact_orders
GROUP BY product_category;


-- ============================================================================
-- STEP 5: Data Quality Monitoring (Act 3 - Intelligence & Governance)
-- Purpose: DMFs for automated data quality checks and alerting
-- ============================================================================

-- Switch back to standard warehouse for DMF views (interactive warehouses can't query DMF results)
USE WAREHOUSE hol_wh;
USE SCHEMA dash_automated_intelligence_db.raw;

-- Set DMF schedule to trigger on data changes
ALTER TABLE orders SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
ALTER TABLE order_items SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- Add NULL_COUNT DMFs to orders table
ALTER TABLE orders ADD DATA METRIC FUNCTION 
  SNOWFLAKE.CORE.NULL_COUNT ON (order_id),
  SNOWFLAKE.CORE.NULL_COUNT ON (customer_id),
  SNOWFLAKE.CORE.NULL_COUNT ON (order_date),
  SNOWFLAKE.CORE.NULL_COUNT ON (total_amount);

-- Add NULL_COUNT DMFs to order_items table
-- NOTE: Intentionally monitoring product_category instead of product_name (mis-attached)
-- This is a deliberate error for attendees to discover and fix in the Data Quality section
ALTER TABLE order_items ADD DATA METRIC FUNCTION 
  SNOWFLAKE.CORE.NULL_COUNT ON (order_item_id),
  SNOWFLAKE.CORE.NULL_COUNT ON (order_id),
  SNOWFLAKE.CORE.NULL_COUNT ON (product_id),
  SNOWFLAKE.CORE.NULL_COUNT ON (product_category),
  SNOWFLAKE.CORE.NULL_COUNT ON (quantity),
  SNOWFLAKE.CORE.NULL_COUNT ON (unit_price);

-- Create view for DMF results
-- SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS is a plain account-scoped view;
-- filter it down to the two tables we monitor.
CREATE OR REPLACE VIEW vw_dq_monitoring_results AS
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'DASH_AUTOMATED_INTELLIGENCE_DB'
  AND table_schema = 'RAW'
  AND table_name IN ('ORDERS', 'ORDER_ITEMS');

-- Create alert tracking table
CREATE TABLE IF NOT EXISTS data_quality_alerts (
  alert_time TIMESTAMP_NTZ,
  issue_summary VARCHAR,
  PRIMARY KEY (alert_time)
);

-- Create alert to monitor data quality issues
CREATE OR REPLACE ALERT data_quality_alert
  WAREHOUSE = hol_wh
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1
    FROM vw_dq_monitoring_results
    WHERE 
      table_database = 'DASH_AUTOMATED_INTELLIGENCE_DB'
      AND table_schema = 'RAW'
      AND table_name IN ('ORDERS', 'ORDER_ITEMS')
      AND metric_name = 'NULL_COUNT'
      AND value::INT > 0
      AND measurement_time >= DATEADD('MINUTE', -10, CURRENT_TIMESTAMP())
  ))
  THEN
INSERT INTO dash_automated_intelligence_db.raw.data_quality_alerts (alert_time, issue_summary)
    SELECT 
      CURRENT_TIMESTAMP(),
      'Data Quality Issue: NULL values detected - check vw_dq_monitoring_results for details';

-- Resume alert (start monitoring)
ALTER ALERT data_quality_alert RESUME;


-- ============================================================================
-- STEP 6: AI/ML Infrastructure (Act 3 - Intelligence & Governance)
-- Purpose: Tables for Cortex AI functions, semantic search, and Cortex Agent
-- ============================================================================

USE SCHEMA dash_automated_intelligence_db.raw;
USE WAREHOUSE hol_wh;

-- Product catalog with descriptions for semantic search
CREATE TABLE IF NOT EXISTS product_catalog (
  product_id INT PRIMARY KEY,
  product_name VARCHAR(100),
  product_category VARCHAR(50),
  description TEXT,
  features TEXT,
  price DECIMAL(10,2),
  stock_quantity INT
);

-- Customer reviews for sentiment analysis and AI_AGG
CREATE TABLE IF NOT EXISTS product_reviews (
  review_id INT AUTOINCREMENT PRIMARY KEY,
  product_id INT,
  customer_id INT,
  review_date DATE,
  rating INT,
  review_title VARCHAR(200),
  review_text TEXT,
  verified_purchase BOOLEAN
);

-- Customer support tickets for AI_COMPLETE and classification
CREATE TABLE IF NOT EXISTS support_tickets (
  ticket_id INT AUTOINCREMENT PRIMARY KEY,
  customer_id INT,
  ticket_date TIMESTAMP_NTZ,
  category VARCHAR(50),
  priority VARCHAR(20),
  subject VARCHAR(200),
  description TEXT,
  resolution TEXT,
  status VARCHAR(20)
);

-- Insert sample product catalog data (use MERGE to prevent duplicates)
MERGE INTO product_catalog t
USING (
  SELECT * FROM (VALUES
  (1001, 'Powder Skis', 'Skis', 'Premium powder skis designed for deep snow conditions. Featuring a wide waist and rockered tip for effortless floating in backcountry powder.', 'Wide waist (115mm), Rockered tip and tail, Carbon fiber construction, Lightweight design', 799.99, 15),
  (1002, 'All-Mountain Skis', 'Skis', 'Versatile all-mountain skis perfect for any terrain. Handles groomed runs, powder, and moguls with equal confidence.', 'Medium waist (88mm), Progressive sidecut, Titanal reinforcement, Durable construction', 649.99, 25),
  (1003, 'Freestyle Snowboard', 'Snowboards', 'Twin-tip freestyle snowboard for park and pipe. Perfect for tricks, jumps, and jibbing with a soft, playful flex.', 'True twin shape, Soft flex rating, Sintered base, Pop-optimized core', 549.99, 20),
  (1004, 'Freeride Snowboard', 'Snowboards', 'Directional freeride snowboard for charging hard in variable conditions. Stiff and stable for high-speed descents.', 'Directional shape, Stiff flex, Carbon stringers, Powder-friendly nose', 699.99, 12),
  (1005, 'Ski Boots', 'Boots', 'High-performance alpine ski boots with customizable fit. Four-buckle design with walk mode for comfort and power transmission.', '130 flex rating, GripWalk soles, Heat-moldable liner, Walk mode', 449.99, 30),
  (1006, 'Snowboard Boots', 'Boots', 'Comfortable snowboard boots with Boa lacing system. Quick entry and perfect fit adjustment for all-day riding.', 'Boa lacing system, Medium flex, Heat-moldable liner, Vibram outsole', 349.99, 35),
  (1007, 'Ski Poles', 'Accessories', 'Lightweight aluminum ski poles with ergonomic grips. Adjustable length for different terrain and snow conditions.', 'Aluminum construction, Adjustable length (105-135cm), Powder baskets, Padded straps', 79.99, 50),
  (1008, 'Ski Goggles', 'Accessories', 'Anti-fog ski goggles with interchangeable lenses. Superior optics and peripheral vision for all weather conditions.', 'Anti-fog coating, UV protection, Interchangeable lenses, Helmet compatible', 149.99, 40),
  (1009, 'Snowboard Bindings', 'Accessories', 'Responsive snowboard bindings with tool-free adjustment. Lightweight and compatible with all mounting systems.', 'Tool-free adjustment, Canted footbeds, Universal disk, Highback rotation', 249.99, 28),
  (1010, 'Ski Helmet', 'Accessories', 'Safety-certified ski helmet with integrated audio system. Lightweight construction with adjustable ventilation.', 'MIPS protection, Audio-ready, Adjustable vents, Goggle clip, Multiple sizes', 179.99, 45)
  ) AS s(product_id, product_name, product_category, description, features, price, stock_quantity)
) s
ON t.product_id = s.product_id
WHEN MATCHED THEN
  UPDATE SET
    product_name = s.product_name,
    product_category = s.product_category,
    description = s.description,
    features = s.features,
    price = s.price,
    stock_quantity = s.stock_quantity
WHEN NOT MATCHED THEN
  INSERT (product_id, product_name, product_category, description, features, price, stock_quantity)
  VALUES (s.product_id, s.product_name, s.product_category, s.description, s.features, s.price, s.stock_quantity);

-- ============================================================================
-- STEP 7: Cortex Search Service (Act 3 - Intelligence & Governance)
-- Purpose: Semantic search over product catalog for Snowflake CoWork
-- ============================================================================

-- Enable change tracking on product catalog for Cortex Search
ALTER TABLE product_catalog SET CHANGE_TRACKING = TRUE;

-- Create Cortex Search Service for semantic search over product descriptions
CREATE OR REPLACE CORTEX SEARCH SERVICE product_search_service
  ON description
  ATTRIBUTES product_id, product_name, product_category, features, price
  WAREHOUSE = hol_wh
  TARGET_LAG = '1 hour'
  AS (
    SELECT
      product_id,
      product_name,
      product_category,
      description,
      features,
      price,
      stock_quantity
    FROM product_catalog
  );

-- Enable change tracking on reviews and tickets for Agentic Search
ALTER TABLE product_reviews SET CHANGE_TRACKING = TRUE;
ALTER TABLE support_tickets SET CHANGE_TRACKING = TRUE;

-- Create a unified view for Agentic Search (combines reviews + tickets)
CREATE OR REPLACE VIEW customer_feedback AS
SELECT
    CAST(review_id AS VARCHAR) AS doc_id,
    review_title AS title,
    review_text AS content,
    'review' AS source_type,
    CAST(NULL AS VARCHAR(50)) AS category,
    rating,
    review_date AS date_field,
    customer_id
FROM product_reviews
UNION ALL
SELECT
    CAST(ticket_id AS VARCHAR) AS doc_id,
    subject AS title,
    description AS content,
    'ticket' AS source_type,
    category,
    CAST(NULL AS INT) AS rating,
    ticket_date::DATE AS date_field,
    customer_id
FROM support_tickets;

-- Multi-index Cortex Search for Agentic Search (reviews + tickets combined)
CREATE OR REPLACE CORTEX SEARCH SERVICE customer_feedback_search
  TEXT INDEXES title, doc_id
  VECTOR INDEXES content (model = 'snowflake-arctic-embed-l-v2.0')
  ATTRIBUTES source_type, category, rating, date_field, customer_id
  WAREHOUSE = hol_wh
  TARGET_LAG = '1 hour'
AS (
  SELECT doc_id, title, content, source_type, category, rating, date_field, customer_id
  FROM customer_feedback
);

-- Verify Cortex Search Services
SHOW CORTEX SEARCH SERVICES IN SCHEMA dash_automated_intelligence_db.raw;


-- ============================================================================
-- STEP 8: Semantic View (Act 3 - Intelligence & Governance)
-- Purpose: SQL-based semantic layer for Cortex Analyst natural language queries
-- ============================================================================

USE SCHEMA dash_automated_intelligence_db.semantic;

-- Create Semantic View for business analytics
-- Note: Semantic Views use SQL syntax (not YAML) and enable natural language queries
-- Clause order MUST be: TABLES → RELATIONSHIPS → FACTS → DIMENSIONS → METRICS
CREATE OR REPLACE SEMANTIC VIEW business_analytics_semantic
TABLES (
    orders AS dash_automated_intelligence_db.raw.orders
        PRIMARY KEY (order_id)
        WITH SYNONYMS = ('sales', 'transactions', 'purchases')
        COMMENT = 'Customer orders with amounts and status',
    
    customers AS dash_automated_intelligence_db.raw.customers
        PRIMARY KEY (customer_id)
        WITH SYNONYMS = ('buyers', 'clients', 'users')
        COMMENT = 'Customer master data with segments',
    
    items AS dash_automated_intelligence_db.raw.order_items
        PRIMARY KEY (order_item_id)
        WITH SYNONYMS = ('line_items', 'order_details', 'products_ordered')
        COMMENT = 'Individual items within each order'
)
RELATIONSHIPS (
    orders (customer_id) REFERENCES customers,
    items (order_id) REFERENCES orders
)
FACTS (
    orders.total_amount AS total_amount
        WITH SYNONYMS = ('order_value', 'sale_amount', 'revenue')
        COMMENT = 'Total monetary value of the order',
    
    items.quantity AS quantity
        WITH SYNONYMS = ('qty', 'units', 'count')
        COMMENT = 'Number of units ordered',
    
    items.unit_price AS unit_price
        WITH SYNONYMS = ('price', 'cost', 'item_price')
        COMMENT = 'Price per unit',
    
    items.line_total AS line_total
        WITH SYNONYMS = ('item_total', 'line_amount', 'item_revenue')
        COMMENT = 'Total value of the line item (quantity * unit_price)'
)
DIMENSIONS (
    orders.order_status AS order_status
        WITH SYNONYMS = ('status', 'state')
        COMMENT = 'Current status of the order',
    
    orders.order_date AS order_date
        WITH SYNONYMS = ('date', 'purchase_date', 'transaction_date')
        COMMENT = 'Date when order was placed',
    
    customers.customer_segment AS customer_segment
        WITH SYNONYMS = ('segment', 'tier', 'category')
        COMMENT = 'Customer classification tier',
    
    customers.first_name AS first_name
        COMMENT = 'Customer first name',
    
    customers.last_name AS last_name
        COMMENT = 'Customer last name',
    
    customers.state AS state
        WITH SYNONYMS = ('region', 'location', 'geography')
        COMMENT = 'US state where the customer is located',
    
    items.product_category AS product_category
        WITH SYNONYMS = ('category', 'product_type', 'item_category')
        COMMENT = 'Product category classification'
)
METRICS (
    orders.total_revenue AS SUM(total_amount)
        WITH SYNONYMS = ('revenue', 'sales', 'total_sales')
        COMMENT = 'Sum of all order amounts',
    
    orders.order_count AS COUNT(DISTINCT orders.order_id)
        WITH SYNONYMS = ('number_of_orders', 'total_orders')
        COMMENT = 'Count of unique orders',
    
    customers.customer_count AS COUNT(DISTINCT customers.customer_id)
        WITH SYNONYMS = ('number_of_customers', 'total_customers')
        COMMENT = 'Count of unique customers',
    
    orders.average_order_value AS AVG(total_amount)
        WITH SYNONYMS = ('aov', 'avg_order', 'mean_order_value')
        COMMENT = 'Average monetary value per order',
    
    items.total_items_revenue AS SUM(line_total)
        WITH SYNONYMS = ('item_revenue', 'product_revenue', 'line_item_sales')
        COMMENT = 'Sum of all line item totals'
)
COMMENT = 'Semantic layer for natural language business analytics queries'

AI_VERIFIED_QUERIES (
    monthly_revenue_trend AS (
        QUESTION 'Show me monthly revenue trend from August to November 2026'
        VERIFIED_AT 1772645863
        ONBOARDING_QUESTION TRUE
        VERIFIED_BY '(STEWARD = dash)'
        SQL 'SELECT
                 DATE_TRUNC(''month'', o.order_date) AS month,
                 SUM(o.total_amount) AS total_revenue
             FROM orders AS o
             WHERE o.order_date >= ''2026-08-01''
               AND o.order_date < ''2027-01-01''
             GROUP BY DATE_TRUNC(''month'', o.order_date)
             ORDER BY month'
    ),
    revenue_and_customers_by_state AS (
        QUESTION 'What is our total revenue and customer count by state?'
        VERIFIED_AT 1772645863
        ONBOARDING_QUESTION TRUE
        VERIFIED_BY '(STEWARD = dash)'
        SQL 'SELECT
                 c.state,
                 SUM(o.total_amount) AS total_revenue,
                 COUNT(DISTINCT c.customer_id) AS customer_count
             FROM orders AS o
             JOIN customers AS c ON o.customer_id = c.customer_id
             GROUP BY c.state
             ORDER BY total_revenue DESC'
    ),
    october_revenue_comparison AS (
        QUESTION 'What was the revenue in October 2026 compared to September 2026?'
        VERIFIED_AT 1772645863
        ONBOARDING_QUESTION FALSE
        VERIFIED_BY '(STEWARD = dash)'
        SQL 'SELECT
                 DATE_TRUNC(''month'', o.order_date) AS month,
                 SUM(o.total_amount) AS total_revenue
             FROM orders AS o
             WHERE o.order_date >= ''2026-09-01''
               AND o.order_date < ''2026-11-01''
             GROUP BY DATE_TRUNC(''month'', o.order_date)
             ORDER BY month'
    )
);

-- Verify Semantic View creation
SHOW SEMANTIC VIEWS IN SCHEMA dash_automated_intelligence_db.semantic;
DESCRIBE SEMANTIC VIEW business_analytics_semantic;


-- ============================================================================
-- Create Stages
-- ============================================================================

CREATE STAGE IF NOT EXISTS dash_automated_intelligence_db.raw.semantic_models
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for storing semantic model YAML files for Cortex Analyst';

CREATE STAGE IF NOT EXISTS dash_automated_intelligence_db.raw.the_dashboard_stage
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Streamlit dashboard application files';

-- ============================================================================
-- Load Seed Data from S3 (Parquet)
-- ============================================================================

CREATE OR REPLACE FILE FORMAT dash_automated_intelligence_db.raw.parquet_format
    TYPE = 'PARQUET';

CREATE OR REPLACE FILE FORMAT dash_automated_intelligence_db.raw.csv_format
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('');

CREATE OR REPLACE STAGE dash_automated_intelligence_db.raw.hol_data_stage
    URL = 's3://sfquickstarts/summit_dev_day_2026_automated_intelligence_hol/'
    FILE_FORMAT = dash_automated_intelligence_db.raw.parquet_format;

-- Suspend DTs before loading data to prevent them from refreshing with pre-shift dates
ALTER DYNAMIC TABLE dash_automated_intelligence_db.dynamic_tables.enriched_orders SUSPEND;
ALTER DYNAMIC TABLE dash_automated_intelligence_db.dynamic_tables.enriched_order_items SUSPEND;

COPY INTO dash_automated_intelligence_db.raw.customers
FROM @dash_automated_intelligence_db.raw.hol_data_stage/parquet/
FILE_FORMAT = (FORMAT_NAME = 'dash_automated_intelligence_db.raw.parquet_format')
PATTERN = '.*customers.*\.parquet'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FORCE = TRUE;

EXECUTE IMMEDIATE $$
DECLARE
  scale VARCHAR DEFAULT (SELECT $data_scale);
BEGIN
  IF (scale = '50M') THEN
    COPY INTO dash_automated_intelligence_db.raw.orders
    FROM @dash_automated_intelligence_db.raw.hol_data_stage/parquet/
    FILE_FORMAT = (FORMAT_NAME = 'dash_automated_intelligence_db.raw.parquet_format')
    PATTERN = '.*orders_.*\.parquet'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE;
  ELSE
    COPY INTO dash_automated_intelligence_db.raw.orders
    FROM @dash_automated_intelligence_db.raw.hol_data_stage/parquet/
    FILE_FORMAT = (FORMAT_NAME = 'dash_automated_intelligence_db.raw.parquet_format')
    PATTERN = '.*orders_00[12]\.parquet'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE;
  END IF;
END;
$$;

EXECUTE IMMEDIATE $$
DECLARE
  scale VARCHAR DEFAULT (SELECT $data_scale);
BEGIN
  IF (scale = '50M') THEN
    COPY INTO dash_automated_intelligence_db.raw.order_items
    FROM @dash_automated_intelligence_db.raw.hol_data_stage/parquet/
    FILE_FORMAT = (FORMAT_NAME = 'dash_automated_intelligence_db.raw.parquet_format')
    PATTERN = '.*order_items_.*\.parquet'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE;
  ELSE
    COPY INTO dash_automated_intelligence_db.raw.order_items
    FROM @dash_automated_intelligence_db.raw.hol_data_stage/parquet/
    FILE_FORMAT = (FORMAT_NAME = 'dash_automated_intelligence_db.raw.parquet_format')
    PATTERN = '.*order_items_00[12]\.parquet'
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE;
  END IF;
END;
$$;

COPY INTO dash_automated_intelligence_db.raw.product_reviews
FROM @dash_automated_intelligence_db.raw.hol_data_stage/
FILE_FORMAT = (FORMAT_NAME = 'dash_automated_intelligence_db.raw.csv_format')
PATTERN = '.*product_reviews\.csv'
FORCE = TRUE;

COPY INTO dash_automated_intelligence_db.raw.support_tickets
FROM @dash_automated_intelligence_db.raw.hol_data_stage/
FILE_FORMAT = (FORMAT_NAME = 'dash_automated_intelligence_db.raw.csv_format')
PATTERN = '.*support_tickets\.csv'
FORCE = TRUE;

-- ============================================================================
-- Shift dates forward 14 months (Jun-Sep 2025 → Aug-Dec 2026)
-- Makes the dataset feel current for future HOL sessions
-- ============================================================================
UPDATE dash_automated_intelligence_db.raw.orders
SET order_date = DATEADD(month, 14, order_date);

UPDATE dash_automated_intelligence_db.raw.product_reviews
SET review_date = DATEADD(month, 14, review_date);

UPDATE dash_automated_intelligence_db.raw.support_tickets
SET ticket_date = DATEADD(month, 14, ticket_date);

-- Trim reviews/tickets that shifted beyond the order window (Aug-Nov 2026)
DELETE FROM dash_automated_intelligence_db.raw.product_reviews WHERE review_date >= '2026-12-01';
DELETE FROM dash_automated_intelligence_db.raw.support_tickets WHERE ticket_date >= '2026-12-01';

-- Resume DTs now that source data has correct dates
ALTER DYNAMIC TABLE dash_automated_intelligence_db.dynamic_tables.enriched_orders RESUME;
ALTER DYNAMIC TABLE dash_automated_intelligence_db.dynamic_tables.enriched_order_items RESUME;

-- ============================================================================
-- Inject Product Affinity Patterns
-- Purpose: Create realistic co-purchase patterns so product affinity models
-- show meaningful "frequently bought together" relationships
-- ============================================================================

-- Ski gear affinity: Orders with Skis get Ski Boots swapped into an Accessories slot
UPDATE dash_automated_intelligence_db.raw.order_items oi
SET product_id = 1005, product_name = 'Ski Boots', product_category = 'Boots',
    unit_price = 449.99, line_total = 449.99 * quantity
FROM (
    SELECT oi2.order_item_id
    FROM dash_automated_intelligence_db.raw.order_items oi2
    INNER JOIN dash_automated_intelligence_db.raw.order_items ski
        ON oi2.order_id = ski.order_id AND ski.product_id IN (1001, 1002)
    WHERE oi2.product_category = 'Accessories'
      AND oi2.product_id NOT IN (1001, 1002, 1005)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY oi2.order_id ORDER BY RANDOM()) = 1
) matched
WHERE oi.order_item_id = matched.order_item_id
  AND RANDOM() < 0.4 * 2147483647;

-- Snowboard gear affinity: Orders with Snowboards get Snowboard Boots swapped in
UPDATE dash_automated_intelligence_db.raw.order_items oi
SET product_id = 1006, product_name = 'Snowboard Boots', product_category = 'Boots',
    unit_price = 349.99, line_total = 349.99 * quantity
FROM (
    SELECT oi2.order_item_id
    FROM dash_automated_intelligence_db.raw.order_items oi2
    INNER JOIN dash_automated_intelligence_db.raw.order_items sb
        ON oi2.order_id = sb.order_id AND sb.product_id IN (1003, 1004)
    WHERE oi2.product_category = 'Accessories'
      AND oi2.product_id NOT IN (1003, 1004, 1006)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY oi2.order_id ORDER BY RANDOM()) = 1
) matched
WHERE oi.order_item_id = matched.order_item_id
  AND RANDOM() < 0.4 * 2147483647;

-- ============================================================================
-- Inject Customer Frequency Variation
-- Purpose: Create purchase frequency variation so customer segmentation
-- produces meaningful champions/loyal/at-risk segments (~1% champions,
-- ~3% loyal, ~67% potential_loyalists, ~27% promising, ~2% lost)
-- ============================================================================

-- Champions: 5% of Premium customers (customer_id <= 100K) get 10 extra orders
INSERT INTO dash_automated_intelligence_db.raw.orders
    (order_id, customer_id, order_date, order_status, total_amount, discount_percent, shipping_cost)
SELECT
    UUID_STRING(),
    c.customer_id,
    DATEADD(day, UNIFORM(0, 117, RANDOM()), '2026-08-05'::DATE),
    'Completed',
    UNIFORM(500, 3000, RANDOM())::DECIMAL(10,2),
    CASE WHEN RANDOM() > 0.9 * 2147483647 THEN UNIFORM(5, 10, RANDOM()) ELSE 0 END,
    UNIFORM(5, 50, RANDOM())::DECIMAL(10,2)
FROM dash_automated_intelligence_db.raw.customers c
CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 10)) g
WHERE c.customer_segment = 'Premium'
  AND c.customer_id <= 100000;

-- Loyal: Standard customers (100K-300K) get 5 extra orders
INSERT INTO dash_automated_intelligence_db.raw.orders
    (order_id, customer_id, order_date, order_status, total_amount, discount_percent, shipping_cost)
SELECT
    UUID_STRING(),
    c.customer_id,
    DATEADD(day, UNIFORM(0, 117, RANDOM()), '2026-08-05'::DATE),
    'Completed',
    UNIFORM(100, 800, RANDOM())::DECIMAL(10,2),
    CASE WHEN RANDOM() > 0.6 * 2147483647 THEN UNIFORM(5, 20, RANDOM()) ELSE 0 END,
    UNIFORM(5, 50, RANDOM())::DECIMAL(10,2)
FROM dash_automated_intelligence_db.raw.customers c
CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 5)) g
WHERE c.customer_segment = 'Standard'
  AND c.customer_id BETWEEN 100001 AND 300000;

-- At-risk: cancel most orders for 2000 Basic customers
UPDATE dash_automated_intelligence_db.raw.orders
SET order_status = 'Cancelled'
WHERE customer_id IN (
    SELECT customer_id
    FROM dash_automated_intelligence_db.raw.customers
    WHERE customer_segment = 'Basic'
    ORDER BY customer_id
    LIMIT 2000
)
AND order_status != 'Cancelled'
AND RANDOM() < 0.7 * 2147483647;

-- ============================================================================
-- Inject Data Quality Issues (intentional NULLs for DMF demo)
-- Purpose: ~200 NULL values in monitored columns so DMFs detect violations
-- ============================================================================

CREATE OR REPLACE TEMPORARY TABLE dash_automated_intelligence_db.raw.dq_bad_orders AS
SELECT order_id FROM dash_automated_intelligence_db.raw.orders SAMPLE (200 ROWS);

UPDATE dash_automated_intelligence_db.raw.orders o
SET total_amount = NULL
FROM dash_automated_intelligence_db.raw.dq_bad_orders b
WHERE o.order_id = b.order_id;

CREATE OR REPLACE TEMPORARY TABLE dash_automated_intelligence_db.raw.dq_bad_items AS
SELECT order_item_id FROM dash_automated_intelligence_db.raw.order_items SAMPLE (200 ROWS);

UPDATE dash_automated_intelligence_db.raw.order_items oi
SET quantity = NULL
FROM dash_automated_intelligence_db.raw.dq_bad_items b
WHERE oi.order_item_id = b.order_item_id;

-- Also inject NULLs into product_name (NOT monitored by DMF — intentional gap)
CREATE OR REPLACE TEMPORARY TABLE dash_automated_intelligence_db.raw.dq_bad_items_names AS
SELECT order_item_id FROM dash_automated_intelligence_db.raw.order_items SAMPLE (150 ROWS);

UPDATE dash_automated_intelligence_db.raw.order_items oi
SET product_name = NULL
FROM dash_automated_intelligence_db.raw.dq_bad_items_names b
WHERE oi.order_item_id = b.order_item_id;

-- Fire the alert immediately so results are available by the Data Quality section
-- NOTE: EXECUTE ALERT queries vw_dq_monitoring_results, which relies on DMF measurements
-- from TRIGGER_ON_CHANGES being committed to SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS.
-- That write is asynchronous and is not guaranteed to complete before EXECUTE ALERT runs,
-- so the alert condition may evaluate to FALSE and insert nothing.
-- Fix: seed data_quality_alerts directly using inline DMF calls (synchronous) so the table
-- is populated immediately. The 5-minute scheduled alert then keeps it up to date.
INSERT INTO dash_automated_intelligence_db.raw.data_quality_alerts (alert_time, issue_summary)
SELECT
    CURRENT_TIMESTAMP(),
    'Data Quality Issue: NULL values detected - '
    || SNOWFLAKE.CORE.NULL_COUNT(SELECT total_amount FROM dash_automated_intelligence_db.raw.orders)::TEXT
    || ' NULL total_amounts in ORDERS, '
    || SNOWFLAKE.CORE.NULL_COUNT(SELECT quantity FROM dash_automated_intelligence_db.raw.order_items)::TEXT
    || ' NULL quantities in ORDER_ITEMS';

EXECUTE ALERT dash_automated_intelligence_db.raw.data_quality_alert;

-- ============================================================================
-- Refresh Dynamic Tables (now that seed data is loaded)
-- ============================================================================

ALTER DYNAMIC TABLE dash_automated_intelligence_db.dynamic_tables.enriched_orders REFRESH;
ALTER DYNAMIC TABLE dash_automated_intelligence_db.dynamic_tables.enriched_order_items REFRESH;
ALTER DYNAMIC TABLE dash_automated_intelligence_db.dynamic_tables.fact_orders REFRESH;
ALTER DYNAMIC TABLE dash_automated_intelligence_db.dynamic_tables.daily_business_metrics REFRESH;
ALTER DYNAMIC TABLE dash_automated_intelligence_db.dynamic_tables.product_performance_metrics REFRESH;

-- ============================================================================
-- Refresh Cortex Search Services (index the loaded data immediately)
-- ============================================================================

ALTER CORTEX SEARCH SERVICE dash_automated_intelligence_db.raw.product_search_service REFRESH;
ALTER CORTEX SEARCH SERVICE dash_automated_intelligence_db.raw.customer_feedback_search REFRESH;

-- ============================================================================
-- Create Interactive Tables (after data load so they capture all 50M rows)
-- Purpose: Sub-100ms queries for high-concurrency serving (GA Dec 2025)
-- ============================================================================

USE SCHEMA dash_automated_intelligence_db.interactive;
USE WAREHOUSE hol_wh;

CREATE OR REPLACE INTERACTIVE TABLE customer_order_analytics
  CLUSTER BY (customer_id)
AS
SELECT 
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.customer_segment,
  COUNT(DISTINCT o.order_id) as total_orders,
  SUM(o.total_amount) as total_spent,
  AVG(o.total_amount) as avg_order_value,
  MIN(o.order_date) as first_order_date,
  MAX(o.order_date) as last_order_date
FROM dash_automated_intelligence_db.raw.customers c
INNER JOIN dash_automated_intelligence_db.raw.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.customer_segment;

CREATE OR REPLACE INTERACTIVE TABLE order_lookup
  CLUSTER BY (order_id)
AS
SELECT 
  o.order_id,
  o.customer_id,
  o.order_date,
  o.order_status,
  o.total_amount,
  c.first_name,
  c.last_name,
  c.email
FROM dash_automated_intelligence_db.raw.orders o
INNER JOIN dash_automated_intelligence_db.raw.customers c ON o.customer_id = c.customer_id;

CREATE OR REPLACE INTERACTIVE WAREHOUSE hol_interactive_wh
  TABLES (customer_order_analytics, order_lookup)
  WAREHOUSE_SIZE = 'XSMALL';

ALTER WAREHOUSE IF EXISTS hol_interactive_wh RESUME IF SUSPENDED;

-- Switch back to standard warehouse for remaining operations
USE WAREHOUSE hol_wh;

-- ============================================================================
-- Verify Row Counts
-- ============================================================================

SELECT 'Raw Tables' AS category, 'customers' AS table_name, COUNT(*) AS row_count 
FROM dash_automated_intelligence_db.raw.customers
UNION ALL
SELECT 'Raw Tables', 'orders', COUNT(*) 
FROM dash_automated_intelligence_db.raw.orders
UNION ALL
SELECT 'Raw Tables', 'order_items', COUNT(*) 
FROM dash_automated_intelligence_db.raw.order_items
UNION ALL
SELECT 'Raw Tables', 'product_reviews', COUNT(*) 
FROM dash_automated_intelligence_db.raw.product_reviews
UNION ALL
SELECT 'Raw Tables', 'support_tickets', COUNT(*) 
FROM dash_automated_intelligence_db.raw.support_tickets;

-- ============================================================================
-- STEP: Row Access Policy & West Coast Manager Role
-- Purpose: Demonstrate region-based RBAC with the Business Insights Agent
-- ============================================================================

CREATE OR REPLACE ROLE WEST_COAST_MANAGER
    COMMENT = 'Regional manager with access limited to CA, OR, and WA states only';

SET current_user = (SELECT CURRENT_USER());
GRANT ROLE WEST_COAST_MANAGER TO USER IDENTIFIER($current_user);

GRANT USAGE ON DATABASE DASH_AUTOMATED_INTELLIGENCE_DB TO ROLE WEST_COAST_MANAGER;
GRANT USAGE ON SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.RAW TO ROLE WEST_COAST_MANAGER;
GRANT USAGE ON SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.DYNAMIC_TABLES TO ROLE WEST_COAST_MANAGER;
GRANT USAGE ON WAREHOUSE HOL_WH TO ROLE WEST_COAST_MANAGER;

GRANT SELECT ON ALL TABLES IN SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.RAW TO ROLE WEST_COAST_MANAGER;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.DYNAMIC_TABLES TO ROLE WEST_COAST_MANAGER;

-- Grant access to the Cortex Search services used by the Business Insights
-- Agent's search tools. Search services are not tables, so the SELECT grants
-- above do not cover them — USAGE must be granted separately, or the agent's
-- search_customer_feedback and search_products tools fail with access errors
-- when run as WEST_COAST_MANAGER.
GRANT USAGE ON CORTEX SEARCH SERVICE DASH_AUTOMATED_INTELLIGENCE_DB.RAW.CUSTOMER_FEEDBACK_SEARCH TO ROLE WEST_COAST_MANAGER;
GRANT USAGE ON CORTEX SEARCH SERVICE DASH_AUTOMATED_INTELLIGENCE_DB.RAW.PRODUCT_SEARCH_SERVICE TO ROLE WEST_COAST_MANAGER;

-- Grant SELECT on the CUSTOMER_FEEDBACK view, which is the base_table for the
-- multi-index search_customer_feedback tool (analytical search persists/reads
-- from it). SELECT ON ALL TABLES above does not cover views, so grant it
-- explicitly — without it the search tool fails when run as WEST_COAST_MANAGER.
GRANT SELECT ON VIEW DASH_AUTOMATED_INTELLIGENCE_DB.RAW.CUSTOMER_FEEDBACK TO ROLE WEST_COAST_MANAGER;

-- Grant access to the semantic view used by the Business Insights Agent's
-- query_business_data (Cortex Analyst text-to-SQL) tool. Schema USAGE alone
-- does not authorize the semantic view object. Semantic views use SELECT
-- (not USAGE), and Cortex Agents require the executing role to have SELECT on
-- BOTH the semantic view and its underlying tables. The underlying-table
-- SELECT is already covered by the SELECT ON ALL TABLES grant above; this
-- grants SELECT on the semantic view itself. Without it the agent's
-- text-to-SQL tool fails with "Semantic View ... not authorized" when run
-- as WEST_COAST_MANAGER.
GRANT SELECT ON SEMANTIC VIEW DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.BUSINESS_ANALYTICS_SEMANTIC TO ROLE WEST_COAST_MANAGER;

-- Create a dedicated demo user whose DEFAULT_ROLE is WEST_COAST_MANAGER.
-- Cortex Agents run with the querying user's DEFAULT role (not the active
-- Snowsight role), so switching role in Snowsight has no effect on the agent.
-- To verify Row Access Policies through CoWork, log into Snowsight as this
-- user, then ask the Business Insights Agent — its results filter to CA, OR, WA.
-- NOTE: No password is set here (to avoid committing a secret). Before first use,
-- set a login password out-of-band, e.g. in Snowsight or via:
--   ALTER USER west_coast_manager_user SET PASSWORD = '<your-choice>';
CREATE OR REPLACE USER west_coast_manager_user
    DEFAULT_ROLE = WEST_COAST_MANAGER
    DEFAULT_WAREHOUSE = HOL_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Demo user for verifying Row Access Policies through the Business Insights Agent';

GRANT ROLE WEST_COAST_MANAGER TO USER west_coast_manager_user;

USE DATABASE DASH_AUTOMATED_INTELLIGENCE_DB;
USE SCHEMA RAW;

CREATE OR REPLACE ROW ACCESS POLICY customers_region_policy
AS (state VARCHAR) RETURNS BOOLEAN ->
    CASE 
        WHEN CURRENT_ROLE() IN ('AUTOMATED_INTELLIGENCE_ADMIN', 'ACCOUNTADMIN') THEN TRUE
        WHEN CURRENT_ROLE() = 'WEST_COAST_MANAGER'
             AND state IN ('CA', 'OR', 'WA') THEN TRUE
        ELSE FALSE
    END
COMMENT = 'Restricts west_coast_manager to CA, OR, and WA states only';

ALTER TABLE DASH_AUTOMATED_INTELLIGENCE_DB.RAW.CUSTOMERS 
    ADD ROW ACCESS POLICY customers_region_policy ON (state);

-- ============================================================================
-- STEP: Agent Evaluation Dataset
-- Purpose: Ground-truth data for evaluating the Business Insights Agent
-- ============================================================================

USE SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC;

CREATE OR REPLACE TABLE agent_evaluation_data (
    input_query VARCHAR,
    ground_truth VARIANT
);

INSERT INTO agent_evaluation_data
SELECT 'Show me monthly revenue trend from August to November 2026',
       PARSE_JSON('{"ground_truth_output": "The agent should query structured business data using the query_business_data tool to produce a monthly revenue breakdown. The response should include revenue figures for each month from August 2026 through November 2026."}')
UNION ALL SELECT 'Which month had the lowest revenue, and what do customer reviews say about that period?',
       PARSE_JSON('{"ground_truth_output": "The agent should use BOTH query_business_data and search_customer_feedback. The response should identify the lowest-revenue month from structured data and connect it to qualitative customer feedback from reviews or tickets during that period."}')
UNION ALL SELECT 'Find reviews mentioning wrong size with a rating below 3',
       PARSE_JSON('{"ground_truth_output": "The agent should use search_customer_feedback to find product reviews that mention sizing issues with low ratings below 3. Results should include specific review content, product names, and ratings."}')
UNION ALL SELECT 'Why are customers returning ski boots?',
       PARSE_JSON('{"ground_truth_output": "The agent should search customer feedback related to ski boot returns. The response should identify common return reasons such as sizing issues, comfort problems, or quality concerns, citing specific customer feedback."}')
UNION ALL SELECT 'What is our total revenue and customer count by state?',
       PARSE_JSON('{"ground_truth_output": "The agent should use query_business_data to aggregate total revenue and distinct customer count grouped by state. The response should include states with their corresponding revenue and customer counts."}')
UNION ALL SELECT 'What are the top complaint themes in support tickets?',
       PARSE_JSON('{"ground_truth_output": "The agent should use search_customer_feedback to find and analyze support tickets. The response should identify main complaint themes with examples from actual tickets."}')
UNION ALL SELECT 'How many reviews mention sizing issues, and which products are most affected?',
       PARSE_JSON('{"ground_truth_output": "The agent should use search_customer_feedback to find reviews mentioning sizing issues. The response should provide a count and identify which products are most frequently mentioned in sizing complaints."}');

-- ============================================================================
-- CROSS-ROLE READ GRANTS (run after all pipeline tables exist)
-- The Streamlit dashboard is owned by ACCOUNTADMIN (snow streamlit deploy sets the
-- owner to the deploy connection's role) and dbt runs as AUTOMATED_INTELLIGENCE_ADMIN.
-- Grant AUTOMATED_INTELLIGENCE_ADMIN SELECT on the pipeline tables it reads as dbt
-- sources, so the dbt role / a fallback app role can access them. (dbt->ACCOUNTADMIN
-- direction is handled by the FUTURE grants above + the post-dbt ALL grants note.)
-- ============================================================================
GRANT USAGE ON SCHEMA dash_automated_intelligence_db.raw TO ROLE automated_intelligence_admin;
GRANT USAGE ON SCHEMA dash_automated_intelligence_db.dynamic_tables TO ROLE automated_intelligence_admin;
GRANT USAGE ON SCHEMA dash_automated_intelligence_db.interactive TO ROLE automated_intelligence_admin;
GRANT SELECT ON ALL TABLES IN SCHEMA dash_automated_intelligence_db.raw TO ROLE automated_intelligence_admin;
-- INSERT + UPDATE needed for merge_staging_to_raw (caller's rights proc merges streaming data into raw)
GRANT INSERT, UPDATE ON TABLE dash_automated_intelligence_db.raw.orders       TO ROLE automated_intelligence_admin;
GRANT INSERT, UPDATE ON TABLE dash_automated_intelligence_db.raw.order_items  TO ROLE automated_intelligence_admin;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA dash_automated_intelligence_db.dynamic_tables TO ROLE automated_intelligence_admin;
GRANT SELECT ON ALL TABLES IN SCHEMA dash_automated_intelligence_db.interactive TO ROLE automated_intelligence_admin;

-- ============================================================================
-- FINAL STEP: Deploy Streamlit Dashboard (run from CLI)
-- ============================================================================
-- Run the following from the repo root after this SQL completes:
--
--   cd streamlit-dashboard && snow streamlit deploy --replace -c <your-connection>
--
-- ============================================================================
