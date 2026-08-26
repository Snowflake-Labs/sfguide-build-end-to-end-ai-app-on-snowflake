-- ============================================================================
-- Iceberg Tables - Partitioned Writes Demo
-- ============================================================================
-- Snowflake manages Iceberg tables with automatic partitioning and
-- optimized writes. Available since 2025.
--
-- Key Features:
-- - Automatic partition pruning
-- - CLUSTER BY for write optimization
-- - Native Snowflake management with open format interoperability
-- ============================================================================

USE ROLE AUTOMATED_INTELLIGENCE_ADMIN;
USE DATABASE DASH_AUTOMATED_INTELLIGENCE_DB;
USE WAREHOUSE AUTOMATED_INTELLIGENCE_WH;

-- ============================================================================
-- PART 1: Create Iceberg Table with Partitioning
-- ============================================================================

-- Create schema for Iceberg tables
CREATE SCHEMA IF NOT EXISTS DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG;

-- Create Iceberg table with date partitioning
-- Note: Requires external volume and catalog integration for external Iceberg
-- This example shows managed Iceberg table (Snowflake-managed)
CREATE OR REPLACE ICEBERG TABLE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_PARTITIONED
    CLUSTER BY (order_year, order_month)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'my_iceberg_volume'  -- Replace with actual volume
    BASE_LOCATION = 'orders_partitioned/'
AS
SELECT 
    order_id,
    customer_id,
    order_date,
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    total_amount,
    order_status
FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS
LIMIT 1000;

-- Alternative: Managed Iceberg Table (Snowflake handles storage)
CREATE OR REPLACE ICEBERG TABLE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_MANAGED
    CATALOG = 'SNOWFLAKE'
    CLUSTER BY (YEAR(order_date), MONTH(order_date))
AS
SELECT 
    order_id,
    customer_id,
    order_date,
    total_amount,
    order_status
FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS
LIMIT 1000;

-- ============================================================================
-- PART 2: Verify Partitioning
-- ============================================================================

-- Check table properties
DESCRIBE TABLE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_PARTITIONED;

-- View Iceberg metadata
SHOW ICEBERG TABLES IN SCHEMA DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG;

-- Check clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION(
    'DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_PARTITIONED',
    '(order_year, order_month)'
);

-- ============================================================================
-- PART 3: Partitioned Insert (Optimized Writes)
-- ============================================================================

-- Insert new data - Snowflake optimizes writes based on CLUSTER BY
INSERT INTO DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_PARTITIONED
SELECT 
    order_id,
    customer_id,
    order_date,
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    total_amount,
    order_status
FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS
WHERE order_date >= '2026-08-01'
LIMIT 500;

-- ============================================================================
-- PART 4: Query with Partition Pruning
-- ============================================================================

-- This query benefits from partition pruning
-- Only scans partitions for Aug 2026
SELECT 
    order_year,
    order_month,
    COUNT(*) AS order_count,
    SUM(total_amount) AS revenue
FROM DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_PARTITIONED
WHERE order_year = 2026 AND order_month = 8
GROUP BY order_year, order_month;

-- Check query profile to verify partition pruning
-- Look for "Partitions scanned" vs "Partitions total"

-- ============================================================================
-- PART 5: Convert Existing Table to Iceberg
-- ============================================================================

-- Option 1: CREATE TABLE AS SELECT
CREATE OR REPLACE ICEBERG TABLE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.CUSTOMERS_ICEBERG
    CATALOG = 'SNOWFLAKE'
    CLUSTER BY (customer_segment, state)
AS
SELECT * FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.CUSTOMERS;

-- Option 2: ALTER TABLE (convert in place)
-- ALTER TABLE my_table CONVERT TO ICEBERG
-- Note: Check documentation for current support

-- ============================================================================
-- PART 6: Time Travel with Iceberg
-- ============================================================================

-- Iceberg tables support time travel via snapshots
SELECT * FROM DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_PARTITIONED
AT (TIMESTAMP => DATEADD(hour, -1, CURRENT_TIMESTAMP()));

-- View snapshot history
SELECT * FROM TABLE(DASH_AUTOMATED_INTELLIGENCE_DB.INFORMATION_SCHEMA.ICEBERG_TABLE_SNAPSHOT_HISTORY(
    TABLE_NAME => 'ORDERS_PARTITIONED'
))
ORDER BY COMMITTED_AT DESC
LIMIT 10;

-- ============================================================================
-- PART 7: Best Practices for Partitioned Iceberg Tables
-- ============================================================================

/*
1. PARTITION SELECTION:
   - Choose columns with low cardinality (date parts, categories)
   - Avoid high-cardinality columns (IDs, timestamps)
   - 1000-10000 partitions is typical sweet spot

2. CLUSTER BY STRATEGIES:
   - Time-series data: CLUSTER BY (YEAR(date), MONTH(date))
   - Geographic data: CLUSTER BY (region, country)
   - Multi-tenant: CLUSTER BY (tenant_id, date)

3. WRITE OPTIMIZATION:
   - Insert data in partition order when possible
   - Use larger batches (avoid many small inserts)
   - Let Snowflake auto-compact micro-partitions

4. QUERY OPTIMIZATION:
   - Always filter on partition columns first
   - Use explicit predicates (WHERE year = 2026, not WHERE YEAR(date) = 2026)
   - Check query profile for partition pruning

5. INTEROPERABILITY:
   - Use external volumes for cross-engine access
   - Iceberg format enables Spark/Presto/Trino queries
   - Snowflake manages compaction and optimization
*/

-- ============================================================================
-- PART 8: External Iceberg Table (Read from Existing Iceberg)
-- ============================================================================

-- Read existing Iceberg table from external storage
/*
CREATE OR REPLACE ICEBERG TABLE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.EXTERNAL_ORDERS
    EXTERNAL_VOLUME = 'my_s3_volume'
    CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 's3://my-bucket/iceberg/orders/'
    METADATA_FILE_PATH = 'metadata/v1.metadata.json';
*/

-- ============================================================================
-- ============================================================================
--
--  ICEBERG V3 FEATURES (Preview - March 2026)
--
--  Apache Iceberg v3 introduces three key capabilities:
--    1. Deletion Vectors  - Puffin files replace positional deletes
--    2. Row Lineage       - _row_id and _last_updated_sequence_number
--    3. Default Values    - Column-level defaults in the schema
--
--  ⚠️  V3 is currently in PREVIEW. The upgrade from v2 to v3 is ONE-WAY
--      and cannot be reversed.
--  ⚠️  V3 tables are NOT readable by pg_lake (DuckDB v1.3.2 supports v1/v2
--      only). Do NOT upgrade tables consumed by pg_lake.
--
-- ============================================================================
-- ============================================================================

-- ============================================================================
-- PART 9: Create an Iceberg V3 Table
-- ============================================================================
-- Setting FORMAT_VERSION = 3 creates a v3 table with deletion vectors
-- and row lineage enabled automatically.

CREATE OR REPLACE ICEBERG TABLE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'my_iceberg_volume'  -- Replace with actual volume
    BASE_LOCATION = 'orders_v3/'
    FORMAT_VERSION = 3
AS
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    order_status
FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS
LIMIT 500;

-- Verify the table was created as v3
DESCRIBE ICEBERG TABLE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3;

-- Check row count after initial load
SELECT COUNT(*) AS initial_row_count
FROM DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3;

-- ============================================================================
-- PART 10: Deletion Vectors (Merge-on-Read)
-- ============================================================================
-- In v2, UPDATE/DELETE/MERGE rewrites entire data files (copy-on-write).
-- In v3, these operations write lightweight Puffin "deletion vector" files
-- that mark rows as deleted without rewriting the original Parquet files.
-- This dramatically reduces write amplification for update-heavy workloads.

-- UPDATE: marks old row values via deletion vector, writes new values
UPDATE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
SET    order_status = 'UPDATED_V3',
       total_amount = total_amount * 1.10
WHERE  order_id IN (
    SELECT order_id
    FROM DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
    LIMIT 10
);

-- DELETE: writes a deletion vector instead of rewriting the data file
DELETE FROM DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
WHERE  order_status = 'CANCELLED'
AND    order_id IN (
    SELECT order_id
    FROM DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
    WHERE order_status = 'CANCELLED'
    LIMIT 5
);

-- MERGE: benefits most from deletion vectors — mixed insert/update/delete
-- operations generate small Puffin files instead of full file rewrites
MERGE INTO DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3 AS target
USING (
    SELECT
        order_id,
        customer_id,
        order_date,
        total_amount * 1.05 AS total_amount,
        'MERGED_V3' AS order_status
    FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS
    LIMIT 20
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET
        target.total_amount  = source.total_amount,
        target.order_status  = source.order_status
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_date, total_amount, order_status)
    VALUES (source.order_id, source.customer_id, source.order_date,
            source.total_amount, source.order_status);

-- Verify the results of deletion-vector-backed operations
SELECT order_status, COUNT(*) AS cnt
FROM DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
GROUP BY order_status
ORDER BY cnt DESC;

-- ============================================================================
-- PART 11: Row Lineage
-- ============================================================================
-- V3 tables automatically track two metadata fields on every row:
--   _row_id                         – unique identifier per row
--   _last_updated_sequence_number   – commit sequence that last touched the row
--
-- These enable efficient incremental processing and change-data-capture
-- without external CDC tooling.

-- Query row lineage fields alongside business data
SELECT
    order_id,
    order_status,
    total_amount,
    _row_id,
    _last_updated_sequence_number
FROM DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
ORDER BY _last_updated_sequence_number DESC, _row_id
LIMIT 20;

-- Identify rows changed after a specific commit sequence
-- (useful for incremental ETL — only process rows modified since last run)
SELECT
    order_id,
    order_status,
    _row_id,
    _last_updated_sequence_number
FROM DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
WHERE _last_updated_sequence_number > 1
ORDER BY _last_updated_sequence_number, _row_id;

-- Count rows by their last-update sequence to see the commit distribution
SELECT
    _last_updated_sequence_number AS commit_seq,
    COUNT(*)                      AS rows_touched
FROM DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
GROUP BY _last_updated_sequence_number
ORDER BY commit_seq;

-- ============================================================================
-- PART 12: Default Values & V2-to-V3 Upgrade
-- ============================================================================
-- V3 supports column-level default values in the schema.
-- When a new column is added with a DEFAULT, existing rows return the
-- default without a backfill rewrite.

ALTER ICEBERG TABLE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
    ADD COLUMN priority VARCHAR DEFAULT 'STANDARD';

-- New inserts pick up the default automatically
INSERT INTO DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
    (order_id, customer_id, order_date, total_amount, order_status)
VALUES
    (999901, 'CUST_V3_A', CURRENT_DATE, 150.00, 'NEW'),
    (999902, 'CUST_V3_B', CURRENT_DATE, 275.50, 'NEW');

-- Existing rows return the default; new rows show explicit or default value
SELECT order_id, order_status, priority
FROM DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3
WHERE order_id >= 999901
   OR priority IS NOT NULL
ORDER BY order_id DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Upgrading an existing V2 table to V3 (one-way, cannot downgrade)
-- ---------------------------------------------------------------------------
-- ⚠️  Only upgrade tables that are NOT consumed by v2-only readers (e.g. pg_lake).
-- The upgrade is atomic and does not rewrite data files.
/*
ALTER ICEBERG TABLE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_PARTITIONED
    SET FORMAT_VERSION = 3;
*/

-- ============================================================================
-- PART 13: Dynamic Iceberg Tables (GA April 2026)
-- ============================================================================
-- Dynamic Iceberg tables now support PARTITION BY, TARGET_FILE_SIZE, and
-- PATH_LAYOUT for optimized write patterns and cross-engine interoperability.
-- Also: table/column descriptions propagate from external catalogs (GA Apr 2026).

-- Dynamic Iceberg table with partitioning and file size control
CREATE OR REPLACE DYNAMIC ICEBERG TABLE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.DAILY_ORDERS_ICEBERG
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'my_iceberg_volume'  -- Replace with actual volume
    BASE_LOCATION = 'daily_orders_dynamic/'
    TARGET_LAG = '1 hour'
    WAREHOUSE = AUTOMATED_INTELLIGENCE_WH
    PARTITION BY (order_year, order_month)
    TARGET_FILE_SIZE = 256  -- MB, controls Parquet file size for downstream readers
AS
SELECT
    order_id,
    customer_id,
    order_date,
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    total_amount,
    order_status
FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS;

-- Dynamic Iceberg table with PATH_LAYOUT for Hive-compatible partition paths
-- (enables direct reads from Spark/Trino/Presto without metadata catalog)
/*
CREATE OR REPLACE DYNAMIC ICEBERG TABLE DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.HIVE_COMPAT_ORDERS
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'my_iceberg_volume'
    BASE_LOCATION = 'hive_orders/'
    TARGET_LAG = '1 hour'
    WAREHOUSE = AUTOMATED_INTELLIGENCE_WH
    PARTITION BY (order_year, order_month)
    PATH_LAYOUT = 'order_year={order_year}/order_month={order_month}'
    TARGET_FILE_SIZE = 128
AS
SELECT
    order_id,
    customer_id,
    order_date,
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    total_amount
FROM DASH_AUTOMATED_INTELLIGENCE_DB.RAW.ORDERS;
*/


-- ============================================================================
-- PART 14: Cleanup & Compatibility Notes
-- ============================================================================

-- Drop the v3 demo table (optional — uncomment to clean up)
-- DROP ICEBERG TABLE IF EXISTS DASH_AUTOMATED_INTELLIGENCE_DB.ICEBERG.ORDERS_V3;

-- ---------------------------------------------------------------------------
-- Compatibility matrix (as of April 2026):
-- ---------------------------------------------------------------------------
-- | Reader / Engine           | v1  | v2  | v3  |
-- |---------------------------|-----|-----|-----|
-- | Snowflake (managed)       | yes | yes | yes (Preview) |
-- | pg_lake / DuckDB v1.3.2   | yes | yes | no            |
-- | DuckDB v1.4+              | yes | yes | no  (roadmap)  |
-- | Apache Spark 3.5+         | yes | yes | yes            |
-- | Starburst Galaxy          | yes | yes | yes            |
-- | Trino (open-source)       | yes | yes | no  (roadmap)  |
-- ---------------------------------------------------------------------------
--
-- Key takeaways:
--   - V3 is ideal for Snowflake-only or Spark-based pipelines today.
--   - Do NOT upgrade tables consumed by pg_lake until DuckDB ships v3 support.
--   - Deletion vectors reduce write amplification for MERGE-heavy workloads.
--   - Row lineage (_row_id, _last_updated_sequence_number) enables lightweight
--     incremental processing without external CDC infrastructure.
--   - The v2 to v3 upgrade is atomic and instant, but irreversible.
--   - Dynamic Iceberg tables with PARTITION BY + TARGET_FILE_SIZE are GA Apr 2026.
--   - Table/column descriptions from external catalogs (Unity, Glue) now
--     propagate into Snowflake automatically (GA Apr 2026).
-- ---------------------------------------------------------------------------

-- ============================================================================
-- Demo Complete
-- ============================================================================
SELECT 'Iceberg Partitioned Writes Demo Complete (including V3 Preview + Dynamic Iceberg)' AS status;
