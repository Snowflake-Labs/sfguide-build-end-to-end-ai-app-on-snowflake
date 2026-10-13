-- ============================================================================
-- CLEANUP: Drop all HOL objects
-- ============================================================================
-- Run this after the lab to remove all objects created during the session.
-- Requires ACCOUNTADMIN role.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Remove the agent from the CoWork (Snowflake Intelligence) object, if the
-- account has one, so no dangling entry is left after the database is dropped.
EXECUTE IMMEDIATE $$
DECLARE
    si_name VARCHAR;
BEGIN
    SHOW SNOWFLAKE INTELLIGENCES;
    SELECT MAX("name") INTO :si_name FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    IF (si_name IS NULL) THEN
        RETURN 'No CoWork object; nothing to deregister.';
    END IF;
    EXECUTE IMMEDIATE 'ALTER SNOWFLAKE INTELLIGENCE ' || si_name ||
        ' DROP AGENT DASH_AUTOMATED_INTELLIGENCE_DB.SEMANTIC.BUSINESS_INSIGHTS_AGENT';
    RETURN 'Agent removed from CoWork object ' || si_name || '.';
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Agent was not registered in CoWork; nothing to remove.';
END;
$$;

DROP DATABASE IF EXISTS DASH_AUTOMATED_INTELLIGENCE_DB CASCADE;
DROP WAREHOUSE IF EXISTS HOL_WH;
DROP WAREHOUSE IF EXISTS HOL_GEN1_WH;
DROP WAREHOUSE IF EXISTS HOL_GEN2_WH;
DROP WAREHOUSE IF EXISTS HOL_INTERACTIVE_WH;
-- setup.sql set your default role to AUTOMATED_INTELLIGENCE_ADMIN; reset it
-- before that role is dropped.
SET current_user = (SELECT CURRENT_USER());
ALTER USER IDENTIFIER($current_user) SET DEFAULT_ROLE = ACCOUNTADMIN;
DROP ROLE IF EXISTS AUTOMATED_INTELLIGENCE_ADMIN;
-- Older versions of setup.sql created a separate RLS demo user.
DROP USER IF EXISTS west_coast_manager_user;
DROP ROLE IF EXISTS WEST_COAST_MANAGER;
