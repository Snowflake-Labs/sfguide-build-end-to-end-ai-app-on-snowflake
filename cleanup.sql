-- ============================================================================
-- CLEANUP: Drop all HOL objects
-- ============================================================================
-- Run this after the lab to remove all objects created during the session.
-- Requires ACCOUNTADMIN role.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

DROP DATABASE IF EXISTS DASH_AUTOMATED_INTELLIGENCE_DB CASCADE;
DROP WAREHOUSE IF EXISTS HOL_WH;
DROP WAREHOUSE IF EXISTS HOL_GEN2_WH;
DROP WAREHOUSE IF EXISTS HOL_INTERACTIVE_WH;
DROP ROLE IF EXISTS AUTOMATED_INTELLIGENCE_ADMIN;
DROP USER IF EXISTS west_coast_manager_user;
DROP ROLE IF EXISTS WEST_COAST_MANAGER;
