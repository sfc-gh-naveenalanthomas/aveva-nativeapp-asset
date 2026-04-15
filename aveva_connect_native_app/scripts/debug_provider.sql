-- =============================================================================
-- AVEVA CONNECT - Provider-Side Debug Script (POLARIS1)
-- =============================================================================
-- Run on the PROVIDER account (where the app package + CLD live).
-- This script replicates what ONBOARD_CUSTOMER() does, step-by-step,
-- so you can isolate which step fails.
--
-- Replace <CUSTOMER_NAME> with the actual customer name (e.g. POLARIS2_TEST)
-- Replace <CLD_DB> with the CLD database name (e.g. AVEVA_POLARIS2_TEST_DB)
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- -------------------------------------------
-- 0. VARIABLES (edit these)
-- -------------------------------------------
SET CUSTOMER_NAME  = 'POLARIS2_TEST';
SET APP_PKG        = 'AVEVA_CONNECT_APP_PKG';
SET CLD_DB         = 'AVEVA_POLARIS2_TEST_DB';

-- -------------------------------------------
-- 1. CHECK APP PACKAGE EXISTS
-- -------------------------------------------
SHOW APPLICATION PACKAGES LIKE 'AVEVA_CONNECT_APP_PKG';

-- Check versions & current release directive
SHOW VERSIONS IN APPLICATION PACKAGE IDENTIFIER($APP_PKG);

-- Check stage has all required files
LIST @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE;
-- Expected: manifest.yml, setup.sql, README.md, streamlit/main.py, streamlit/environment.yml

-- -------------------------------------------
-- 2. CHECK CATALOG INTEGRATION
-- -------------------------------------------
SHOW CATALOG INTEGRATIONS;
-- Look for AVEVA_<CUSTOMER_NAME>_CATALOG_INT
-- Verify: ENABLED = TRUE, CATALOG_SOURCE = ICEBERG_REST

-- Describe it for details
-- DESCRIBE CATALOG INTEGRATION AVEVA_POLARIS2_TEST_CATALOG_INT;

-- -------------------------------------------
-- 3. CHECK CLD DATABASE
-- -------------------------------------------
SHOW DATABASES LIKE '%AVEVA%';
-- Verify your CLD database exists and is kind = CATALOG-LINKED DATABASE

-- Check CLD tables have synced
SHOW TABLES IN DATABASE IDENTIFIER($CLD_DB);
-- If 0 tables: CLD hasn't synced yet. Wait or check catalog integration.

-- Verify you can query a CLD table (pick one from SHOW TABLES above)
-- SELECT COUNT(*) FROM AVEVA_POLARIS2_TEST_DB."<schema>"."<table_name>" LIMIT 1;

-- -------------------------------------------
-- 4. CHECK PROXY VIEWS IN APP PACKAGE
-- -------------------------------------------
SHOW VIEWS IN SCHEMA AVEVA_CONNECT_APP_PKG.PROXY_VIEWS;
-- Should show secure views, one per CLD table

-- Verify proxy views are queryable (pick one)
-- SELECT * FROM AVEVA_CONNECT_APP_PKG.PROXY_VIEWS."<view_name>" LIMIT 5;

-- Verify proxy views are granted to the share
SHOW GRANTS ON SCHEMA AVEVA_CONNECT_APP_PKG.PROXY_VIEWS;
SHOW GRANTS TO SHARE IN APPLICATION PACKAGE AVEVA_CONNECT_APP_PKG;

-- -------------------------------------------
-- 5. CHECK SHARED_CONTENT SCHEMA
-- -------------------------------------------
SHOW VIEWS IN SCHEMA AVEVA_CONNECT_APP_PKG.SHARED_CONTENT;
-- Should have ACCOUNT_VIEW_MAP

SELECT * FROM AVEVA_CONNECT_APP_PKG.SHARED_CONTENT.ACCOUNT_VIEW_MAP;
-- Should show consumer account -> view pattern mapping

-- -------------------------------------------
-- 6. CHECK REFERENCE_USAGE GRANTS
-- -------------------------------------------
-- Both the CLD DB and AVEVA_CONNECT admin DB need REFERENCE_USAGE granted to the share
SHOW GRANTS ON DATABASE IDENTIFIER($CLD_DB);
-- Look for: REFERENCE_USAGE granted TO SHARE IN APPLICATION PACKAGE

SHOW GRANTS ON DATABASE AVEVA_CONNECT;
-- Look for: REFERENCE_USAGE granted TO SHARE IN APPLICATION PACKAGE

-- -------------------------------------------
-- 7. CHECK LISTING
-- -------------------------------------------
SHOW LISTINGS;
-- Look for AVEVA_CONNECT_<CUSTOMER_NAME>_LISTING
-- Verify: state = PUBLISHED

-- -------------------------------------------
-- 8. CHECK ONBOARDING LOG (from ONBOARD_CUSTOMER proc)
-- -------------------------------------------
SELECT * FROM AVEVA_CONNECT.ADMIN.ONBOARDING_LOG
WHERE CUSTOMER_NAME = $CUSTOMER_NAME
ORDER BY STEP_NUMBER, EXECUTED_AT DESC;

-- Check customer registration
SELECT * FROM AVEVA_CONNECT.ADMIN.CUSTOMERS
WHERE CUSTOMER_NAME = $CUSTOMER_NAME;

-- Check which tables were bundled vs not bundled
SELECT * FROM AVEVA_CONNECT.ADMIN.CUSTOMER_TABLES
WHERE CUSTOMER_NAME = $CUSTOMER_NAME
ORDER BY STATUS, TABLE_NAME;

-- -------------------------------------------
-- 9. TEST: CREATE DEV MODE APP (same as consumer would get)
-- -------------------------------------------
-- This tests whether the setup.sql runs cleanly
-- DROP APPLICATION IF EXISTS AVEVA_CONNECT_DEV;
-- CREATE APPLICATION AVEVA_CONNECT_DEV
--   FROM APPLICATION PACKAGE AVEVA_CONNECT_APP_PKG
--   USING '@AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE';

-- Grant external data access (CRITICAL for semantic view over CLD)
-- SELECT SYSTEM$SET_APPLICATION_RESTRICTED_FEATURE_ACCESS(
--   'AVEVA_CONNECT_DEV', 'external_data', '{"allowed_cloud_providers" : "all"}');

-- Grant imported privileges (for Cortex AI functions)
-- GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION AVEVA_CONNECT_DEV;
-- GRANT USAGE ON WAREHOUSE COMPUTE_WH TO APPLICATION AVEVA_CONNECT_DEV;

-- Run the init pipeline
-- CALL AVEVA_CONNECT_DEV.CORE.REINITIALIZE();

-- Verify
-- CALL AVEVA_CONNECT_DEV.CORE.HEALTH_CHECK();
-- SELECT * FROM AVEVA_CONNECT_DEV.CONFIG.APP_STATE ORDER BY KEY;
-- DESCRIBE SEMANTIC VIEW AVEVA_CONNECT_DEV.DATA_VIEWS.AVEVA_ANALYTICS;

-- -------------------------------------------
-- 10. CHECK CONFIG TABLE
-- -------------------------------------------
SELECT * FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG ORDER BY CONFIG_KEY;
