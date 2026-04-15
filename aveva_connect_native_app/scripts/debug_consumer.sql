-- =============================================================================
-- AVEVA CONNECT - Consumer-Side Debug Script (POLARIS2)
-- =============================================================================
-- Run on the CONSUMER account (where the app is installed from a listing).
-- This script checks every layer of the installed app to isolate failures.
--
-- Replace <APP_NAME> with the installed app name
--   e.g. AVEVA_CONNECT_POLARIS2_TEST
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- -------------------------------------------
-- 0. VARIABLES (edit these)
-- -------------------------------------------
SET APP_NAME = 'AVEVA_CONNECT_POLARIS2_TEST';

-- -------------------------------------------
-- 1. CHECK APP EXISTS AND VERSION
-- -------------------------------------------
SHOW APPLICATIONS;
-- Verify: app exists, upgrade_state = COMPLETE, check version & patch

DESCRIBE APPLICATION IDENTIFIER($APP_NAME);
-- Check: upgrade_state, restricted_callers_rights, debug_mode

-- -------------------------------------------
-- 2. CHECK RESTRICTED FEATURE: EXTERNAL_DATA
-- -------------------------------------------
-- This is the #1 cause of semantic view failure.
-- The consumer MUST approve external_data for the app to resolve CLD proxy views.
-- If not approved, CREATE SEMANTIC VIEW fails silently and SEMANTIC_VIEW_CREATED = FALSE.

-- Grant it (idempotent — safe to re-run):
SELECT SYSTEM$SET_APPLICATION_RESTRICTED_FEATURE_ACCESS(
  $APP_NAME, 'external_data', '{"allowed_cloud_providers" : "all"}');

-- -------------------------------------------
-- 3. CHECK IMPORTED PRIVILEGES
-- -------------------------------------------
-- Required for Cortex AI functions (Complete, Classify, Forecast, etc.)
SHOW GRANTS TO APPLICATION IDENTIFIER($APP_NAME);
-- Look for: IMPORTED PRIVILEGES on DATABASE SNOWFLAKE
-- Look for: USAGE on WAREHOUSE

-- Grant if missing:
-- GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION IDENTIFIER($APP_NAME);
-- GRANT USAGE ON WAREHOUSE COMPUTE_WH TO APPLICATION IDENTIFIER($APP_NAME);

-- -------------------------------------------
-- 4. CHECK APP STATE (initialization status)
-- -------------------------------------------
SELECT * FROM AVEVA_CONNECT_POLARIS2_TEST.CONFIG.APP_STATE ORDER BY KEY;
-- Key values to check:
--   VIEWS_INITIALIZED     = TRUE   (Step 1 ran)
--   METADATA_DISCOVERED   = TRUE   (Step 2 ran)
--   SEMANTIC_VIEW_CREATED = TRUE   (Step 3 ran — THIS is the Cortex Analyst gate)
--   VIEW_COUNT            = N      (number of proxy views discovered)
--   STREAM_COUNT          = N      (number of signals discovered)

-- -------------------------------------------
-- 5. CHECK INIT PROGRESS (detailed step status)
-- -------------------------------------------
SELECT * FROM AVEVA_CONNECT_POLARIS2_TEST.CONFIG.INIT_PROGRESS ORDER BY STARTED_AT;
-- Look for: STATUS = COMPLETE for all 3 steps
-- If STATUS = ERROR, the DETAILS column has the error message

-- -------------------------------------------
-- 6. CHECK PROXY VIEWS (shared from provider)
-- -------------------------------------------
SHOW VIEWS IN SCHEMA AVEVA_CONNECT_POLARIS2_TEST.PROXY_VIEWS;
-- Should show the same proxy views the provider bundled

-- Try querying one (replace with actual view name):
-- SELECT * FROM AVEVA_CONNECT_POLARIS2_TEST.PROXY_VIEWS."windturbine_fixed_2603" LIMIT 5;
-- If this fails: external_data not approved, or provider didn't grant to share

-- -------------------------------------------
-- 7. CHECK STREAM METADATA
-- -------------------------------------------
SELECT COUNT(*) AS TOTAL_STREAMS FROM AVEVA_CONNECT_POLARIS2_TEST.CONFIG.STREAM_METADATA;
SELECT DOMAIN_CATEGORY, COUNT(*) AS STREAMS
FROM AVEVA_CONNECT_POLARIS2_TEST.CONFIG.STREAM_METADATA
GROUP BY DOMAIN_CATEGORY ORDER BY STREAMS DESC;

-- -------------------------------------------
-- 8. CHECK SEMANTIC VIEW
-- -------------------------------------------
-- This is what Cortex Analyst uses. If it doesn't exist, the "Talk to Your Data" page shows a warning.
DESCRIBE SEMANTIC VIEW AVEVA_CONNECT_POLARIS2_TEST.DATA_VIEWS.AVEVA_ANALYTICS;
-- If error "does not exist": SEMANTIC_VIEW_CREATED is FALSE, need to re-run REINITIALIZE

-- -------------------------------------------
-- 9. RUN HEALTH CHECK
-- -------------------------------------------
CALL AVEVA_CONNECT_POLARIS2_TEST.CORE.HEALTH_CHECK();
-- Returns JSON with status for each subsystem:
--   shared_content_sync  — proxy views match stored count
--   app_state_keys       — all required keys present
--   stream_metadata      — metadata rows exist
--   proxy_views          — proxy views accessible
--   semantic_view        — semantic view exists and is queryable

-- -------------------------------------------
-- 10. FIX: RE-RUN INITIALIZATION
-- -------------------------------------------
-- If SEMANTIC_VIEW_CREATED = FALSE, the fix is:
--   1. Ensure external_data is approved (step 2 above)
--   2. Re-run REINITIALIZE:

-- CALL AVEVA_CONNECT_POLARIS2_TEST.CORE.REINITIALIZE();
-- This clears all state and re-runs the 3-step pipeline:
--   Step 1: INITIALIZE_VIEWS   — discovers proxy views
--   Step 2: DISCOVER_METADATA  — inspects columns, classifies domains
--   Step 3: CREATE_SEMANTIC_VIEWS — builds AVEVA_ANALYTICS semantic view

-- After REINITIALIZE, verify:
-- SELECT * FROM AVEVA_CONNECT_POLARIS2_TEST.CONFIG.APP_STATE WHERE KEY = 'SEMANTIC_VIEW_CREATED';
-- Should be TRUE

-- -------------------------------------------
-- 11. FIX: FORCE UPGRADE TO LATEST PATCH
-- -------------------------------------------
-- If the app is on an old patch:
-- ALTER APPLICATION AVEVA_CONNECT_POLARIS2_TEST UPGRADE;

-- -------------------------------------------
-- 12. CHECK INIT LOCK (if REINITIALIZE hangs)
-- -------------------------------------------
SELECT * FROM AVEVA_CONNECT_POLARIS2_TEST.CONFIG.INIT_LOCK;
-- If rows exist, a previous init is "stuck". Clear it:
-- DELETE FROM AVEVA_CONNECT_POLARIS2_TEST.CONFIG.INIT_LOCK;
-- Then re-run REINITIALIZE.

-- -------------------------------------------
-- 13. TEST CORTEX ANALYST (if semantic view exists)
-- -------------------------------------------
-- Quick test — ask a simple question:
-- This calls the Cortex Analyst API directly (same as Streamlit does):
-- SELECT SNOWFLAKE.CORTEX.ANALYST(
--   'AVEVA_CONNECT_POLARIS2_TEST.DATA_VIEWS.AVEVA_ANALYTICS',
--   'How many wind turbine signals are there?'
-- );
