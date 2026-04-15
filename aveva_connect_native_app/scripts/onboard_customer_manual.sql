-- =============================================================================
-- AVEVA CONNECT - Manual Onboarding (what ONBOARD_CUSTOMER proc does)
-- =============================================================================
-- This is the step-by-step SQL equivalent of:
--   CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
--       'CUSTOMER_NAME', 'ACCOUNT', 'CATALOG_URI', 'BEARER_TOKEN', 'WAREHOUSE_ID');
--
-- Use this to run each step individually for debugging.
-- Replace ALL <PLACEHOLDER> values before running.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- -------------------------------------------
-- VARIABLES (edit ALL of these)
-- -------------------------------------------
SET CUSTOMER_NAME    = '<CUSTOMER_NAME>';       -- e.g. 'POLARIS2_TEST'
SET CUSTOMER_ACCOUNT = '<ACCOUNT>';             -- e.g. 'SFSENORTHAMERICA.POLARIS2'
SET CATALOG_URI      = '<CATALOG_URI>';         -- Databricks Delta Sharing catalog URI
SET BEARER_TOKEN     = '<BEARER_TOKEN>';        -- Databricks bearer token
SET WAREHOUSE_ID     = '<WAREHOUSE_ID>';        -- Databricks warehouse ID
SET APP_PKG          = 'AVEVA_CONNECT_APP_PKG';
SET APP_VERSION      = 'V1_2';

-- Derived names
SET CATALOG_INT  = 'AVEVA_' || $CUSTOMER_NAME || '_CATALOG_INT';
SET CLD_DB       = 'AVEVA_' || $CUSTOMER_NAME || '_DB';
SET LISTING_NAME = 'AVEVA_CONNECT_' || $CUSTOMER_NAME || '_LISTING';


-- =============================================================================
-- STEP 1: APP PACKAGE (idempotent)
-- =============================================================================
CREATE APPLICATION PACKAGE IF NOT EXISTS IDENTIFIER($APP_PKG);
CREATE SCHEMA IF NOT EXISTS AVEVA_CONNECT_APP_PKG.APP_SRC;
CREATE STAGE IF NOT EXISTS AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE
    DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Upload files (run from aveva_connect_native_app/ directory via SnowSQL):
-- PUT file://manifest.yml              @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
-- PUT file://setup.sql                 @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
-- PUT file://README.md                 @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
-- PUT file://streamlit/main.py         @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/streamlit/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
-- PUT file://streamlit/environment.yml @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/streamlit/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

-- Verify files:
LIST @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE;

-- Register version (use ADD if REGISTER fails):
-- ALTER APPLICATION PACKAGE IDENTIFIER($APP_PKG) ADD VERSION IDENTIFIER($APP_VERSION)
--   USING '@AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE';

-- Or add a patch to existing version:
-- ALTER APPLICATION PACKAGE IDENTIFIER($APP_PKG) ADD PATCH FOR VERSION IDENTIFIER($APP_VERSION)
--   USING '@AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE';

-- Set release directive:
-- ALTER APPLICATION PACKAGE IDENTIFIER($APP_PKG)
--   MODIFY RELEASE CHANNEL DEFAULT
--   SET DEFAULT RELEASE DIRECTIVE VERSION = V1_2 PATCH = <latest_patch>;

-- Verify:
SHOW VERSIONS IN APPLICATION PACKAGE IDENTIFIER($APP_PKG);


-- =============================================================================
-- STEP 2: CATALOG INTEGRATION (Iceberg REST / Delta Sharing)
-- =============================================================================
-- Creates the catalog integration that connects Snowflake to the Databricks
-- Delta Sharing endpoint. This enables Snowflake to discover and read Iceberg tables.

CREATE OR REPLACE CATALOG INTEGRATION IDENTIFIER($CATALOG_INT)
    CATALOG_SOURCE = ICEBERG_REST
    TABLE_FORMAT = ICEBERG
    REST_CONFIG = (
        CATALOG_URI = $CATALOG_URI
        WAREHOUSE = $WAREHOUSE_ID
        ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
    )
    REST_AUTHENTICATION = (
        TYPE = BEARER
        BEARER_TOKEN = $BEARER_TOKEN
    )
    ENABLED = TRUE
    REFRESH_INTERVAL_SECONDS = 30;

-- Verify:
DESCRIBE CATALOG INTEGRATION IDENTIFIER($CATALOG_INT);


-- =============================================================================
-- STEP 3: CATALOG-LINKED DATABASE (CLD)
-- =============================================================================
-- Creates a CLD that auto-discovers tables from the catalog integration.
-- Tables sync within ~30-90 seconds.

CREATE OR REPLACE DATABASE IDENTIFIER($CLD_DB)
    LINKED_CATALOG = (
        CATALOG = $CATALOG_INT
        SYNC_INTERVAL_SECONDS = 300
        NAMESPACE_MODE = FLATTEN_NESTED_NAMESPACE
        NAMESPACE_FLATTEN_DELIMITER = '/'
    );

-- Wait for tables to sync (check every 10 seconds)
SHOW TABLES IN DATABASE IDENTIFIER($CLD_DB);
-- If 0 tables, wait 30s and try again. Up to 90s for initial sync.


-- =============================================================================
-- STEP 4: SHARE DATA (grant references to app package)
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS AVEVA_CONNECT_APP_PKG.SHARED_CONTENT;

GRANT USAGE ON SCHEMA AVEVA_CONNECT_APP_PKG.SHARED_CONTENT
    TO SHARE IN APPLICATION PACKAGE IDENTIFIER($APP_PKG);

-- CLD database needs REFERENCE_USAGE so proxy views can reference it
GRANT REFERENCE_USAGE ON DATABASE IDENTIFIER($CLD_DB)
    TO SHARE IN APPLICATION PACKAGE IDENTIFIER($APP_PKG);

-- Admin database needs REFERENCE_USAGE for account mapping view
GRANT REFERENCE_USAGE ON DATABASE AVEVA_CONNECT
    TO SHARE IN APPLICATION PACKAGE IDENTIFIER($APP_PKG);


-- =============================================================================
-- STEP 5: CREATE PROXY VIEWS (CLD tables -> app package)
-- =============================================================================
-- For each table discovered in the CLD, create a secure proxy view in the app
-- package. These views are what the consumer app sees in PROXY_VIEWS schema.

CREATE SCHEMA IF NOT EXISTS AVEVA_CONNECT_APP_PKG.PROXY_VIEWS;

GRANT USAGE ON SCHEMA AVEVA_CONNECT_APP_PKG.PROXY_VIEWS
    TO SHARE IN APPLICATION PACKAGE IDENTIFIER($APP_PKG);

-- First, check what tables exist in the CLD:
SHOW TABLES IN DATABASE IDENTIFIER($CLD_DB);

-- Then for EACH table, run these two commands (replace <schema> and <table>):
-- CREATE OR REPLACE SECURE VIEW AVEVA_CONNECT_APP_PKG.PROXY_VIEWS."<table>"
--   AS SELECT * FROM <CLD_DB>."<schema>"."<table>";
-- GRANT SELECT ON VIEW AVEVA_CONNECT_APP_PKG.PROXY_VIEWS."<table>"
--   TO SHARE IN APPLICATION PACKAGE AVEVA_CONNECT_APP_PKG;

-- Verify proxy views:
SHOW VIEWS IN SCHEMA AVEVA_CONNECT_APP_PKG.PROXY_VIEWS;


-- =============================================================================
-- STEP 6: ACCOUNT MAPPING
-- =============================================================================
-- Maps the consumer account to a view pattern so the app knows which views to expose.

INSERT INTO AVEVA_CONNECT.ADMIN.ACCOUNT_MAPPING (SNOWFLAKE_ACCOUNT, VIEW_PATTERN)
    SELECT $CUSTOMER_ACCOUNT, '%'
    WHERE NOT EXISTS (
        SELECT 1 FROM AVEVA_CONNECT.ADMIN.ACCOUNT_MAPPING
        WHERE SNOWFLAKE_ACCOUNT = $CUSTOMER_ACCOUNT
    );

CREATE OR REPLACE SECURE VIEW AVEVA_CONNECT_APP_PKG.SHARED_CONTENT.ACCOUNT_VIEW_MAP
    AS SELECT SNOWFLAKE_ACCOUNT, VIEW_PATTERN FROM AVEVA_CONNECT.ADMIN.ACCOUNT_MAPPING;

GRANT SELECT ON VIEW AVEVA_CONNECT_APP_PKG.SHARED_CONTENT.ACCOUNT_VIEW_MAP
    TO SHARE IN APPLICATION PACKAGE IDENTIFIER($APP_PKG);


-- =============================================================================
-- STEP 7: CREATE & PUBLISH LISTING
-- =============================================================================
-- Organization listing (same org, internal distribution):
ALTER APPLICATION PACKAGE IDENTIFIER($APP_PKG) SET DISTRIBUTION = 'INTERNAL';

-- Replace <SHORT_ACCOUNT> with just the account locator (e.g. POLARIS2)
-- CREATE ORGANIZATION LISTING IDENTIFIER($LISTING_NAME)
--     APPLICATION PACKAGE AVEVA_CONNECT_APP_PKG
--     AS '
-- title: "AVEVA Connect - <CUSTOMER_NAME>"
-- subtitle: "Industrial PI Data Analytics"
-- description: "Cross-account PI data historian analytics powered by Cortex AI"
-- listing_terms:
--   type: "OFFLINE"
-- organization_profile: "INTERNAL"
-- organization_targets:
--   access:
--   - account: "<SHORT_ACCOUNT>"
-- locations:
--   access_regions:
--   - name: "PUBLIC.AWS_US_WEST_2"
-- resharing:
--   enabled: false'
--     PUBLISH = FALSE REVIEW = FALSE;

-- Publish:
-- ALTER LISTING IDENTIFIER($LISTING_NAME) PUBLISH;

-- Verify:
SHOW LISTINGS;


-- =============================================================================
-- STEP 8: REGISTER CUSTOMER
-- =============================================================================
INSERT INTO AVEVA_CONNECT.ADMIN.CUSTOMERS (CUSTOMER_NAME, SNOWFLAKE_ACCOUNT_ID, STATUS, NOTES)
    SELECT $CUSTOMER_NAME, $CUSTOMER_ACCOUNT, 'ACTIVE', 'Onboarded manually'
    WHERE NOT EXISTS (
        SELECT 1 FROM AVEVA_CONNECT.ADMIN.CUSTOMERS
        WHERE SNOWFLAKE_ACCOUNT_ID = $CUSTOMER_ACCOUNT
    );

-- Verify:
SELECT * FROM AVEVA_CONNECT.ADMIN.CUSTOMERS WHERE CUSTOMER_NAME = $CUSTOMER_NAME;


-- =============================================================================
-- DONE — Consumer can now install from the listing
-- =============================================================================
-- After consumer installs, they MUST:
--   1. Approve external_data:
--      SELECT SYSTEM$SET_APPLICATION_RESTRICTED_FEATURE_ACCESS(
--        '<APP_NAME>', 'external_data', '{"allowed_cloud_providers":"all"}');
--   2. Grant privileges:
--      GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION <APP_NAME>;
--      GRANT USAGE ON WAREHOUSE COMPUTE_WH TO APPLICATION <APP_NAME>;
--   3. Run initialization:
--      CALL <APP_NAME>.CORE.REINITIALIZE();
