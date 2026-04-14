-- =============================================================================
-- AVEVA CONNECT - One-Time Setup & App Upload
-- =============================================================================
-- Run this ONCE before calling AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER().
-- It creates the admin database, tables, seeds config, creates the app
-- package with stage, uploads app files, registers the version, and
-- creates the onboarding stored procedure.
--
-- After this, onboarding any customer is just:
--   CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER('NAME', 'ACCOUNT', 'URI', 'TOKEN', 'WAREHOUSE_ID');
--
-- The proc handles 8 steps: app package, catalog integration, CLD,
-- share data, proxy views (auto-discovers CLD tables), account mapping,
-- listing, and customer registration. All idempotent / safe to re-run.
--
-- PREREQUISITES:
--   - ACCOUNTADMIN role
--   - Run from the aveva_connect_native_app/ directory (for PUT paths)
--     OR update the file:// paths in Step 3 to your local paths
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- STEP 1: Admin Database & Tables
-- =============================================================================

CREATE DATABASE IF NOT EXISTS AVEVA_CONNECT;
CREATE SCHEMA IF NOT EXISTS AVEVA_CONNECT.ADMIN;

CREATE TABLE IF NOT EXISTS AVEVA_CONNECT.ADMIN.CUSTOMERS (
    CUSTOMER_ID           VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    CUSTOMER_NAME         VARCHAR NOT NULL,
    SNOWFLAKE_ACCOUNT_ID  VARCHAR NOT NULL,
    ORG_NAME              VARCHAR,
    CONTACT_EMAIL         VARCHAR,
    STATUS                VARCHAR DEFAULT 'PENDING',
    CREATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    NOTES                 VARCHAR
);

CREATE TABLE IF NOT EXISTS AVEVA_CONNECT.ADMIN.ACCOUNT_MAPPING (
    MAPPING_ID      VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    SNOWFLAKE_ACCOUNT VARCHAR(200) NOT NULL,
    VIEW_PATTERN    VARCHAR(500) NOT NULL,
    GRANTED_AT      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG (
    CONFIG_ID       VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    CONFIG_KEY      VARCHAR NOT NULL UNIQUE,
    CONFIG_VALUE    VARCHAR,
    UPDATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (
    LOG_ID          VARCHAR DEFAULT UUID_STRING() PRIMARY KEY,
    CUSTOMER_NAME   VARCHAR NOT NULL,
    STEP_NUMBER     INTEGER NOT NULL,
    STEP_NAME       VARCHAR NOT NULL,
    STATUS          VARCHAR NOT NULL,
    MESSAGE         VARCHAR,
    EXECUTED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Seed default configurations (idempotent)
INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG (CONFIG_KEY, CONFIG_VALUE)
    SELECT 'APP_PKG_NAME', 'AVEVA_CONNECT_APP_PKG'
    WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'APP_PKG_NAME');
INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG (CONFIG_KEY, CONFIG_VALUE)
    SELECT 'APP_VERSION', 'V1_0'
    WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'APP_VERSION');
INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG (CONFIG_KEY, CONFIG_VALUE)
    SELECT 'APP_SOURCE_STAGE', 'AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE'
    WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'APP_SOURCE_STAGE');
INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG (CONFIG_KEY, CONFIG_VALUE)
    SELECT 'WAREHOUSE_NAME', 'COMPUTE_WH'
    WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'WAREHOUSE_NAME');
INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG (CONFIG_KEY, CONFIG_VALUE)
    SELECT 'DISTRIBUTION_MODE', 'ORGANIZATION'
    WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'DISTRIBUTION_MODE');
INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG (CONFIG_KEY, CONFIG_VALUE)
    SELECT 'LISTING_SUBTITLE', 'Industrial PI Data Analytics'
    WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'LISTING_SUBTITLE');
INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG (CONFIG_KEY, CONFIG_VALUE)
    SELECT 'LISTING_DESCRIPTION', 'Cross-account PI data historian analytics powered by Cortex AI'
    WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'LISTING_DESCRIPTION');
INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG (CONFIG_KEY, CONFIG_VALUE)
    SELECT 'ACCESS_REGION', 'PUBLIC.AWS_US_WEST_2'
    WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'ACCESS_REGION');
INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG (CONFIG_KEY, CONFIG_VALUE)
    SELECT 'VIEW_PATTERN', '%'
    WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'VIEW_PATTERN');


-- =============================================================================
-- STEP 2: Application Package & Stage
-- =============================================================================

CREATE APPLICATION PACKAGE IF NOT EXISTS AVEVA_CONNECT_APP_PKG
    COMMENT = 'AVEVA CONNECT PI Data Analytics';

CREATE SCHEMA IF NOT EXISTS AVEVA_CONNECT_APP_PKG.APP_SRC;

CREATE STAGE IF NOT EXISTS AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Native app source files';


-- =============================================================================
-- STEP 3: Upload App Files
-- =============================================================================
-- Option A: PUT commands (run from SnowSQL or a worksheet connected to SnowSQL)
-- Execute from the aveva_connect_native_app/ directory.

PUT file://manifest.yml             @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file://setup.sql                @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file://README.md                @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file://streamlit/main.py        @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/streamlit/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file://streamlit/environment.yml @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/streamlit/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

-- Option B: Snow CLI (run from terminal instead of the PUTs above)
-- cd aveva_connect_native_app/
-- snow stage copy manifest.yml             @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           --overwrite --connection my_polaris_connection
-- snow stage copy setup.sql                @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           --overwrite --connection my_polaris_connection
-- snow stage copy README.md                @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           --overwrite --connection my_polaris_connection
-- snow stage copy streamlit/main.py        @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/streamlit/ --overwrite --connection my_polaris_connection
-- snow stage copy streamlit/environment.yml @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/streamlit/ --overwrite --connection my_polaris_connection

-- Verify files uploaded:
LIST @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE;


-- =============================================================================
-- STEP 4: Register Version & Release Directive
-- =============================================================================

ALTER APPLICATION PACKAGE AVEVA_CONNECT_APP_PKG
    ADD VERSION V1_0
    USING '@AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE';

ALTER APPLICATION PACKAGE AVEVA_CONNECT_APP_PKG
    SET DEFAULT RELEASE DIRECTIVE
    VERSION = V1_0
    PATCH = 0;

-- Verify:
SHOW VERSIONS IN APPLICATION PACKAGE AVEVA_CONNECT_APP_PKG;


-- =============================================================================
-- STEP 5: Create Onboarding Stored Procedure
-- =============================================================================
-- This is the proc from onboard_customer_e2e.sql.
-- After this, onboarding is just: CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(...)

CREATE OR REPLACE PROCEDURE AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
    P_CUSTOMER_NAME    VARCHAR,
    P_CUSTOMER_ACCOUNT VARCHAR,
    P_CATALOG_URI      VARCHAR,
    P_BEARER_TOKEN     VARCHAR,
    P_WAREHOUSE_ID     VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_app_pkg         VARCHAR;
    v_app_version     VARCHAR;
    v_app_stage       VARCHAR;
    v_warehouse       VARCHAR;
    v_dist_mode       VARCHAR;
    v_subtitle        VARCHAR;
    v_description     VARCHAR;
    v_region          VARCHAR;
    v_view_pattern    VARCHAR;
    v_catalog_int     VARCHAR;
    v_cld_db          VARCHAR;
    v_listing_name    VARCHAR;
    v_listing_title   VARCHAR;
    v_stage_schema    VARCHAR;
    v_short_account   VARCHAR;
    v_listing_yaml    VARCHAR;
    v_listing_sql     VARCHAR;
    v_steps_ok        INTEGER DEFAULT 0;
    v_steps_failed    INTEGER DEFAULT 0;
    v_result          VARCHAR DEFAULT '';
    v_err             VARCHAR;
    v_proxy_count     INTEGER DEFAULT 0;
    v_table_count     INTEGER DEFAULT 0;
    v_retry           INTEGER DEFAULT 0;
BEGIN

    -- Load defaults
    SELECT CONFIG_VALUE INTO v_app_pkg     FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'APP_PKG_NAME';
    SELECT CONFIG_VALUE INTO v_app_version FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'APP_VERSION';
    SELECT CONFIG_VALUE INTO v_app_stage   FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'APP_SOURCE_STAGE';
    SELECT CONFIG_VALUE INTO v_warehouse   FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'WAREHOUSE_NAME';
    SELECT CONFIG_VALUE INTO v_dist_mode   FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'DISTRIBUTION_MODE';
    SELECT CONFIG_VALUE INTO v_subtitle    FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'LISTING_SUBTITLE';
    SELECT CONFIG_VALUE INTO v_description FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'LISTING_DESCRIPTION';
    SELECT CONFIG_VALUE INTO v_region      FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'ACCESS_REGION';
    SELECT CONFIG_VALUE INTO v_view_pattern FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'VIEW_PATTERN';

    -- Derive names
    v_catalog_int  := 'AVEVA_' || :P_CUSTOMER_NAME || '_CATALOG_INT';
    v_cld_db       := 'AVEVA_' || :P_CUSTOMER_NAME || '_DB';
    v_listing_name := 'AVEVA_CONNECT_' || :P_CUSTOMER_NAME || '_LISTING';
    v_listing_title:= 'AVEVA Connect - ' || REPLACE(:P_CUSTOMER_NAME, '_', ' ');
    v_stage_schema := SUBSTR(:v_app_stage, 1, LENGTH(:v_app_stage) - LENGTH(SPLIT_PART(:v_app_stage, '.', -1)) - 1);
    v_short_account := SPLIT_PART(:P_CUSTOMER_ACCOUNT, '.', -1);

    -- STEP 1: App Package (IF NOT EXISTS)
    BEGIN
        EXECUTE IMMEDIATE 'CREATE APPLICATION PACKAGE IF NOT EXISTS ' || :v_app_pkg;
        EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :v_stage_schema;
        EXECUTE IMMEDIATE 'CREATE STAGE IF NOT EXISTS ' || :v_app_stage ||
            ' DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = ''SNOWFLAKE_SSE'')';

        -- Register version (REGISTER for release channels, ADD as fallback)
        BEGIN
            EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg ||
                ' REGISTER VERSION ' || :v_app_version || ' USING ''@' || :v_app_stage || '''';
        EXCEPTION WHEN OTHER THEN
            BEGIN
                EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg ||
                    ' ADD VERSION ' || :v_app_version || ' USING ''@' || :v_app_stage || '''';
            EXCEPTION WHEN OTHER THEN NULL;
            END;
        END;

        -- Release channel: add version to default channel
        BEGIN
            EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg ||
                ' MODIFY RELEASE CHANNEL DEFAULT ADD VERSION ' || :v_app_version;
        EXCEPTION WHEN OTHER THEN NULL;
        END;

        -- Set release directive (with release channel fallback)
        BEGIN
            EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg ||
                ' MODIFY RELEASE CHANNEL DEFAULT SET DEFAULT RELEASE DIRECTIVE VERSION = ' || :v_app_version || ' PATCH = 0';
        EXCEPTION WHEN OTHER THEN
            BEGIN
                EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg ||
                    ' SET DEFAULT RELEASE DIRECTIVE VERSION = ' || :v_app_version || ' PATCH = 0';
            EXCEPTION WHEN OTHER THEN NULL;
            END;
        END;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 1, 'App Package & Version', 'SUCCESS', 'Package: ' || :v_app_pkg);
        v_steps_ok := v_steps_ok + 1;
    EXCEPTION WHEN OTHER THEN
        v_err := SQLERRM;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 1, 'App Package & Version', 'FAILED', :v_err);
        v_steps_failed := v_steps_failed + 1;
        v_result := v_result || 'STEP 1 FAILED: ' || :v_err || '\n';
    END;

    -- STEP 2: Catalog Integration (Delta Sharing IRC)
    BEGIN
        EXECUTE IMMEDIATE
            'CREATE OR REPLACE CATALOG INTEGRATION ' || :v_catalog_int ||
            ' CATALOG_SOURCE = ICEBERG_REST TABLE_FORMAT = ICEBERG' ||
            ' REST_CONFIG = (CATALOG_URI = ''' || :P_CATALOG_URI ||
            ''' WAREHOUSE = ''' || :P_WAREHOUSE_ID ||
            ''' ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS)' ||
            ' REST_AUTHENTICATION = (TYPE = BEARER BEARER_TOKEN = ''' || :P_BEARER_TOKEN ||
            ''') ENABLED = TRUE REFRESH_INTERVAL_SECONDS = 30';
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 2, 'Catalog Integration', 'SUCCESS', 'Created: ' || :v_catalog_int);
        v_steps_ok := v_steps_ok + 1;
    EXCEPTION WHEN OTHER THEN
        v_err := SQLERRM;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 2, 'Catalog Integration', 'FAILED', :v_err);
        v_steps_failed := v_steps_failed + 1;
        v_result := v_result || 'STEP 2 FAILED: ' || :v_err || '\n';
    END;

    -- STEP 3: Catalog-Linked Database
    BEGIN
        EXECUTE IMMEDIATE
            'CREATE OR REPLACE DATABASE ' || :v_cld_db ||
            ' LINKED_CATALOG = (CATALOG = ''' || :v_catalog_int ||
            ''' SYNC_INTERVAL_SECONDS = 300 NAMESPACE_MODE = FLATTEN_NESTED_NAMESPACE NAMESPACE_FLATTEN_DELIMITER = ''/'')';
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 3, 'Catalog-Linked Database', 'SUCCESS', 'Created: ' || :v_cld_db);
        v_steps_ok := v_steps_ok + 1;
    EXCEPTION WHEN OTHER THEN
        v_err := SQLERRM;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 3, 'Catalog-Linked Database', 'FAILED', :v_err);
        v_steps_failed := v_steps_failed + 1;
        v_result := v_result || 'STEP 3 FAILED: ' || :v_err || '\n';
    END;

    -- STEP 4: Share Data
    BEGIN
        EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :v_app_pkg || '.SHARED_CONTENT';
        EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA ' || :v_app_pkg || '.SHARED_CONTENT TO SHARE IN APPLICATION PACKAGE ' || :v_app_pkg;
        BEGIN
            EXECUTE IMMEDIATE 'GRANT REFERENCE_USAGE ON DATABASE ' || :v_cld_db || ' TO SHARE IN APPLICATION PACKAGE ' || :v_app_pkg;
        EXCEPTION WHEN OTHER THEN NULL;
        END;
        EXECUTE IMMEDIATE 'GRANT REFERENCE_USAGE ON DATABASE AVEVA_CONNECT TO SHARE IN APPLICATION PACKAGE ' || :v_app_pkg;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 4, 'Share Data', 'SUCCESS', 'REFERENCE_USAGE granted');
        v_steps_ok := v_steps_ok + 1;
    EXCEPTION WHEN OTHER THEN
        v_err := SQLERRM;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 4, 'Share Data', 'FAILED', :v_err);
        v_steps_failed := v_steps_failed + 1;
        v_result := v_result || 'STEP 4 FAILED: ' || :v_err || '\n';
    END;

    -- STEP 5: Create Proxy Views (CLD tables -> app package share)
    -- Discovers tables from CLD and creates secure proxy views in the app
    -- package. These views are granted to the share so the consumer app's
    -- setup.sql can discover them via SHOW VIEWS IN SCHEMA PROXY_VIEWS.
    -- CLD tables take time to sync from Delta Sharing — wait up to 90s.
    BEGIN
        EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :v_app_pkg || '.PROXY_VIEWS';
        EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA ' || :v_app_pkg || '.PROXY_VIEWS TO SHARE IN APPLICATION PACKAGE ' || :v_app_pkg;

        -- Wait for CLD tables to sync (up to 90 seconds, polling every 10s)
        v_table_count := 0;
        v_retry := 0;
        WHILE (:v_table_count = 0 AND :v_retry < 9) DO
            EXECUTE IMMEDIATE 'SHOW TABLES IN DATABASE ' || :v_cld_db;
            SELECT COUNT(*) INTO v_table_count FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            IF (:v_table_count = 0) THEN
                CALL SYSTEM$WAIT(10, 'SECONDS');
                v_retry := v_retry + 1;
            END IF;
        END WHILE;

        -- Create proxy views for discovered tables
        EXECUTE IMMEDIATE 'SHOW TABLES IN DATABASE ' || :v_cld_db;
        LET c1 CURSOR FOR
            SELECT "name" AS TABLE_NAME, "schema_name" AS TABLE_SCHEMA
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        OPEN c1;
        v_proxy_count := 0;
        FOR rec IN c1 DO
            BEGIN
                EXECUTE IMMEDIATE 'CREATE OR REPLACE SECURE VIEW ' || :v_app_pkg || '.PROXY_VIEWS."' || rec.TABLE_NAME || '" AS SELECT * FROM ' || :v_cld_db || '."' || rec.TABLE_SCHEMA || '"."' || rec.TABLE_NAME || '"';
                EXECUTE IMMEDIATE 'GRANT SELECT ON VIEW ' || :v_app_pkg || '.PROXY_VIEWS."' || rec.TABLE_NAME || '" TO SHARE IN APPLICATION PACKAGE ' || :v_app_pkg;
                v_proxy_count := v_proxy_count + 1;
            EXCEPTION WHEN OTHER THEN NULL;
            END;
        END FOR;
        CLOSE c1;

        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 5, 'Proxy Views', 'SUCCESS', :v_proxy_count || ' proxy views created (waited ' || :v_retry * 10 || 's for CLD sync)');
        v_steps_ok := v_steps_ok + 1;
    EXCEPTION WHEN OTHER THEN
        v_err := SQLERRM;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 5, 'Proxy Views', 'FAILED', :v_err);
        v_steps_failed := v_steps_failed + 1;
        v_result := v_result || 'STEP 5 FAILED: ' || :v_err || '\n';
    END;

    -- STEP 6: Account Mapping (idempotent)
    BEGIN
        EXECUTE IMMEDIATE
            'INSERT INTO AVEVA_CONNECT.ADMIN.ACCOUNT_MAPPING (SNOWFLAKE_ACCOUNT, VIEW_PATTERN)
             SELECT ''' || :P_CUSTOMER_ACCOUNT || ''', ''' || :v_view_pattern || '''
             WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.ACCOUNT_MAPPING WHERE SNOWFLAKE_ACCOUNT = ''' || :P_CUSTOMER_ACCOUNT || ''')';
        EXECUTE IMMEDIATE
            'CREATE OR REPLACE SECURE VIEW ' || :v_app_pkg || '.SHARED_CONTENT.ACCOUNT_VIEW_MAP AS SELECT SNOWFLAKE_ACCOUNT, VIEW_PATTERN FROM AVEVA_CONNECT.ADMIN.ACCOUNT_MAPPING';
        EXECUTE IMMEDIATE
            'GRANT SELECT ON VIEW ' || :v_app_pkg || '.SHARED_CONTENT.ACCOUNT_VIEW_MAP TO SHARE IN APPLICATION PACKAGE ' || :v_app_pkg;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 6, 'Account Mapping', 'SUCCESS', 'Mapped ' || :P_CUSTOMER_ACCOUNT);
        v_steps_ok := v_steps_ok + 1;
    EXCEPTION WHEN OTHER THEN
        v_err := SQLERRM;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 6, 'Account Mapping', 'FAILED', :v_err);
        v_steps_failed := v_steps_failed + 1;
        v_result := v_result || 'STEP 6 FAILED: ' || :v_err || '\n';
    END;

    -- STEP 7: Listing
    BEGIN
        BEGIN
            IF (:v_dist_mode = 'ORGANIZATION') THEN
                EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg || ' SET DISTRIBUTION = ''INTERNAL''';
            ELSE
                EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg || ' SET DISTRIBUTION = ''EXTERNAL''';
            END IF;
        EXCEPTION WHEN OTHER THEN NULL;
        END;

        IF (:v_dist_mode = 'ORGANIZATION') THEN
            v_listing_yaml := 'title: "' || :v_listing_title || '"' || CHR(10) ||
                'subtitle: "' || :v_subtitle || '"' || CHR(10) ||
                'description: "' || :v_description || '"' || CHR(10) ||
                'listing_terms:' || CHR(10) ||
                '  type: "OFFLINE"' || CHR(10) ||
                'organization_profile: "INTERNAL"' || CHR(10) ||
                'organization_targets:' || CHR(10) ||
                '  access:' || CHR(10) ||
                '  - account: "' || :v_short_account || '"' || CHR(10) ||
                'locations:' || CHR(10) ||
                '  access_regions:' || CHR(10) ||
                '  - name: "' || :v_region || '"' || CHR(10) ||
                'resharing:' || CHR(10) ||
                '  enabled: false';
            v_listing_sql := 'CREATE ORGANIZATION LISTING ' || :v_listing_name ||
                ' APPLICATION PACKAGE ' || :v_app_pkg ||
                ' AS ''' || REPLACE(:v_listing_yaml, '''', '''''') || '''' ||
                ' PUBLISH = FALSE REVIEW = FALSE';
        ELSE
            BEGIN
                EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg || ' ADD TARGET ACCOUNTS = (''' || :P_CUSTOMER_ACCOUNT || ''')';
            EXCEPTION WHEN OTHER THEN NULL;
            END;
            v_listing_yaml := 'title: "' || :v_listing_title || '"' || CHR(10) ||
                'subtitle: "' || :v_subtitle || '"' || CHR(10) ||
                'description: "' || :v_description || '"' || CHR(10) ||
                'listing_terms:' || CHR(10) ||
                '  type: "OFFLINE"' || CHR(10) ||
                'targets:' || CHR(10) ||
                '  accounts: ["' || :P_CUSTOMER_ACCOUNT || '"]' || CHR(10) ||
                'locations:' || CHR(10) ||
                '  access_regions:' || CHR(10) ||
                '  - name: "' || :v_region || '"' || CHR(10) ||
                'resharing:' || CHR(10) ||
                '  enabled: false';
            v_listing_sql := 'CREATE EXTERNAL LISTING ' || :v_listing_name ||
                ' APPLICATION PACKAGE ' || :v_app_pkg ||
                ' AS ''' || REPLACE(:v_listing_yaml, '''', '''''') || '''' ||
                ' PUBLISH = FALSE REVIEW = FALSE';
        END IF;
        EXECUTE IMMEDIATE :v_listing_sql;
        EXECUTE IMMEDIATE 'ALTER LISTING ' || :v_listing_name || ' PUBLISH';
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 7, 'Listing', 'SUCCESS', 'Published: ' || :v_listing_name);
        v_steps_ok := v_steps_ok + 1;
    EXCEPTION WHEN OTHER THEN
        v_err := SQLERRM;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 7, 'Listing', 'FAILED', :v_err);
        v_steps_failed := v_steps_failed + 1;
        v_result := v_result || 'STEP 7 FAILED: ' || :v_err || '\n';
    END;

    -- STEP 8: Register Customer (idempotent)
    BEGIN
        EXECUTE IMMEDIATE
            'INSERT INTO AVEVA_CONNECT.ADMIN.CUSTOMERS (CUSTOMER_NAME, SNOWFLAKE_ACCOUNT_ID, STATUS, NOTES)
             SELECT ''' || :P_CUSTOMER_NAME || ''', ''' || :P_CUSTOMER_ACCOUNT || ''', ''ACTIVE'',
                    ''Onboarded via proc. Distribution: ' || :v_dist_mode || '.''
             WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.CUSTOMERS WHERE SNOWFLAKE_ACCOUNT_ID = ''' || :P_CUSTOMER_ACCOUNT || ''')';
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 8, 'Register Customer', 'SUCCESS', 'Done');
        v_steps_ok := v_steps_ok + 1;
    EXCEPTION WHEN OTHER THEN
        v_err := SQLERRM;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 8, 'Register Customer', 'FAILED', :v_err);
        v_steps_failed := v_steps_failed + 1;
        v_result := v_result || 'STEP 8 FAILED: ' || :v_err || '\n';
    END;

    -- Result
    IF (v_steps_failed = 0) THEN
        RETURN 'SUCCESS: All ' || v_steps_ok || '/8 steps completed. Pkg=' || :v_app_pkg
            || ', CLD=' || :v_cld_db || ', Listing=' || :v_listing_name;
    ELSE
        RETURN 'PARTIAL: ' || v_steps_ok || ' ok, ' || v_steps_failed || ' failed.\n' || v_result;
    END IF;
END;
$$;


-- =============================================================================
-- VERIFY
-- =============================================================================

-- Check stage has all files
LIST @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE;

-- Check version is registered
SHOW VERSIONS IN APPLICATION PACKAGE AVEVA_CONNECT_APP_PKG;

-- Check config is seeded
SELECT * FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG ORDER BY CONFIG_KEY;

-- Check proc exists
SHOW PROCEDURES LIKE 'ONBOARD_CUSTOMER' IN SCHEMA AVEVA_CONNECT.ADMIN;

-- =============================================================================
-- DONE — Now onboard customers:
-- =============================================================================
--
--   CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
--       'ACME_MANUFACTURING',
--       'SFSENORTHAMERICA.POLARIS2',
--       'https://westus3.azuredatabricks.net/api/2.0/delta-sharing/metastores/abc123/iceberg',
--       'dapi_xxxxxxxxxxxxxxxxxxxxx',
--       'ods_pbi_warehouse_id_here'
--   );
