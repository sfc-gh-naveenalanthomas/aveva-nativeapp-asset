-- =============================================================================
-- AVEVA CONNECT - End-to-End Customer Onboarding Stored Procedure
-- =============================================================================
-- Single CALL to onboard a customer. 5 required + 2 optional parameters:
--
--   CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
--       'ACME_MANUFACTURING',                                    -- customer_name
--       'SFSENORTHAMERICA.POLARIS2',                             -- customer_account
--       'https://sharing.delta.io/delta-sharing/metastores/abc', -- catalog_uri
--       '<BEARER_TOKEN>',                                        -- bearer_token
--       'warehouse_id_from_databricks',                          -- warehouse_id
--       'AZURE_WESTUS2',                                         -- region (optional)
--       'AZURE'                                                  -- cloud  (optional hint)
--   );
--
-- region defaults to provider's CURRENT_REGION() if not provided.
-- Accepts flexible input: 'AZURE_WESTUS2', 'azure westus2', 'PUBLIC.AZURE_WESTUS2'
-- Validated against SHOW REGIONS. Cloud is derived automatically.
-- P_CLOUD is only needed as a hint when passing a short region like 'westus2'.
--
-- Everything else uses defaults from AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG.
-- The proc handles EVERYTHING: app package, stage, version, catalog integration,
-- CLD, data sharing, proxy views, listing, and customer registration.
-- All with IF NOT EXISTS / idempotent patterns. Safe to re-run.
--
-- No external volume or Azure consent needed — Delta Sharing IRC handles
-- data access via the bearer token. The sharing server serves the data.
--
-- DISTRIBUTION MODES:
--   ORGANIZATION — Uses CREATE ORGANIZATION LISTING (same org, INTERNAL distribution)
--   EXTERNAL     — Uses CREATE EXTERNAL LISTING (cross-org, requires security review)
--
-- PREREQUISITES:
--   - Run setup_and_upload.sql once (creates DB, tables, seeds config, uploads app files)
--   - ACCOUNTADMIN role
--   - App source files uploaded to the APP_SOURCE_STAGE
-- =============================================================================


-- =============================================================================
-- SETUP (run once — or use setup_and_upload.sql for a complete setup)
-- =============================================================================

USE ROLE ACCOUNTADMIN;

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

-- Seed default configurations (idempotent — won't overwrite existing values)
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
    SELECT 'CLOUD', 'AWS'
    WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'CLOUD');


-- =============================================================================
-- STORED PROCEDURE
-- =============================================================================

CREATE OR REPLACE PROCEDURE AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
    P_CUSTOMER_NAME    VARCHAR,                -- e.g. 'ACME_MANUFACTURING' (alphanumeric + underscore)
    P_CUSTOMER_ACCOUNT VARCHAR,                -- e.g. 'SFSENORTHAMERICA.POLARIS2'
    P_CATALOG_URI      VARCHAR,                -- Delta Sharing endpoint URL
    P_BEARER_TOKEN     VARCHAR,                -- Databricks PAT or Delta Sharing recipient token
    P_WAREHOUSE_ID     VARCHAR,                -- Databricks warehouse/share ID
    P_REGION           VARCHAR DEFAULT NULL,   -- e.g. 'PUBLIC.AZURE_WESTUS2' (NULL = use config)
    P_CLOUD            VARCHAR DEFAULT NULL    -- e.g. 'AZURE' (NULL = derived from region)
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    -- Defaults loaded from ONBOARDING_CONFIG
    v_app_pkg         VARCHAR;
    v_app_version     VARCHAR;
    v_app_stage       VARCHAR;
    v_warehouse       VARCHAR;
    v_dist_mode       VARCHAR;
    v_subtitle        VARCHAR;
    v_description     VARCHAR;
    v_region          VARCHAR;
    v_cloud           VARCHAR;
    v_input           VARCHAR;

    -- Derived names
    v_catalog_int     VARCHAR;
    v_cld_db          VARCHAR;
    v_listing_name    VARCHAR;
    v_listing_title   VARCHAR;
    v_stage_schema    VARCHAR;
    v_short_account   VARCHAR;

    -- Listing construction
    v_listing_yaml    VARCHAR;
    v_listing_sql     VARCHAR;

    -- Tracking
    v_steps_ok        INTEGER DEFAULT 0;
    v_steps_failed    INTEGER DEFAULT 0;
    v_result          VARCHAR DEFAULT '';
    v_err             VARCHAR;
    v_proxy_count     INTEGER DEFAULT 0;
BEGIN

    -- =========================================================================
    -- Load defaults from ONBOARDING_CONFIG
    -- =========================================================================
    SELECT CONFIG_VALUE INTO v_app_pkg        FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'APP_PKG_NAME';
    SELECT CONFIG_VALUE INTO v_app_version    FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'APP_VERSION';
    SELECT CONFIG_VALUE INTO v_app_stage      FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'APP_SOURCE_STAGE';
    SELECT CONFIG_VALUE INTO v_warehouse      FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'WAREHOUSE_NAME';
    SELECT CONFIG_VALUE INTO v_dist_mode      FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'DISTRIBUTION_MODE';
    SELECT CONFIG_VALUE INTO v_subtitle       FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'LISTING_SUBTITLE';
    SELECT CONFIG_VALUE INTO v_description    FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG WHERE CONFIG_KEY = 'LISTING_DESCRIPTION';

    -- =========================================================================
    -- Resolve region & cloud
    -- =========================================================================
    -- If not provided: default to provider's region via CURRENT_REGION()
    -- If provided: normalize input and validate against SHOW REGIONS
    IF (:P_REGION IS NULL) THEN
        SELECT CURRENT_REGION() INTO v_region;
    ELSE
        -- Normalize: uppercase, spaces to underscores, strip PUBLIC. prefix
        v_input := UPPER(REPLACE(TRIM(:P_REGION), ' ', '_'));
        IF (STARTSWITH(:v_input, 'PUBLIC.')) THEN
            v_input := SUBSTR(:v_input, 8);
        END IF;
        -- If cloud hint provided and input doesn't already start with it
        IF (:P_CLOUD IS NOT NULL AND NOT STARTSWITH(:v_input, UPPER(:P_CLOUD))) THEN
            v_input := UPPER(:P_CLOUD) || '_' || :v_input;
        END IF;
        -- Validate against SHOW REGIONS
        SHOW REGIONS;
        BEGIN
            SELECT "region_group" || '.' || "snowflake_region" INTO v_region
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            WHERE "snowflake_region" = :v_input AND "region_group" = 'PUBLIC'
            LIMIT 1;
        EXCEPTION WHEN OTHER THEN
            v_region := NULL;
        END;
        IF (:v_region IS NULL) THEN
            RETURN 'ERROR: Invalid region "' || :P_REGION || '" (normalized: "' || :v_input || '"). Run SHOW REGIONS for valid values.';
        END IF;
    END IF;
    -- Derive cloud from resolved region: PUBLIC.AZURE_WESTUS2 -> AZURE
    v_cloud := SPLIT_PART(REPLACE(:v_region, 'PUBLIC.', ''), '_', 1);

    -- Derive names from customer_name
    v_catalog_int  := 'AVEVA_' || :P_CUSTOMER_NAME || '_CATALOG_INT';
    v_cld_db       := 'AVEVA_' || :P_CUSTOMER_NAME || '_DB';
    v_listing_name := 'AVEVA_CONNECT_' || :P_CUSTOMER_NAME || '_LISTING';
    v_listing_title:= 'AVEVA Connect - ' || REPLACE(:P_CUSTOMER_NAME, '_', ' ');
    v_stage_schema := SUBSTR(:v_app_stage, 1, LENGTH(:v_app_stage) - LENGTH(SPLIT_PART(:v_app_stage, '.', -1)) - 1);
    v_short_account := SPLIT_PART(:P_CUSTOMER_ACCOUNT, '.', -1);

    -- =========================================================================
    -- STEP 1: App Package, Stage & Version (IF NOT EXISTS)
    -- =========================================================================
    BEGIN
        EXECUTE IMMEDIATE 'CREATE APPLICATION PACKAGE IF NOT EXISTS ' || :v_app_pkg;
        EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :v_stage_schema;
        EXECUTE IMMEDIATE 'CREATE STAGE IF NOT EXISTS ' || :v_app_stage ||
            ' DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = ''SNOWFLAKE_SSE'')';

        -- Add version (skip gracefully if already exists)
        BEGIN
            EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg ||
                ' ADD VERSION ' || :v_app_version || ' USING ''@' || :v_app_stage || '''';
        EXCEPTION WHEN OTHER THEN NULL;
        END;

        -- Release channel: add version to default channel
        BEGIN
            EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg ||
                ' MODIFY RELEASE CHANNEL DEFAULT ADD VERSION ' || :v_app_version;
        EXCEPTION WHEN OTHER THEN NULL;
        END;

        -- Set release directive (skip gracefully if already set or no files)
        BEGIN
            EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg ||
                ' MODIFY RELEASE CHANNEL DEFAULT SET DEFAULT RELEASE DIRECTIVE VERSION = ' || :v_app_version || ' PATCH = 0';
        EXCEPTION WHEN OTHER THEN
            -- Fallback for packages without release channels
            BEGIN
                EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg ||
                    ' SET DEFAULT RELEASE DIRECTIVE VERSION = ' || :v_app_version || ' PATCH = 0';
            EXCEPTION WHEN OTHER THEN NULL;
            END;
        END;

        -- Enable auto-fulfillment refresh (auto-replicate to consumer regions on release directive change)
        BEGIN
            EXECUTE IMMEDIATE 'ALTER APPLICATION PACKAGE ' || :v_app_pkg || ' SET LISTING_AUTO_REFRESH = ''ON''';
        EXCEPTION WHEN OTHER THEN NULL;
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

    -- =========================================================================
    -- STEP 2: Catalog Integration (Delta Sharing IRC)
    -- =========================================================================
    -- WAREHOUSE = Databricks warehouse/share ID, NOT a Snowflake warehouse.
    -- ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS required for Delta Sharing.
    -- No external volume needed — the sharing server handles data access.
    BEGIN
        EXECUTE IMMEDIATE
            'CREATE CATALOG INTEGRATION IF NOT EXISTS ' || :v_catalog_int ||
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

    -- =========================================================================
    -- STEP 3: Catalog-Linked Database
    -- =========================================================================
    -- CLD auto-discovers tables from the Delta Sharing catalog.
    -- No EXTERNAL_VOLUME needed — Delta Sharing serves data via the endpoint.
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

    -- =========================================================================
    -- STEP 4: Share Data to App Package (IF NOT EXISTS)
    -- =========================================================================
    BEGIN
        EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :v_app_pkg || '.SHARED_CONTENT';
        EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA ' || :v_app_pkg || '.SHARED_CONTENT TO SHARE IN APPLICATION PACKAGE ' || :v_app_pkg;
        -- REFERENCE_USAGE on CLD (may not exist if step 3 failed — skip gracefully)
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

    -- =========================================================================
    -- STEP 5: Create Proxy Views (CLD tables -> app package share)
    -- =========================================================================
    -- Discovers tables from CLD and creates secure proxy views in the app
    -- package. These views are granted to the share so the consumer app's
    -- setup.sql can discover them via SHOW VIEWS IN SCHEMA PROXY_VIEWS.
    BEGIN
        EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS ' || :v_app_pkg || '.PROXY_VIEWS';
        EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA ' || :v_app_pkg || '.PROXY_VIEWS TO SHARE IN APPLICATION PACKAGE ' || :v_app_pkg;

        -- Discover tables from CLD and create proxy views
        -- Note: CLD tables may take a moment to sync. SHOW TABLES discovers what's available.
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
            VALUES (:P_CUSTOMER_NAME, 5, 'Proxy Views', 'SUCCESS', :v_proxy_count || ' proxy views created');
        v_steps_ok := v_steps_ok + 1;
    EXCEPTION WHEN OTHER THEN
        v_err := SQLERRM;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 5, 'Proxy Views', 'FAILED', :v_err);
        v_steps_failed := v_steps_failed + 1;
        v_result := v_result || 'STEP 5 FAILED: ' || :v_err || '\n';
    END;

    -- =========================================================================
    -- STEP 6: Create Listing
    -- =========================================================================
    -- ORGANIZATION mode → CREATE ORGANIZATION LISTING (INTERNAL distribution)
    -- EXTERNAL mode    → CREATE EXTERNAL LISTING (requires security review)
    BEGIN
        -- Set distribution (skip if already set or can't change)
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
                'auto_fulfillment:' || CHR(10) ||
                '  refresh_type: SUB_DATABASE' || CHR(10) ||
                'resharing:' || CHR(10) ||
                '  enabled: false';
            v_listing_sql := 'CREATE ORGANIZATION LISTING ' || :v_listing_name ||
                ' APPLICATION PACKAGE ' || :v_app_pkg ||
                ' AS ''' || REPLACE(:v_listing_yaml, '''', '''''') || '''' ||
                ' PUBLISH = FALSE REVIEW = FALSE';
        ELSE
            -- Add target account (skip if already added)
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
                'auto_fulfillment:' || CHR(10) ||
                '  refresh_type: SUB_DATABASE' || CHR(10) ||
                'resharing:' || CHR(10) ||
                '  enabled: false';
            v_listing_sql := 'CREATE EXTERNAL LISTING ' || :v_listing_name ||
                ' APPLICATION PACKAGE ' || :v_app_pkg ||
                ' AS ''' || REPLACE(:v_listing_yaml, '''', '''''') || '''' ||
                ' PUBLISH = FALSE REVIEW = FALSE';
        END IF;
        BEGIN
            EXECUTE IMMEDIATE :v_listing_sql;
        EXCEPTION WHEN OTHER THEN NULL;
        END;
        BEGIN
            EXECUTE IMMEDIATE 'ALTER LISTING ' || :v_listing_name || ' PUBLISH';
        EXCEPTION WHEN OTHER THEN NULL;
        END;

        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 6, 'Listing', 'SUCCESS', 'Published: ' || :v_listing_name || ' (region=' || :v_region || ', cloud=' || :v_cloud || ')');
        v_steps_ok := v_steps_ok + 1;
    EXCEPTION WHEN OTHER THEN
        v_err := SQLERRM;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 6, 'Listing', 'FAILED', :v_err);
        v_steps_failed := v_steps_failed + 1;
        v_result := v_result || 'STEP 6 FAILED: ' || :v_err || '\n';
    END;

    -- =========================================================================
    -- STEP 7: Register Customer (idempotent)
    -- =========================================================================
    BEGIN
        EXECUTE IMMEDIATE
            'INSERT INTO AVEVA_CONNECT.ADMIN.CUSTOMERS (CUSTOMER_NAME, SNOWFLAKE_ACCOUNT_ID, STATUS, NOTES)
             SELECT ''' || :P_CUSTOMER_NAME || ''', ''' || :P_CUSTOMER_ACCOUNT || ''', ''ACTIVE'',
                    ''Onboarded via proc. Distribution: ' || :v_dist_mode || '.''
             WHERE NOT EXISTS (SELECT 1 FROM AVEVA_CONNECT.ADMIN.CUSTOMERS WHERE SNOWFLAKE_ACCOUNT_ID = ''' || :P_CUSTOMER_ACCOUNT || ''')';

        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 7, 'Register Customer', 'SUCCESS', 'Done');
        v_steps_ok := v_steps_ok + 1;
    EXCEPTION WHEN OTHER THEN
        v_err := SQLERRM;
        INSERT INTO AVEVA_CONNECT.ADMIN.ONBOARDING_LOG (CUSTOMER_NAME, STEP_NUMBER, STEP_NAME, STATUS, MESSAGE)
            VALUES (:P_CUSTOMER_NAME, 7, 'Register Customer', 'FAILED', :v_err);
        v_steps_failed := v_steps_failed + 1;
        v_result := v_result || 'STEP 7 FAILED: ' || :v_err || '\n';
    END;

    -- =========================================================================
    -- RESULT
    -- =========================================================================
    IF (v_steps_failed = 0) THEN
        RETURN 'SUCCESS: All ' || v_steps_ok || '/7 steps completed. '
            || 'Pkg=' || :v_app_pkg
            || ', CLD=' || :v_cld_db
            || ', Listing=' || :v_listing_name
            || ', Region=' || :v_region
            || ', Cloud=' || :v_cloud
            || '\n\nCONSUMER MUST RUN (after installing the app):\n'
            || '1. GRANT USAGE ON WAREHOUSE <wh> TO APPLICATION <app_name>;\n'
            || '2. GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION <app_name>;\n'
            || '3. SELECT SYSTEM$SET_APPLICATION_RESTRICTED_FEATURE_ACCESS(''<app_name>'', ''external_data'', ''{\"allowed_cloud_providers\":\"all\"}'');';
    ELSE
        RETURN 'PARTIAL: ' || v_steps_ok || ' ok, ' || v_steps_failed || ' failed.\n' || v_result
            || 'Check: SELECT * FROM AVEVA_CONNECT.ADMIN.ONBOARDING_LOG WHERE CUSTOMER_NAME = ''' || :P_CUSTOMER_NAME || ''' ORDER BY STEP_NUMBER;';
    END IF;

END;
$$;


-- =============================================================================
-- USAGE
-- =============================================================================
--
-- 1. Upload app files once (from the aveva_connect_native_app/ directory):
--
--    snow stage copy manifest.yml @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/ --overwrite
--    snow stage copy setup.sql @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/ --overwrite
--    snow stage copy README.md @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/ --overwrite
--    snow stage copy streamlit/ @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/streamlit/ --recursive --overwrite
--
-- 2. Onboard a customer:
--
--    -- Same region as provider (region auto-detected via CURRENT_REGION()):
--    CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
--        'ACME_MANUFACTURING',
--        'SFSENORTHAMERICA.POLARIS2',
--        'https://westus3.azuredatabricks.net/api/2.0/delta-sharing/metastores/abc123/iceberg',
--        'dapi_xxxxxxxxxxxxxxxxxxxxx',
--        'ods_pbi_warehouse_id_here'
--    );
--
--    -- Azure target region (Aveva use case):
--    CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
--        'AVEVA_PROD',
--        'AVEVAORG.AVEVA_ACCOUNT',
--        'https://westus3.azuredatabricks.net/api/2.0/delta-sharing/metastores/abc123/iceberg',
--        'dapi_xxxxxxxxxxxxxxxxxxxxx',
--        'ods_pbi_warehouse_id_here',
--        'AZURE_WESTUS2'
--    );
--
--    -- Short region with cloud hint:
--    CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
--        'AVEVA_PROD', 'AVEVAORG.ACCT', '...', '...', '...',
--        'westus2', 'AZURE'
--    );
--
--    Flexible region input — all of these resolve to PUBLIC.AZURE_WESTUS2:
--      'AZURE_WESTUS2'           -- standard
--      'azure westus2'           -- spaces + lowercase ok
--      'PUBLIC.AZURE_WESTUS2'    -- full form ok
--      'westus2' + cloud='AZURE' -- cloud hint builds the full name
--
-- 3. Override defaults (before calling):
--
--    UPDATE AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG
--    SET CONFIG_VALUE = 'EXTERNAL' WHERE CONFIG_KEY = 'DISTRIBUTION_MODE';
--
-- 4. Check results:
--
--    SELECT * FROM AVEVA_CONNECT.ADMIN.ONBOARDING_LOG
--    WHERE CUSTOMER_NAME = 'ACME_MANUFACTURING' ORDER BY STEP_NUMBER;
--
-- 5. Re-run is safe — all steps are idempotent (IF NOT EXISTS / NOT EXISTS / OR REPLACE)
--
-- 6. Cleanup a customer:
--
--    DROP LISTING IF EXISTS AVEVA_CONNECT_ACME_MANUFACTURING_LISTING;
--    DROP DATABASE IF EXISTS AVEVA_ACME_MANUFACTURING_DB;
--    DROP CATALOG INTEGRATION IF EXISTS AVEVA_ACME_MANUFACTURING_CATALOG_INT;
--    DELETE FROM AVEVA_CONNECT.ADMIN.CUSTOMERS WHERE SNOWFLAKE_ACCOUNT_ID = 'SFSENORTHAMERICA.POLARIS2';
--    DELETE FROM AVEVA_CONNECT.ADMIN.ONBOARDING_LOG WHERE CUSTOMER_NAME = 'ACME_MANUFACTURING';
--
-- 7. Auto-fulfillment:
--    The listing includes auto_fulfillment with SUB_DATABASE refresh.
--    LISTING_AUTO_REFRESH is set ON for the app package.
--    When listing targets a remote region, Snowflake auto-replicates the app.
