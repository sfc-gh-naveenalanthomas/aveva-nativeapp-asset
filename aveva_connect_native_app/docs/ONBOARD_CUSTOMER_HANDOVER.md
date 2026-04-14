# AVEVA Connect -- Customer Onboarding Procedure

## Technical Handover Document

**Script:** `onboard_customer_e2e.sql`
**Procedure:** `AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER`
**Version:** Current
**Distribution:** Snowflake Native App via Organization or External Listing

---

## 1. Overview

The `ONBOARD_CUSTOMER` stored procedure automates the end-to-end provisioning of an AVEVA Connect Native App for a new customer. A single `CALL` statement executes a 7-step pipeline that creates all required Snowflake objects, connects to the customer's Delta Sharing endpoint, and publishes a listing the consumer account can install from.

The procedure is **idempotent** -- every step uses `IF NOT EXISTS`, `OR REPLACE`, or `NOT EXISTS` guards. It is safe to re-run against the same customer without side effects.

---

## 2. Parameters

| # | Parameter | Type | Required | Description |
|---|-----------|------|----------|-------------|
| 1 | `P_CUSTOMER_NAME` | VARCHAR | Yes | Unique identifier for the customer (alphanumeric + underscores). Used to derive all object names. Example: `'ACME_MANUFACTURING'` |
| 2 | `P_CUSTOMER_ACCOUNT` | VARCHAR | Yes | Fully qualified Snowflake account identifier. Format: `ORG_NAME.ACCOUNT_NAME`. Example: `'SFSENORTHAMERICA.POLARIS2'` |
| 3 | `P_CATALOG_URI` | VARCHAR | Yes | Delta Sharing / Databricks Unity Catalog Iceberg REST endpoint URL. Example: `'https://westus3.azuredatabricks.net/api/2.0/delta-sharing/metastores/<id>/iceberg'` |
| 4 | `P_BEARER_TOKEN` | VARCHAR | Yes | Databricks personal access token (PAT) or Delta Sharing recipient token for authenticating to the catalog endpoint. |
| 5 | `P_WAREHOUSE_ID` | VARCHAR | Yes | Databricks SQL warehouse ID or Delta Sharing share ID. This is **not** a Snowflake warehouse -- it is passed to the catalog integration's `REST_CONFIG.WAREHOUSE` parameter. |
| 6 | `P_REGION` | VARCHAR | No | Target Snowflake region for the listing. If omitted, defaults to the provider account's region via `CURRENT_REGION()`. Accepts flexible input (see Section 4). Example: `'AZURE_WESTUS2'` |
| 7 | `P_CLOUD` | VARCHAR | No | Cloud provider hint. Only needed when `P_REGION` is a short region name (e.g., `'westus2'`) and the cloud prefix cannot be inferred. Example: `'AZURE'` |

---

## 3. Prerequisites

Before calling the procedure:

1. **Run the setup section of the script** (or `setup_and_upload.sql`). This creates:
   - Database `AVEVA_CONNECT` and schema `AVEVA_CONNECT.ADMIN`
   - Tables: `CUSTOMERS`, `ONBOARDING_CONFIG`, `ONBOARDING_LOG`
   - Default configuration seed values in `ONBOARDING_CONFIG`

2. **Upload application source files** to the app package stage:
   ```
   snow stage copy manifest.yml   @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/ --overwrite
   snow stage copy setup.sql      @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/ --overwrite
   snow stage copy README.md      @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/ --overwrite
   snow stage copy streamlit/     @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/streamlit/ --recursive --overwrite
   ```

3. **Role:** `ACCOUNTADMIN` (the procedure runs as `EXECUTE AS CALLER`).

4. **Have the Delta Sharing credentials ready:** catalog URI, bearer token, and warehouse ID from Databricks.

---

## 4. Region Resolution

The procedure includes built-in region handling with input normalization and validation.

### Default Behavior (no region provided)

When `P_REGION` is `NULL`, the procedure calls `CURRENT_REGION()` which returns the provider account's own region (e.g., `PUBLIC.AWS_US_WEST_2`). No elevated privileges are required.

### Explicit Region (P_REGION provided)

When a region is provided, the procedure:

1. **Normalizes** the input: trims whitespace, converts to uppercase, replaces spaces with underscores.
2. **Strips** the `PUBLIC.` prefix if present (it is added back after validation).
3. **Prepends cloud hint** if `P_CLOUD` is provided and the input does not already start with that cloud prefix. For example, `P_REGION='westus2'` + `P_CLOUD='AZURE'` becomes `AZURE_WESTUS2`.
4. **Validates** against `SHOW REGIONS` to confirm the normalized region exists in Snowflake's region catalog.
5. **Returns an error** if the region is not found, including the original and normalized values for debugging.

### Accepted Input Formats

All of the following resolve to `PUBLIC.AZURE_WESTUS2`:

| Input | Cloud Hint | Result |
|-------|-----------|--------|
| `'AZURE_WESTUS2'` | -- | `PUBLIC.AZURE_WESTUS2` |
| `'azure westus2'` | -- | `PUBLIC.AZURE_WESTUS2` |
| `'PUBLIC.AZURE_WESTUS2'` | -- | `PUBLIC.AZURE_WESTUS2` |
| `'westus2'` | `'AZURE'` | `PUBLIC.AZURE_WESTUS2` |

### Cloud Derivation

The cloud provider (AWS, AZURE, GCP) is automatically derived from the resolved region by extracting the first segment before the underscore. For example, `PUBLIC.AZURE_WESTUS2` yields `AZURE`. This value is used in the success message and onboarding log for operational visibility.

---

## 5. Configuration

The procedure reads runtime defaults from the `AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG` table. These can be updated before calling the procedure to change behavior without modifying the code.

| Config Key | Default Value | Description |
|-----------|---------------|-------------|
| `APP_PKG_NAME` | `AVEVA_CONNECT_APP_PKG` | Name of the Snowflake Application Package |
| `APP_VERSION` | `V1_0` | Version label for the app package |
| `APP_SOURCE_STAGE` | `AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE` | Fully qualified stage containing app source files (manifest.yml, setup.sql, Streamlit app) |
| `WAREHOUSE_NAME` | `COMPUTE_WH` | Snowflake warehouse (reserved for future use) |
| `DISTRIBUTION_MODE` | `ORGANIZATION` | `ORGANIZATION` for same-org sharing (INTERNAL distribution) or `EXTERNAL` for cross-org sharing |
| `LISTING_SUBTITLE` | `Industrial PI Data Analytics` | Subtitle displayed on the listing |
| `LISTING_DESCRIPTION` | `Cross-account PI data historian analytics powered by Cortex AI` | Description displayed on the listing |
| `ACCESS_REGION` | `PUBLIC.AWS_US_WEST_2` | Legacy config value (not used by the procedure -- region is now resolved via parameters or `CURRENT_REGION()`) |
| `CLOUD` | `AWS` | Legacy config value (cloud is now derived from the resolved region) |

To change a value:
```sql
UPDATE AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG
SET CONFIG_VALUE = 'EXTERNAL' WHERE CONFIG_KEY = 'DISTRIBUTION_MODE';
```

---

## 6. Execution Pipeline (7 Steps)

Each step is wrapped in a `BEGIN...EXCEPTION` block. If a step fails, the error is captured and logged, and execution continues to the next step. This ensures maximum progress even when individual steps encounter issues.

### Step 1: Application Package, Stage, and Version

**Objects created:**
- Application Package (e.g., `AVEVA_CONNECT_APP_PKG`)
- Schema for the stage (e.g., `AVEVA_CONNECT_APP_PKG.APP_SRC`)
- Internal stage with directory and SSE encryption (e.g., `AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE`)

**Actions:**
1. `CREATE APPLICATION PACKAGE IF NOT EXISTS`
2. `CREATE SCHEMA IF NOT EXISTS` for the stage
3. `CREATE STAGE IF NOT EXISTS` with directory enabled and Snowflake SSE encryption
4. `ALTER APPLICATION PACKAGE ... ADD VERSION` using the source stage (skips if version exists)
5. `ALTER APPLICATION PACKAGE ... MODIFY RELEASE CHANNEL DEFAULT ADD VERSION` (skips if already added)
6. `ALTER APPLICATION PACKAGE ... SET DEFAULT RELEASE DIRECTIVE` with fallback for packages without release channels
7. `ALTER APPLICATION PACKAGE ... SET LISTING_AUTO_REFRESH = 'ON'` -- enables automatic replication to consumer regions when release directives change

**Note on versioning:** The stage must contain valid app source files (`manifest.yml`, `setup.sql`, Streamlit app) before calling the procedure. The `ADD VERSION` command reads from the stage at execution time.

### Step 2: Catalog Integration (Delta Sharing IRC)

**Object created:** Catalog Integration (e.g., `AVEVA_ACME_MANUFACTURING_CATALOG_INT`)

**Configuration:**
- `CATALOG_SOURCE = ICEBERG_REST` with `TABLE_FORMAT = ICEBERG`
- `REST_CONFIG`: Uses the provided catalog URI and Databricks warehouse ID
- `ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS` -- required for Delta Sharing; the sharing server provides temporary credentials to access the underlying storage
- `REST_AUTHENTICATION = BEARER` with the provided token
- `REFRESH_INTERVAL_SECONDS = 30` -- how often Snowflake refreshes catalog metadata

**Important:** No external volume or Azure/AWS storage consent is needed. The Delta Sharing server handles all data access. Snowflake communicates with the REST endpoint using the bearer token, and the server vends short-lived credentials for the actual data files.

### Step 3: Catalog-Linked Database (CLD)

**Object created:** Database (e.g., `AVEVA_ACME_MANUFACTURING_DB`)

**Configuration:**
- `LINKED_CATALOG` points to the catalog integration created in Step 2
- `SYNC_INTERVAL_SECONDS = 300` -- Snowflake re-syncs the table catalog every 5 minutes
- `NAMESPACE_MODE = FLATTEN_NESTED_NAMESPACE` -- flattens nested Databricks namespaces (catalog/schema/table) into Snowflake-compatible schema/table structure
- `NAMESPACE_FLATTEN_DELIMITER = '/'` -- uses `/` as the delimiter when flattening

The CLD automatically discovers and surfaces all tables shared through the Delta Sharing endpoint. New tables added to the share appear in the CLD after the next sync interval.

### Step 4: Data Sharing Grants

**Actions:**
1. `CREATE SCHEMA IF NOT EXISTS <app_pkg>.SHARED_CONTENT` -- shared content schema within the app package
2. `GRANT USAGE ON SCHEMA <app_pkg>.SHARED_CONTENT TO SHARE IN APPLICATION PACKAGE` -- makes the schema accessible to the app
3. `GRANT REFERENCE_USAGE ON DATABASE <cld_db> TO SHARE IN APPLICATION PACKAGE` -- allows the app package to reference CLD tables through views
4. `GRANT REFERENCE_USAGE ON DATABASE AVEVA_CONNECT TO SHARE IN APPLICATION PACKAGE` -- allows the app package to reference the admin database (for configuration and logging tables)

**Note:** The `REFERENCE_USAGE` grant on the CLD is wrapped in its own exception handler. If Step 3 failed (CLD not created), this grant is skipped gracefully rather than causing Step 4 to fail entirely.

### Step 5: Proxy View Discovery and Creation

**Objects created:** Secure views in `<app_pkg>.PROXY_VIEWS` schema (e.g., `AVEVA_CONNECT_APP_PKG.PROXY_VIEWS."rotatingmachine_fixed_26q1"`)

**How it works:**
1. Creates the `PROXY_VIEWS` schema in the app package
2. Grants usage on the schema to the app package share
3. Runs `SHOW TABLES IN DATABASE <cld_db>` to discover all tables that have synced from the Delta Sharing catalog
4. Iterates over each discovered table using a cursor
5. For each table, creates a `SECURE VIEW` that `SELECT * FROM` the CLD table
6. Grants `SELECT` on each view to the app package share
7. Logs the total number of proxy views created

**Why proxy views:** Snowflake Native App packages cannot directly share CLD/Iceberg tables. Secure views act as a proxy layer -- the consumer app's `setup.sql` discovers available data by running `SHOW VIEWS IN SCHEMA PROXY_VIEWS` and creates application-level views that reference them.

**Table sync timing:** CLD tables may take up to the sync interval (5 minutes) to appear after the catalog integration is created. If the procedure runs immediately after Step 3, the CLD may not have synced yet, resulting in 0 proxy views. Re-running the procedure will pick up any newly synced tables.

### Step 6: Listing Creation and Publication

**Object created:** Listing (e.g., `AVEVA_CONNECT_ACME_MANUFACTURING_LISTING`)

**Two distribution modes:**

**ORGANIZATION mode** (default -- same Snowflake organization):
- Sets app package distribution to `INTERNAL`
- Creates an `ORGANIZATION LISTING` with:
  - `organization_profile: "INTERNAL"`
  - `organization_targets` scoped to the specific consumer account
  - `access_regions` set to the resolved region
  - `auto_fulfillment` with `refresh_type: SUB_DATABASE`
  - `resharing: enabled: false`
- Listing is created unpublished (`PUBLISH = FALSE, REVIEW = FALSE`) then immediately published via `ALTER LISTING ... PUBLISH`

**EXTERNAL mode** (cross-organization):
- Sets app package distribution to `EXTERNAL`
- Adds the consumer account as a target account on the app package
- Creates an `EXTERNAL LISTING` with:
  - `targets.accounts` scoped to the specific consumer account
  - Same `access_regions`, `auto_fulfillment`, and `resharing` settings
- External listings require Snowflake security review before consumer installation

**Auto-fulfillment:** When the listing targets a region different from the provider's region, Snowflake automatically replicates the application package to the consumer's region. The `LISTING_AUTO_REFRESH = 'ON'` setting (from Step 1) combined with `auto_fulfillment: refresh_type: SUB_DATABASE` in the listing YAML enables this. Replication is managed entirely by Snowflake -- no manual intervention is required.

### Step 7: Customer Registration

**Action:** Inserts a record into `AVEVA_CONNECT.ADMIN.CUSTOMERS` with:
- Customer name and Snowflake account identifier
- Status: `ACTIVE`
- Notes indicating the distribution mode used

The insert uses a `NOT EXISTS` subquery to prevent duplicate registration if the procedure is re-run for the same account.

---

## 7. Naming Conventions

All object names are derived deterministically from `P_CUSTOMER_NAME`:

| Object | Naming Pattern | Example (`P_CUSTOMER_NAME = 'ACME'`) |
|--------|---------------|---------------------------------------|
| Catalog Integration | `AVEVA_<name>_CATALOG_INT` | `AVEVA_ACME_CATALOG_INT` |
| Catalog-Linked Database | `AVEVA_<name>_DB` | `AVEVA_ACME_DB` |
| Listing | `AVEVA_CONNECT_<name>_LISTING` | `AVEVA_CONNECT_ACME_LISTING` |
| Listing Title | `AVEVA Connect - <name>` | `AVEVA Connect - ACME` |
| Proxy Views Schema | `<app_pkg>.PROXY_VIEWS` | `AVEVA_CONNECT_APP_PKG.PROXY_VIEWS` |

The application package name and version are read from `ONBOARDING_CONFIG`, not derived from the customer name.

---

## 8. Observability and Logging

Every step logs its outcome to `AVEVA_CONNECT.ADMIN.ONBOARDING_LOG`:

| Column | Description |
|--------|-------------|
| `CUSTOMER_NAME` | The customer this log entry belongs to |
| `STEP_NUMBER` | 1-7, corresponding to the pipeline step |
| `STEP_NAME` | Human-readable step name |
| `STATUS` | `SUCCESS` or `FAILED` |
| `MESSAGE` | Details -- object names on success, error message on failure |
| `EXECUTED_AT` | Timestamp of execution |

**Query logs for a specific customer:**
```sql
SELECT * FROM AVEVA_CONNECT.ADMIN.ONBOARDING_LOG
WHERE CUSTOMER_NAME = 'ACME_MANUFACTURING'
ORDER BY STEP_NUMBER;
```

---

## 9. Return Values

### On Full Success (all 7 steps pass)

```
SUCCESS: All 7/7 steps completed. Pkg=AVEVA_CONNECT_APP_PKG, CLD=AVEVA_ACME_DB,
Listing=AVEVA_CONNECT_ACME_LISTING, Region=PUBLIC.AZURE_WESTUS2, Cloud=AZURE

CONSUMER MUST RUN (after installing the app):
1. GRANT USAGE ON WAREHOUSE <wh> TO APPLICATION <app_name>;
2. GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION <app_name>;
3. SELECT SYSTEM$SET_APPLICATION_RESTRICTED_FEATURE_ACCESS('<app_name>', 'external_data',
   '{"allowed_cloud_providers":"all"}');
```

### On Partial Failure

```
PARTIAL: 5 ok, 2 failed.
STEP 2 FAILED: <error details>
STEP 3 FAILED: <error details>
Check: SELECT * FROM AVEVA_CONNECT.ADMIN.ONBOARDING_LOG WHERE CUSTOMER_NAME = 'ACME' ORDER BY STEP_NUMBER;
```

### On Invalid Region

```
ERROR: Invalid region "BANANA" (normalized: "BANANA"). Run SHOW REGIONS for valid values.
```

The procedure exits immediately on invalid region -- no pipeline steps are executed.

---

## 10. Consumer Post-Installation Steps

The app is designed for **zero-friction onboarding**. All required permissions are handled through in-app consent dialogs -- the consumer does not need to run any manual SQL commands.

### Installation Flow

1. **Install the app** from the Snowflake Marketplace / Organization listing via Snowsight.
2. **Associate a warehouse** -- Snowsight prompts for this during installation. The consumer selects an existing warehouse.
3. **Open the app** -- The Streamlit UI guides the consumer through the remaining permissions:

### Automated Permission Gates (in-app)

The Streamlit app presents sequential consent dialogs for any missing permissions:

| Gate | What it grants | How it works |
|------|---------------|--------------|
| **External Data Access** | Allows the app to read shared CLD/Iceberg tables from the provider | Button "Grant External Data Access" triggers `permissions.request_external_data()` -- Snowsight consent dialog |
| **Cortex AI Access** | Grants `IMPORTED PRIVILEGES ON SNOWFLAKE DB` for Cortex AI functions | Button "Grant Cortex AI Access" triggers `permissions.request_account_privileges()` -- Snowsight consent dialog |

Once both permissions are granted, the app proceeds to initialization (view discovery, metadata classification, unified view creation, semantic view generation).

### Privileges Auto-Granted at Install (manifest_version 2)

The following privileges are **automatically granted** when the consumer installs the app (no user action required):

- `EXECUTE TASK` -- background task execution
- `EXECUTE MANAGED TASK` -- managed background tasks

### Manual SQL (only if not using Snowsight)

If the consumer installs via SQL instead of Snowsight, they must grant permissions manually:

```sql
-- 1. Associate a warehouse
GRANT USAGE ON WAREHOUSE <warehouse_name> TO APPLICATION <app_name>;

-- 2. Grant Cortex AI access (Snowflake database)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION <app_name>;

-- 3. Allow external data access (CLD/Iceberg tables from provider)
SELECT SYSTEM$SET_APPLICATION_RESTRICTED_FEATURE_ACCESS(
    '<app_name>',
    'external_data',
    '{"allowed_cloud_providers":"all"}'
);
```

Replace `<warehouse_name>` with an active warehouse in the consumer account and `<app_name>` with the installed application name.

---

## 11. Usage Examples

### Same region as provider (region auto-detected)

```sql
CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
    'ACME_MANUFACTURING',
    'SFSENORTHAMERICA.POLARIS2',
    'https://westus3.azuredatabricks.net/api/2.0/delta-sharing/metastores/abc123/iceberg',
    'dapi_xxxxxxxxxxxxxxxxxxxxx',
    'ods_pbi_warehouse_id_here'
);
```

### Targeting an Azure region

```sql
CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
    'AVEVA_PROD',
    'AVEVAORG.AVEVA_ACCOUNT',
    'https://westus3.azuredatabricks.net/api/2.0/delta-sharing/metastores/abc123/iceberg',
    'dapi_xxxxxxxxxxxxxxxxxxxxx',
    'ods_pbi_warehouse_id_here',
    'AZURE_WESTUS2'
);
```

### Short region name with cloud hint

```sql
CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
    'AVEVA_PROD',
    'AVEVAORG.AVEVA_ACCOUNT',
    'https://westus3.azuredatabricks.net/api/2.0/delta-sharing/metastores/abc123/iceberg',
    'dapi_xxxxxxxxxxxxxxxxxxxxx',
    'ods_pbi_warehouse_id_here',
    'westus2',
    'AZURE'
);
```

---

## 12. Customer Cleanup

To fully remove a customer's onboarded resources:

```sql
-- Remove the listing
DROP LISTING IF EXISTS AVEVA_CONNECT_ACME_MANUFACTURING_LISTING;

-- Remove the catalog-linked database
DROP DATABASE IF EXISTS AVEVA_ACME_MANUFACTURING_DB;

-- Remove the catalog integration
DROP CATALOG INTEGRATION IF EXISTS AVEVA_ACME_MANUFACTURING_CATALOG_INT;

-- Remove customer record
DELETE FROM AVEVA_CONNECT.ADMIN.CUSTOMERS
WHERE SNOWFLAKE_ACCOUNT_ID = 'SFSENORTHAMERICA.POLARIS2';

-- Remove onboarding logs
DELETE FROM AVEVA_CONNECT.ADMIN.ONBOARDING_LOG
WHERE CUSTOMER_NAME = 'ACME_MANUFACTURING';
```

Proxy views in the app package (`PROXY_VIEWS` schema) are shared across all consumers using the same app package and should only be removed if no other customers depend on them.

---

## 13. Architecture Diagram

```
Provider Account                                         Consumer Account
==================                                       ==================

  Databricks / Delta Sharing
  +--------------------------+
  | Unity Catalog            |
  | (Iceberg REST endpoint)  |
  +-------------|------------+
                |
                | Bearer Token Auth
                v
  +-----------------------------+
  | Catalog Integration         |      LISTING
  | (ICEBERG_REST, VENDED_CREDS)|----> (auto_fulfillment) ----> Install App
  +-------------|---------------+                                    |
                |                                                    v
  +-----------------------------+                          +------------------+
  | Catalog-Linked Database     |                          | Installed App    |
  | (auto-sync every 5 min)     |                          | (Streamlit UI)  |
  +-------------|---------------+                          |                  |
                |                                          | Reads data via   |
  +-----------------------------+                          | shared views     |
  | Proxy Views (SECURE)        |<----- SHARE ----------->|                  |
  | in App Package              |   (REFERENCE_USAGE)      +------------------+
  +-----------------------------+
```

---

## 14. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Delta Sharing via Iceberg REST** | No external volume or cloud storage consent required. The sharing server handles authentication and vends temporary credentials. Simplifies cross-cloud deployment. |
| **Proxy views** | Snowflake does not allow direct sharing of CLD/Iceberg tables in app packages. Secure views provide a transparent proxy layer. |
| **EXECUTE AS CALLER** | The procedure runs with the caller's privileges, requiring ACCOUNTADMIN. This avoids ownership transfer issues and ensures full DDL/grant capabilities. |
| **Idempotent design** | All steps use IF NOT EXISTS, OR REPLACE, or NOT EXISTS patterns. Safe to re-run for recovery or to pick up new CLD tables. |
| **Config-driven defaults** | Operational settings (package name, version, distribution mode, listing text) are stored in a configuration table rather than hardcoded. Allows changes without modifying the procedure. |
| **Fail-forward execution** | Each step is independently wrapped in exception handling. A failure in one step (e.g., catalog integration) does not prevent later steps from attempting execution. The final return value summarizes what succeeded and what failed. |
| **CURRENT_REGION() default** | When no region is specified, the procedure uses the provider's own region. This requires no elevated privileges and is correct for same-region deployments. |
| **SHOW REGIONS validation** | User-provided regions are validated against Snowflake's authoritative region catalog. This catches typos and invalid input before any objects are created. |
| **Auto-fulfillment** | `LISTING_AUTO_REFRESH` + `auto_fulfillment: refresh_type: SUB_DATABASE` enables Snowflake-managed replication for cross-region consumers. No manual replication setup required. |

---

## 15. Post-Install: Consumer-Side Steps

After the consumer installs the app from the listing, two permissions must be granted before the app can function. When installing via Snowsight, both are prompted automatically on first launch.

### External Data Access (Required)

The app's `manifest.yml` declares `restricted_features: external_data`. This means the consumer must explicitly allow the app to resolve the shared CLD Iceberg tables through the proxy views.

**Via Snowsight:** The consumer sees an automatic prompt on first launch. Click "Grant".

**Via SQL** (e.g., for automated or CLI-based installs):
```sql
SELECT SYSTEM$SET_APPLICATION_RESTRICTED_FEATURE_ACCESS(
    '<app_name>', 'EXTERNAL_DATA', '{"allowed_cloud_providers": "all"}'
);
```

Without this grant, all queries against proxy views fail with: `Failure during expansion of view ... Error in secure object` or `Insufficient permission to resolve external/iceberg table`.

### Warehouse Binding (Required)

The manifest declares a `WAREHOUSE_REF` reference. Snowsight prompts the consumer to select a warehouse during install.

**Via SQL:**
```sql
CALL <app_name>.CORE.REGISTER_REFERENCE(
    'WAREHOUSE_REF', 'ADD',
    SYSTEM$REFERENCE('WAREHOUSE', 'COMPUTE_WH', 'SESSION', 'USAGE')
);
```

### Cortex AI Access (Required for NL Queries)

The manifest requests `IMPORTED PRIVILEGES ON SNOWFLAKE DB`. Snowsight prompts this automatically.

**Via SQL:**
```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION <app_name>;
```

---

## 16. App Schema Architecture

Once installed, the app creates the following schemas:

| Schema | Type | Purpose |
|--------|------|---------|
| `CORE` | Versioned (`CREATE OR ALTER VERSIONED SCHEMA`) | All 13 stored procedures + Streamlit app. Replaced entirely on each version upgrade. |
| `CONFIG` | Non-versioned (`CREATE SCHEMA IF NOT EXISTS`) | Runtime state: APP_STATE, STREAM_METADATA, ASSET_HIERARCHY, ANOMALY_RESULTS, ANOMALY_SHARE_BACK, INIT_LOCK, INIT_PROGRESS, EVENT_LOG. **Persists across upgrades.** |
| `DATA_VIEWS` | Non-versioned (`CREATE SCHEMA IF NOT EXISTS`) | PI_STREAMS_UNIFIED (unified view) and DYNAMIC_ANALYTICS (semantic view). Recreated by the init pipeline. |
| `PROXY_VIEWS` | App package share | Secure views over CLD Iceberg tables. Managed by the provider, shared into the app via the application package share. Not created by setup.sql. |

### Initialization Pipeline

The app runs a 4-step pipeline on first launch (triggered by the Streamlit UI):

1. **CORE.INITIALIZE_VIEWS()** -- Discovers proxy views in PROXY_VIEWS schema, stores count
2. **CORE.DISCOVER_METADATA()** -- Inspects each view, detects format (Delta Sharing / wide / classic PI), extracts asset/signal names, classifies domains via Cortex AI
3. **CORE.CREATE_UNIFIED_VIEW()** -- Builds `DATA_VIEWS.PI_STREAMS_UNIFIED` as a UNION ALL across all proxy views with a normalized schema
4. **CORE.GENERATE_SEMANTIC_VIEW()** -- Creates `DATA_VIEWS.DYNAMIC_ANALYTICS` semantic view for Cortex Analyst

To re-run: `CALL <app_name>.CORE.REINITIALIZE()`

---

## 17. Signal Name Cleaning

AVEVA PI Data Historian signal names often contain embedded UUIDs and unit annotations:

```
Active Power - 10 min rolling avg.4cda6eb5-885a-5fd4-323f-add6141de672 (kW)
Expected Power.f3a878d6-3edc-5585-14ea-008cd1cec068 (kW)
Revenue - Monthly.1d8f39d7-4532-5b81-0652-bb84f250bd9a ()
BL1_ACT (°)
```

During DISCOVER_METADATA, the app automatically strips:
- **UUID suffixes:** `.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` pattern
- **Unit annotations:** trailing `(kW)`, `(°C)`, `(%)`, `()`, etc.

Resulting clean names:
```
Active Power - 10 min rolling avg
Expected Power
Revenue - Monthly
BL1_ACT
```

These clean names are stored in `CONFIG.STREAM_METADATA.SIGNAL_NAME` and appear in the unified view and semantic view. The raw column names (with UUIDs) are preserved in `CONFIG.STREAM_METADATA.STREAM_NAME` for use as the actual column reference in wide-format views.
