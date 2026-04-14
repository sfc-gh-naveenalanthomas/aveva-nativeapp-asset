# AVEVA Connect -- Snowflake Native App

Share AVEVA PI Data Historian streams with customers as a Snowflake Native App.
Data flows from Databricks (Delta Sharing) into Snowflake via Catalog-Linked
Databases and Iceberg tables, and is delivered to consumer accounts as an
installable application with a built-in Streamlit analytics dashboard powered
by Cortex AI.

---

## What Is in This Repository

```
aveva_connect_native_app/
  manifest.yml                  -- Native App manifest (version, privileges, restricted features)
  setup.sql                     -- App setup script (runs when consumer installs/upgrades)
  README.md                     -- This file
  streamlit/
    main.py                     -- Streamlit analytics dashboard (consumer-facing)
    environment.yml             -- Python dependencies for the Streamlit app
  scripts/
    onboard_customer_e2e.sql    -- Main script: one-time setup + onboarding stored procedure
  docs/
    ONBOARD_CUSTOMER_HANDOVER.md -- Detailed technical reference for the onboarding procedure
```

---

## Architecture

### Data Flow

```
Databricks (Delta Sharing / Unity Catalog)
        |
        | Bearer Token + Iceberg REST
        v
+-------------------+
| Catalog           |
| Integration (IRC) |
+--------+----------+
         |
+--------v----------+         LISTING          +---------------------------+
| Catalog-Linked DB |  ----------------------> | Consumer Account          |
| (Iceberg tables,  |                          | Installs Native App       |
|  auto-sync 5 min) |                          +-------------+-------------+
+--------+----------+                                        |
         |                                      +------------v------------+
+--------v----------+                           |  Streamlit Dashboard    |
| PROXY_VIEWS       |  <--- SHARE (secure) ---> |  (CORE schema)          |
| (App Package,     |                           +------------+------------+
|  provider-managed)|                                        |
+-------------------+                           +------------v------------+
                                                |  Init Pipeline          |
                                                |  1. INITIALIZE_VIEWS    |
                                                |  2. DISCOVER_METADATA   |
                                                |  3. CREATE_UNIFIED_VIEW |
                                                |  4. GENERATE_SEMANTIC   |
                                                +------------+------------+
                                                             |
                                                +------------v------------+
                                                |  DATA_VIEWS schema      |
                                                |  - PI_STREAMS_UNIFIED   |
                                                |  - DYNAMIC_ANALYTICS    |
                                                |    (semantic view for   |
                                                |     Cortex Analyst)     |
                                                +-------------------------+
```

### Schema Layout (Inside the Installed App)

| Schema | Type | Contents |
|--------|------|----------|
| `CORE` | Versioned | All stored procedures (13 total) + Streamlit app. Replaced on each upgrade. |
| `CONFIG` | Non-versioned | Runtime state tables (APP_STATE, STREAM_METADATA, ASSET_HIERARCHY, ANOMALY_RESULTS, EVENT_LOG, etc.). Persists across upgrades. |
| `DATA_VIEWS` | Non-versioned | PI_STREAMS_UNIFIED (unified view across all proxy views) and DYNAMIC_ANALYTICS (semantic view for Cortex Analyst). Recreated by init pipeline. |
| `PROXY_VIEWS` | App Package | Secure views over CLD Iceberg tables. Provider-managed, shared into the app via the application package share. |

---

## Prerequisites

Before you begin, make sure you have the following:

1. **A Snowflake account** with the `ACCOUNTADMIN` role.
2. **A warehouse** (e.g., `COMPUTE_WH`). Any size works; XS is fine for setup.
3. **Snowflake CLI (`snow`)** installed on your machine.
   - Install: https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation
   - After installing, create a connection:
     ```bash
     snow connection add
     ```
     Enter your account identifier (e.g., `MYORG-MYACCOUNT`), username, and role (`ACCOUNTADMIN`).
4. **Delta Sharing credentials** from Databricks:
   - **Catalog URI** -- the Iceberg REST endpoint URL
     (e.g., `https://westus3.azuredatabricks.net/api/2.0/delta-sharing/metastores/<ID>/iceberg`)
   - **Bearer Token** -- a Databricks personal access token (PAT) or Delta Sharing recipient token
   - **Warehouse ID** -- the Databricks SQL warehouse ID or Delta Sharing share ID
5. **Consumer account details** -- the fully qualified Snowflake account of the customer
   who will install the app (e.g., `MYORG.CUSTOMER_ACCOUNT`).

---

## Quick Start (3 Steps)

### Step 1: Run the Setup Script

Open a Snowflake SQL worksheet (Snowsight) or SnowSQL and run the setup portion
of `scripts/onboard_customer_e2e.sql`. This creates:

- Database `AVEVA_CONNECT` with schema `ADMIN`
- Tables: `CUSTOMERS`, `ONBOARDING_CONFIG`, `ONBOARDING_LOG`
- Default configuration values

In a Snowflake worksheet, run everything from the top of the file down to
(and including) the `ONBOARDING_CONFIG` seed inserts (approximately lines 1-105).

Then run the `CREATE OR REPLACE PROCEDURE` block (lines 112-500) to create the
onboarding stored procedure.

### Step 2: Upload the App Files

From your terminal, navigate to this directory and upload the app source files
to the Snowflake stage. Replace `<connection>` with your Snowflake CLI connection name.

```bash
cd aveva_connect_native_app

snow stage copy manifest.yml    @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           --overwrite --connection <connection>
snow stage copy setup.sql       @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           --overwrite --connection <connection>
snow stage copy README.md       @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/           --overwrite --connection <connection>
snow stage copy streamlit/main.py         @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/streamlit/ --overwrite --connection <connection>
snow stage copy streamlit/environment.yml @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE/streamlit/ --overwrite --connection <connection>
```

Verify the files were uploaded:

```sql
LIST @AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE;
```

You should see: `manifest.yml`, `setup.sql`, `README.md`, `streamlit/main.py`,
`streamlit/environment.yml`.

> **Note:** Step 1 creates the app package and stage automatically. If you
> already have an app package with a different name, update the `APP_PKG_NAME`
> value in `AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG` before proceeding.

### Step 3: Onboard a Customer

Run a single `CALL` statement to provision everything for a customer:

```sql
CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
    'CUSTOMER_NAME',                              -- alphanumeric + underscores
    'MYORG.CUSTOMER_ACCOUNT',                     -- consumer's Snowflake account
    'https://westus3.azuredatabricks.net/api/2.0/delta-sharing/metastores/<ID>/iceberg',
    '<BEARER_TOKEN>',                             -- Databricks PAT
    '<WAREHOUSE_ID>'                              -- Databricks warehouse ID
);
```

That's it. The procedure handles all 7 steps automatically:

1. Creates the Application Package, Stage, and Version
2. Creates a Catalog Integration (Delta Sharing / Iceberg REST)
3. Creates a Catalog-Linked Database (auto-discovers tables)
4. Grants data sharing permissions to the app package
5. Creates secure proxy views for all discovered tables
6. Creates and publishes a Listing targeting the consumer account
7. Registers the customer in the tracking table

On success, the output tells you:

```
SUCCESS: All 7/7 steps completed. Pkg=AVEVA_CONNECT_APP_PKG, CLD=AVEVA_CUSTOMER_NAME_DB,
Listing=AVEVA_CONNECT_CUSTOMER_NAME_LISTING, Region=PUBLIC.AWS_US_WEST_2, Cloud=AWS
```

---

## Targeting a Specific Region

By default, the listing targets the provider account's own region. If the
consumer is in a different region (e.g., Azure West US 2), pass the region
as the 6th parameter:

```sql
CALL AVEVA_CONNECT.ADMIN.ONBOARD_CUSTOMER(
    'CUSTOMER_NAME',
    'MYORG.CUSTOMER_ACCOUNT',
    'https://...',
    '<TOKEN>',
    '<WAREHOUSE_ID>',
    'AZURE_WESTUS2'                    -- target region
);
```

The procedure accepts flexible input. All of these resolve to the same region:

| Input | Cloud Hint (7th param) | Result |
|-------|----------------------|--------|
| `'AZURE_WESTUS2'` | -- | `PUBLIC.AZURE_WESTUS2` |
| `'azure westus2'` | -- | `PUBLIC.AZURE_WESTUS2` |
| `'PUBLIC.AZURE_WESTUS2'` | -- | `PUBLIC.AZURE_WESTUS2` |
| `'westus2'` | `'AZURE'` | `PUBLIC.AZURE_WESTUS2` |

If the region is invalid, the procedure returns an error immediately without
creating any objects.

Auto-fulfillment is enabled on the app package (`LISTING_AUTO_REFRESH = ON`).
When the listing targets a region different from the provider's, Snowflake
automatically replicates the app to the consumer's region in the background.

---

## What the Consumer Does

Once the listing is published, the consumer:

1. **Installs the app** from the Snowflake Marketplace or Organization listing
   in Snowsight. Snowsight prompts them to associate a warehouse during install.

2. **Grants permissions.** On first launch, Snowsight automatically prompts the
   consumer for two permission grants (no SQL required):
   - **External Data Access** -- allows the app to resolve the shared Iceberg
     tables through the proxy views. This prompt appears because the manifest
     declares `restricted_features: external_data`.
   - **Cortex AI Access** -- grants `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`
     so the app can use Cortex AI functions (Complete, Analyst, Anomaly Detection).

3. **The app initializes.** The Streamlit dashboard runs the 4-step init pipeline:
   - Discovers proxy views from the PROXY_VIEWS schema
   - Inspects each view, extracts asset/signal metadata, classifies industrial domains via Cortex AI
   - Builds a unified view (`PI_STREAMS_UNIFIED`) normalizing all data formats
   - Generates a semantic view (`DYNAMIC_ANALYTICS`) for Cortex Analyst natural language queries

No manual SQL commands are required by the consumer when installing via Snowsight.

> **If installing via SQL** (e.g., for testing), the consumer must manually grant
> external data access:
> ```sql
> SELECT SYSTEM$SET_APPLICATION_RESTRICTED_FEATURE_ACCESS(
>     '<app_name>', 'EXTERNAL_DATA', '{"allowed_cloud_providers": "all"}'
> );
> ```
> And bind a warehouse:
> ```sql
> CALL <app_name>.CORE.REGISTER_REFERENCE(
>     'WAREHOUSE_REF', 'ADD',
>     SYSTEM$REFERENCE('WAREHOUSE', 'COMPUTE_WH', 'SESSION', 'USAGE')
> );
> ```

---

## Initialization Pipeline

The app runs a 4-step pipeline on first launch (and on re-initialization):

| Step | Procedure | What It Does |
|------|-----------|-------------|
| 1 | `CORE.INITIALIZE_VIEWS()` | Discovers proxy views in the PROXY_VIEWS schema. Stores view count in APP_STATE. |
| 2 | `CORE.DISCOVER_METADATA()` | Inspects each proxy view's columns to detect format (Delta Sharing long, wide-format, or classic PI). Extracts asset/signal names. Strips AVEVA UUID suffixes and unit annotations from signal names for clean display. Classifies domains (wind_energy, rotating_machinery, etc.) using Cortex AI with keyword fallback. Populates STREAM_METADATA and ASSET_HIERARCHY. |
| 3 | `CORE.CREATE_UNIFIED_VIEW()` | Reads STREAM_METADATA, builds a UNION ALL across all proxy views (normalizing different schemas into TS, NUMERIC_VALUE, STRING_VALUE, ASSET_NAME, SIGNAL_NAME, DOMAIN_CATEGORY + quality flags). Creates `DATA_VIEWS.PI_STREAMS_UNIFIED`. |
| 4 | `CORE.GENERATE_SEMANTIC_VIEW()` | Creates `DATA_VIEWS.DYNAMIC_ANALYTICS` semantic view on top of PI_STREAMS_UNIFIED. This enables Cortex Analyst natural language queries. |

To re-run the full pipeline (e.g., after new proxy views are added):

```sql
CALL <app_name>.CORE.REINITIALIZE();
```

---

## Supported Data Formats

The app auto-detects three data formats during discovery:

| Format | Detection | Example |
|--------|-----------|---------|
| **Delta Sharing (long)** | Columns: `Timestamp`, `Field`, `Value`, `Uom` | One view, many signals in `Field` column. E.g., `Rotating Machinery - PS0.Bearing Temperature Value` |
| **Wide format** | `Timestamp` + many signal columns, no `Value`/`Field` | Each column is a separate signal. E.g., `GE01.Active Power - 10 min rolling avg...` |
| **Classic PI** | `Timestamp`, `Value`, quality flags (`IsQuestionable`, etc.) | One view per signal |

Signal names from AVEVA often include embedded UUIDs and unit annotations
(e.g., `Active Power - 10 min rolling avg.4cda6eb5-885a-5fd4-323f-add6141de672 (kW)`).
The app automatically strips these during discovery, producing clean names like
`Active Power - 10 min rolling avg` for display and Cortex Analyst queries.

---

## Customizing Defaults

Before calling the onboarding procedure, you can change defaults in the
configuration table:

```sql
-- View current configuration
SELECT * FROM AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG ORDER BY CONFIG_KEY;

-- Change the app package name
UPDATE AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG
SET CONFIG_VALUE = 'MY_CUSTOM_PKG_NAME' WHERE CONFIG_KEY = 'APP_PKG_NAME';

-- Change the distribution mode (ORGANIZATION = same org, EXTERNAL = cross-org)
UPDATE AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG
SET CONFIG_VALUE = 'EXTERNAL' WHERE CONFIG_KEY = 'DISTRIBUTION_MODE';

-- Change the listing description
UPDATE AVEVA_CONNECT.ADMIN.ONBOARDING_CONFIG
SET CONFIG_VALUE = 'My custom description' WHERE CONFIG_KEY = 'LISTING_DESCRIPTION';
```

| Config Key | Default | Description |
|-----------|---------|-------------|
| `APP_PKG_NAME` | `AVEVA_CONNECT_APP_PKG` | Application Package name |
| `APP_VERSION` | `V1_0` | Version label |
| `APP_SOURCE_STAGE` | `AVEVA_CONNECT_APP_PKG.APP_SRC.STAGE` | Stage with app files |
| `WAREHOUSE_NAME` | `COMPUTE_WH` | Snowflake warehouse |
| `DISTRIBUTION_MODE` | `ORGANIZATION` | `ORGANIZATION` or `EXTERNAL` |
| `LISTING_SUBTITLE` | `Industrial PI Data Analytics` | Listing subtitle |
| `LISTING_DESCRIPTION` | `Cross-account PI data historian analytics powered by Cortex AI` | Listing description |

---

## Checking Results

After onboarding, check the log:

```sql
SELECT * FROM AVEVA_CONNECT.ADMIN.ONBOARDING_LOG
WHERE CUSTOMER_NAME = 'CUSTOMER_NAME'
ORDER BY STEP_NUMBER;
```

Check registered customers:

```sql
SELECT * FROM AVEVA_CONNECT.ADMIN.CUSTOMERS;
```

---

## Re-Running Is Safe

The procedure is fully idempotent. Every step uses `IF NOT EXISTS`, `OR REPLACE`,
or `NOT EXISTS` guards. You can safely re-run the same `CALL` to:

- Pick up new tables that appeared in the Delta Sharing catalog
- Recreate proxy views after schema changes
- Recover from partial failures

---

## Removing a Customer

To fully remove a customer's onboarded resources:

```sql
DROP LISTING IF EXISTS AVEVA_CONNECT_CUSTOMER_NAME_LISTING;
DROP DATABASE IF EXISTS AVEVA_CUSTOMER_NAME_DB;
DROP CATALOG INTEGRATION IF EXISTS AVEVA_CUSTOMER_NAME_CATALOG_INT;
DELETE FROM AVEVA_CONNECT.ADMIN.CUSTOMERS WHERE CUSTOMER_NAME = 'CUSTOMER_NAME';
DELETE FROM AVEVA_CONNECT.ADMIN.ONBOARDING_LOG WHERE CUSTOMER_NAME = 'CUSTOMER_NAME';
```

---

## App Features

The installed Streamlit app provides:

- **Dashboard** -- overview metrics (stream count, asset count, domain distribution)
- **Asset Hierarchy Explorer** -- browse PI assets organized by domain
- **Talk to Your Data** -- ask questions in plain English via Cortex Analyst and the semantic view
- **Stream Browser** -- browse individual streams, preview raw data from proxy views
- **Data Statistics** -- time range, value distributions, trend charts per signal
- **Anomaly Detection** -- rolling z-score anomaly detection on numeric signals with auto-detection mode
- **Time Series Forecasting** -- double exponential smoothing forecasts on any numeric signal
- **Privacy & Telemetry** -- all user queries are redacted via `AI_REDACT` before logging to EVENT_LOG

---

## Stored Procedures Reference

All procedures are in the `CORE` schema and return JSON: `{"status": "SUCCESS|ERROR", "message": "...", "details": {...}}`

| Procedure | Purpose |
|-----------|---------|
| `REGISTER_REFERENCE(ref, op, alias)` | Callback for manifest warehouse reference binding |
| `INITIALIZE_VIEWS()` | Step 1: Discover proxy views |
| `DISCOVER_METADATA()` | Step 2: Extract and classify asset/signal metadata |
| `CREATE_UNIFIED_VIEW()` | Step 3: Build PI_STREAMS_UNIFIED |
| `GENERATE_SEMANTIC_VIEW()` | Step 4: Create DYNAMIC_ANALYTICS semantic view |
| `IS_INITIALIZED()` | Check if init pipeline has completed |
| `GET_SCHEMA_INFO()` | Return schema description for Cortex Analyst context |
| `GET_INIT_STATUS()` | Return init progress for Streamlit UI |
| `LOG_EVENT(type, data, query)` | Log a redacted event to EVENT_LOG |
| `HEALTH_CHECK()` | Validate all app components are healthy |
| `REINITIALIZE()` | Full reset: clear state, drop views, re-run all 4 init steps |
| `DETECT_ANOMALIES(asset, signal, lookback)` | Run anomaly detection on a signal |
| `FORECAST_SIGNAL(asset, signal, horizon)` | Generate a time series forecast |
| `SHARE_ANOMALIES_BACK()` | Copy anomaly results to share-back table for provider |

---

## Troubleshooting

**Problem: "Invalid region" error**
The region you passed doesn't match any Snowflake region. Run `SHOW REGIONS` in
a worksheet to see all valid regions.

**Problem: 0 proxy views created**
The Catalog-Linked Database may not have finished its first sync. Wait 5 minutes
and re-run the procedure. New tables will be discovered.

**Problem: Consumer can't see the listing**
Check that the consumer account identifier is correct (format: `ORG_NAME.ACCOUNT_NAME`).
For `ORGANIZATION` distribution, both accounts must be in the same Snowflake organization.

**Problem: Consumer app shows "External Data Access Required"**
This is normal on first launch. The consumer clicks the "Grant External Data Access"
button in Snowsight to approve. If installing via SQL, run:
```sql
SELECT SYSTEM$SET_APPLICATION_RESTRICTED_FEATURE_ACCESS(
    '<app_name>', 'EXTERNAL_DATA', '{"allowed_cloud_providers": "all"}'
);
```

**Problem: "Insufficient permission to resolve external/iceberg table"**
The consumer has not yet granted external data access. See above.

**Problem: Cortex AI queries fail**
The consumer needs to grant Cortex AI access. If they missed the in-app prompt, they
can run manually:
```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION <app_name>;
```

**Problem: Cortex Analyst returns 0 rows**
Signal names may not match. The app strips AVEVA UUID suffixes and unit annotations
during discovery. Check the actual signal names:
```sql
SELECT DISTINCT SIGNAL_NAME FROM <app_name>.CONFIG.STREAM_METADATA ORDER BY SIGNAL_NAME;
```

**Problem: Initialization is slow**
Domain classification uses Cortex AI (mistral-large2). This can take 1-3 minutes
for 200+ signals. Subsequent re-initializations are the same speed since metadata
is regenerated from scratch.

---

## File Reference

| File | Purpose |
|------|---------|
| `manifest.yml` | Declares app version, privileges (`IMPORTED PRIVILEGES ON SNOWFLAKE DB`), warehouse reference, and `restricted_features: external_data` for Iceberg access. |
| `setup.sql` | Runs inside the consumer account on install/upgrade. Creates schemas (CORE, CONFIG, DATA_VIEWS), tables, all 13 stored procedures, and the Streamlit app object. |
| `streamlit/main.py` | Consumer-facing Streamlit dashboard. Handles permission gates (external data + Cortex AI), initialization flow, and all analytics pages. |
| `streamlit/environment.yml` | Python dependencies (`snowflake-snowpark-python`, `snowflake-native-apps-permission`). |
| `scripts/onboard_customer_e2e.sql` | Provider-side script. Contains setup DDL, config seeds, and the `ONBOARD_CUSTOMER` stored procedure. |
| `docs/ONBOARD_CUSTOMER_HANDOVER.md` | Detailed technical reference for the onboarding procedure -- parameters, pipeline steps, naming conventions. |
