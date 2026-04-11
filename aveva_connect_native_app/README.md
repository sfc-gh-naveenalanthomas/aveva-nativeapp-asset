# AVEVA Connect -- Snowflake Native App

Share AVEVA PI Data Historian streams with customers as a Snowflake Native App.
Data flows from Databricks (Delta Sharing) into Snowflake and is delivered to
consumer accounts as an installable application with a built-in Streamlit
analytics dashboard powered by Cortex AI.

---

## What Is in This Repository

```
aveva_connect_native_app/
  manifest.yml                  -- Native App manifest (version, privileges, restricted features)
  setup.sql                     -- App setup script (runs when consumer installs/upgrades)
  README.md                     -- This file
  streamlit/
    main.py                     -- Streamlit analytics dashboard
    environment.yml             -- Python dependencies for the Streamlit app
  scripts/
    onboard_customer_e2e.sql    -- Main script: one-time setup + onboarding stored procedure
  docs/
    ONBOARD_CUSTOMER_HANDOVER.md -- Detailed technical reference for the onboarding procedure
```

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

2. **Opens the app.** The Streamlit dashboard guides them through two permission
   grants via on-screen buttons (no SQL required):
   - **Grant External Data Access** -- allows the app to read the shared data
   - **Grant Cortex AI Access** -- allows the app to use Snowflake Cortex AI functions

3. The app initializes automatically: discovers tables, classifies data domains,
   creates a unified view, and generates a semantic view for natural language queries.

No manual SQL commands are required by the consumer when installing via Snowsight.

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

- **Asset Hierarchy Explorer** -- browse PI assets organized by domain
- **Domain-Specific Dashboards** -- wind energy, rotating machinery, production quality, packaging
- **Natural Language Queries** -- ask questions in plain English via Cortex Analyst
- **Anomaly Detection** -- rolling z-score anomaly detection on numeric signals
- **Time Series Forecasting** -- predict future values using historical data
- **Data Quality Analysis** -- PI quality flags (questionable, substituted, annotated)
- **Auto-Generated Data Dictionary** -- AI-classified metadata for all streams
- **Privacy-Protected Telemetry** -- all user queries are redacted via `AI_REDACT` before logging

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
button to approve.

**Problem: Cortex AI queries fail**
The consumer needs to grant Cortex AI access. If they missed the in-app prompt, they
can run manually:
```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION <app_name>;
```

---

## File Reference

| File | Purpose |
|------|---------|
| `manifest.yml` | Declares app version, privileges, restricted features. Uses `manifest_version: 2` for auto-granting task execution privileges. |
| `setup.sql` | Runs inside the consumer account on install/upgrade. Creates schemas, tables, stored procedures (view discovery, metadata classification, unified view, semantic view, anomaly detection, forecasting). |
| `streamlit/main.py` | The consumer-facing Streamlit dashboard. Handles permission gates, initialization, and all analytics UI. |
| `streamlit/environment.yml` | Python dependencies (`snowflake-snowpark-python`, `snowflake-native-apps-permission`). |
| `scripts/onboard_customer_e2e.sql` | The main provider-side script. Contains setup DDL, config seeds, and the `ONBOARD_CUSTOMER` stored procedure. |
| `docs/ONBOARD_CUSTOMER_HANDOVER.md` | Detailed technical reference for the stored procedure -- parameters, pipeline steps, naming conventions, architecture diagram. |

---

## Architecture

```
Databricks (Delta Sharing)
        |
        | Bearer Token
        v
+-------------------+          +---------------------+
| Catalog           |  LISTING | Consumer Account    |
| Integration (IRC) |--------->| Installs Native App |
+--------+----------+          +----------+----------+
         |                                |
+--------v----------+          +----------v----------+
| Catalog-Linked DB |          | Streamlit Dashboard |
| (auto-sync 5 min) |          | - Cortex AI Q&A     |
+--------+----------+          | - Anomaly Detection |
         |                     | - Forecasting       |
+--------v----------+          | - Asset Explorer    |
| Proxy Views       |<-SHARE-->| - Domain Dashboards |
| (in App Package)  |          +---------------------+
+-------------------+
```
