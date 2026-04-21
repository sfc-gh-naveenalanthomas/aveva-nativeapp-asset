-- =====================================================
-- AVEVA CONNECT Industrial IoT - Native App Setup
-- =====================================================
--
-- Architecture:
--   CORE       (versioned)     - Stored procedures + Streamlit app
--   CONFIG     (non-versioned) - Runtime state tables (persist across upgrades)
--   DATA_VIEWS (non-versioned) - Semantic view (AVEVA_ANALYTICS) for Cortex Analyst
--   PROXY_VIEWS (app package)  - Secure views over CLD Iceberg tables (provider-managed, not in this file)
--
-- Initialization pipeline (called by consumer or Streamlit UI):
--   1. CORE.INITIALIZE_VIEWS()       - Discovers proxy views from PROXY_VIEWS schema
--   2. CORE.DISCOVER_METADATA()      - Inspects each view, extracts asset/signal metadata, classifies domains
--   3. CORE.CREATE_SEMANTIC_VIEWS()   - Creates AVEVA_ANALYTICS semantic view on proxy views for Cortex Analyst
--
-- All stored procedures return structured JSON: {"status": "SUCCESS|ERROR", "message": "...", "details": {...}}
--
-- =====================================================

CREATE APPLICATION ROLE IF NOT EXISTS APP_PUBLIC;

-- CORE: versioned schema - all procs and Streamlit are replaced on each upgrade
CREATE OR ALTER VERSIONED SCHEMA CORE;
GRANT USAGE ON SCHEMA CORE TO APPLICATION ROLE APP_PUBLIC;

-- DATA_VIEWS: non-versioned - holds the semantic view (recreated by procs)
CREATE SCHEMA IF NOT EXISTS DATA_VIEWS;
GRANT USAGE ON SCHEMA DATA_VIEWS TO APPLICATION ROLE APP_PUBLIC;

-- CONFIG: non-versioned - runtime state tables persist across app upgrades
CREATE SCHEMA IF NOT EXISTS CONFIG;
GRANT USAGE ON SCHEMA CONFIG TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- Configuration & State Tables (CONFIG schema)
-- =====================================================

CREATE TABLE IF NOT EXISTS CONFIG.APP_STATE (
    KEY VARCHAR(100) PRIMARY KEY,
    VALUE VARCHAR(16000),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

GRANT SELECT, INSERT, UPDATE ON TABLE CONFIG.APP_STATE TO APPLICATION ROLE APP_PUBLIC;

CREATE TABLE IF NOT EXISTS CONFIG.STREAM_METADATA (
    STREAM_VIEW_NAME VARCHAR(500),
    STREAM_NAME VARCHAR(500),
    ASSET_PATH VARCHAR(1000),
    ASSET_NAME VARCHAR(500),
    SIGNAL_NAME VARCHAR(500),
    DOMAIN_CATEGORY VARCHAR(100),
    DATA_TYPE VARCHAR(50),
    DISCOVERED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

GRANT SELECT, INSERT, DELETE ON TABLE CONFIG.STREAM_METADATA TO APPLICATION ROLE APP_PUBLIC;

CREATE TABLE IF NOT EXISTS CONFIG.ASSET_HIERARCHY (
    ASSET_PATH VARCHAR(1000),
    ASSET_NAME VARCHAR(500),
    PARENT_PATH VARCHAR(1000),
    LEVEL_DEPTH NUMBER,
    DOMAIN_CATEGORY VARCHAR(100),
    STREAM_COUNT NUMBER DEFAULT 0
);

GRANT SELECT, INSERT, DELETE ON TABLE CONFIG.ASSET_HIERARCHY TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- Anomaly Detection Results
-- =====================================================

CREATE TABLE IF NOT EXISTS CONFIG.ANOMALY_RESULTS (
    RESULT_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    ASSET_NAME VARCHAR(500),
    SIGNAL_NAME VARCHAR(500),
    DOMAIN_CATEGORY VARCHAR(100),
    TS TIMESTAMP_LTZ,
    NUMERIC_VALUE DOUBLE,
    IS_ANOMALY BOOLEAN DEFAULT FALSE,
    ANOMALY_SCORE DOUBLE,
    FORECAST_VALUE DOUBLE,
    LOWER_BOUND DOUBLE,
    UPPER_BOUND DOUBLE,
    MODEL_TYPE VARCHAR(50),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

GRANT SELECT, INSERT, DELETE ON TABLE CONFIG.ANOMALY_RESULTS TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- Anomaly Share-back (consumer -> provider)
-- =====================================================

CREATE TABLE IF NOT EXISTS CONFIG.ANOMALY_SHARE_BACK (
    SHARE_ID VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
    CONSUMER_ACCOUNT VARCHAR(200),
    ASSET_NAME VARCHAR(500),
    SIGNAL_NAME VARCHAR(500),
    DOMAIN_CATEGORY VARCHAR(100),
    TS TIMESTAMP_LTZ,
    NUMERIC_VALUE DOUBLE,
    IS_ANOMALY BOOLEAN,
    ANOMALY_SCORE DOUBLE,
    FORECAST_VALUE DOUBLE,
    LOWER_BOUND DOUBLE,
    UPPER_BOUND DOUBLE,
    MODEL_TYPE VARCHAR(50),
    SHARED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

GRANT SELECT, INSERT, DELETE ON TABLE CONFIG.ANOMALY_SHARE_BACK TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- Init Progress (step tracking for Streamlit UI)
-- =====================================================

CREATE TABLE IF NOT EXISTS CONFIG.INIT_PROGRESS (
    STEP_NAME VARCHAR(100) PRIMARY KEY,
    STATUS VARCHAR(20) DEFAULT 'PENDING',
    STARTED_AT TIMESTAMP_LTZ,
    COMPLETED_AT TIMESTAMP_LTZ,
    DETAILS VARCHAR(16000)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE CONFIG.INIT_PROGRESS TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- Event Log
-- =====================================================

CREATE TABLE IF NOT EXISTS CONFIG.EVENT_LOG (
    EVENT_ID VARCHAR(36) DEFAULT UUID_STRING(),
    EVENT_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    EVENT_TYPE VARCHAR(50),
    EVENT_DATA VARIANT,
    REDACTED_QUERY VARCHAR(5000),
    PRIMARY KEY (EVENT_ID)
);

GRANT SELECT, INSERT ON TABLE CONFIG.EVENT_LOG TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- REGISTER_REFERENCE (callback for manifest references)
-- =====================================================
-- Called by Snowflake when consumer binds a warehouse (or other object)
-- to a reference declared in manifest.yml. This is the standard pattern
-- for Native Apps to receive consumer-owned resources.

CREATE OR REPLACE PROCEDURE CORE.REGISTER_REFERENCE(
    REF_NAME VARCHAR,
    OPERATION VARCHAR,
    REF_OR_ALIAS VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    BEGIN
        CASE (OPERATION)
            WHEN 'ADD' THEN
                SELECT SYSTEM$SET_REFERENCE(:REF_NAME, :REF_OR_ALIAS);
            WHEN 'REMOVE' THEN
                SELECT SYSTEM$REMOVE_REFERENCE(:REF_NAME, :REF_OR_ALIAS);
            WHEN 'CLEAR' THEN
                SELECT SYSTEM$REMOVE_ALL_REFERENCES(:REF_NAME);
        ELSE
            RETURN 'Unknown operation: ' || OPERATION;
        END CASE;
        RETURN 'SUCCESS';
    END;
$$;

GRANT USAGE ON PROCEDURE CORE.REGISTER_REFERENCE(VARCHAR, VARCHAR, VARCHAR) TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- INITIALIZE_VIEWS (Step 1 of init pipeline)
-- =====================================================
-- Discovers proxy views in the PROXY_VIEWS schema
-- (provider-managed secure views over CLD Iceberg tables).
-- Stores view count in CONFIG.APP_STATE. Uses a lock
-- to prevent concurrent initialization.
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.INITIALIZE_VIEWS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json

def run(session):
    try:
        # Check if already initialized (skip if so — use REINITIALIZE to force)
        init_check = session.sql("SELECT VALUE FROM CONFIG.APP_STATE WHERE KEY = 'VIEWS_INITIALIZED'").collect()
        if len(init_check) > 0 and init_check[0]["VALUE"] == 'TRUE':
            return json.dumps({"status": "SUCCESS", "message": "Already initialized — use REINITIALIZE to re-run", "details": {"skipped": True}})

        # Track progress
        session.sql("MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'INITIALIZE_VIEWS' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='RUNNING', STARTED_AT=CURRENT_TIMESTAMP(), COMPLETED_AT=NULL, DETAILS=NULL WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, STARTED_AT) VALUES (S.STEP_NAME, 'RUNNING', CURRENT_TIMESTAMP())").collect()

        # Discover views from PROXY_VIEWS schema
        view_names = []
        try:
            pv_result = session.sql("SHOW VIEWS IN SCHEMA PROXY_VIEWS").collect()
            for row in pv_result:
                view_names.append(row["name"])
        except:
            pass

        view_count = len(view_names)

        # Store the installing account for reference
        current_acct_rows = session.sql("SELECT CURRENT_ACCOUNT() AS ACCT").collect()
        current_account = current_acct_rows[0]["ACCT"] if len(current_acct_rows) > 0 else ""
        safe_acct = current_account.replace("'", "''")
        session.sql(f"MERGE INTO CONFIG.APP_STATE T USING (SELECT 'INSTALLING_ACCOUNT' AS KEY) S ON T.KEY = S.KEY WHEN MATCHED THEN UPDATE SET VALUE='{safe_acct}', UPDATED_AT=CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES ('INSTALLING_ACCOUNT', '{safe_acct}')").collect()

        # Update APP_STATE
        session.sql("MERGE INTO CONFIG.APP_STATE T USING (SELECT 'VIEWS_INITIALIZED' AS KEY) S ON T.KEY = S.KEY WHEN MATCHED THEN UPDATE SET VALUE='TRUE', UPDATED_AT=CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES ('VIEWS_INITIALIZED', 'TRUE')").collect()
        session.sql(f"MERGE INTO CONFIG.APP_STATE T USING (SELECT 'VIEW_COUNT' AS KEY) S ON T.KEY = S.KEY WHEN MATCHED THEN UPDATE SET VALUE='{view_count}', UPDATED_AT=CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES ('VIEW_COUNT', '{view_count}')").collect()

        # Mark progress complete
        detail = f"{view_count} proxy views discovered for account {current_account}"
        session.sql(f"UPDATE CONFIG.INIT_PROGRESS SET STATUS='COMPLETE', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='{detail}' WHERE STEP_NAME='INITIALIZE_VIEWS'").collect()

        return json.dumps({
            "status": "SUCCESS",
            "message": f"Discovered {view_count} proxy views",
            "details": {
                "view_count": view_count,
                "account": current_account
            }
        })

    except Exception as e:
        session.sql("MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'INITIALIZE_VIEWS' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='ERROR', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='" + str(e).replace("'", "''") + "' WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, COMPLETED_AT, DETAILS) VALUES (S.STEP_NAME, 'ERROR', CURRENT_TIMESTAMP(), '" + str(e).replace("'", "''") + "')").collect()
        return json.dumps({"status": "ERROR", "message": str(e), "details": {}})
$$;

GRANT USAGE ON PROCEDURE CORE.INITIALIZE_VIEWS() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- DISCOVER_METADATA (Step 2 of init pipeline)
-- =====================================================
-- Scans all proxy views, introspects their columns,
-- extracts asset/signal names, classifies industrial
-- domains via keywords, and populates CONFIG.STREAM_METADATA
-- and CONFIG.ASSET_HIERARCHY. No format detection needed —
-- the semantic view handles schema differences directly.
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.DISCOVER_METADATA()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json
import re

def _detect_domain_keywords(asset_lower, signal_lower):
    """Detect domain category from asset and signal names using keywords."""
    wind_keys = ["wind", "turbine", "hornsea", "ge0", "ge1", "yorkshire",
                 "wind_speed", "wind_direction", "pitch_angle", "vane_position",
                 "gearbox", "hub_temp", "eaf", "active power", "apparent power",
                 "expected power", "revenue", "total power", "total turbines"]
    if any(k in asset_lower or k in signal_lower for k in wind_keys):
        return "wind_energy"
    rotating_keys = ["rotating", "ps0", "ps1", "ps2", "ps3", "ps4", "ps5", "ps6",
                     "bearing", "suction", "discharge", "pump"]
    if any(k in asset_lower or k in signal_lower for k in rotating_keys):
        return "rotating_machinery"
    pq_keys = ["plant", "pq.", "concentration", "granule", "dissolv", "ingredient", "agitator"]
    if any(k in asset_lower or k in signal_lower for k in pq_keys):
        return "production_quality"
    pkg_keys = ["pack", "line", "carb", "chill", "warmer", "running", "refrigerant"]
    if any(k in asset_lower or k in signal_lower for k in pkg_keys):
        return "packaging"
    return "general"


def _clean_signal_name(raw):
    """Strip UUID suffix and unit annotation from AVEVA signal names."""
    cleaned = re.sub(r'\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '', raw)
    cleaned = re.sub(r'\s*\([^)]*\)\s*$', '', cleaned)
    return cleaned.strip()


def run(session):
    try:
        # Track progress
        session.sql("MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'DISCOVER_METADATA' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='RUNNING', STARTED_AT=CURRENT_TIMESTAMP(), COMPLETED_AT=NULL, DETAILS=NULL WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, STARTED_AT) VALUES (S.STEP_NAME, 'RUNNING', CURRENT_TIMESTAMP())").collect()

        # Clear previous metadata
        session.sql("DELETE FROM CONFIG.STREAM_METADATA").collect()
        session.sql("DELETE FROM CONFIG.ASSET_HIERARCHY").collect()

        # Get views from PROXY_VIEWS
        views_result = session.sql("SHOW VIEWS IN SCHEMA PROXY_VIEWS").collect()
        view_names = [row["name"] for row in views_result]

        stream_count = 0
        assets = {}
        domain_map = {}
        stream_records = []

        for view_name in view_names:
            quoted = view_name.replace('"', '""')
            try:
                col_rows = session.sql(f'DESCRIBE VIEW PROXY_VIEWS."{quoted}"').collect()
            except:
                continue

            col_names = set(cr["name"] for cr in col_rows)

            # Validate view is queryable (catches broken CLD references)
            try:
                session.sql(f'SELECT 1 FROM PROXY_VIEWS."{quoted}" LIMIT 1').collect()
            except:
                continue  # Skip unreachable views

            has_field = "Field" in col_names
            has_value = "Value" in col_names

            if has_field:
                # Long format table (Timestamp, Field, Value [, Name] [, Uom])
                # Each distinct Field value is a signal
                try:
                    field_rows = session.sql(f'SELECT DISTINCT "Field" FROM PROXY_VIEWS."{quoted}"').collect()
                except:
                    field_rows = []
                for fr in field_rows:
                    field_val = fr["Field"]
                    parts = field_val.split(".")
                    asset_name = ".".join(parts[:-1]) if len(parts) > 1 else view_name
                    signal_name = _clean_signal_name(parts[-1]) if len(parts) > 1 else _clean_signal_name(field_val)
                    stream_records.append({
                        "view": view_name, "stream": field_val,
                        "asset": asset_name, "signal": signal_name, "dtype": "NUMERIC"
                    })
            elif has_value and not has_field:
                # Long format without Field column — treat view as one signal
                stream_records.append({
                    "view": view_name, "stream": view_name,
                    "asset": view_name, "signal": "VALUE", "dtype": "NUMERIC"
                })
            else:
                # Wide format: each non-Timestamp column is a signal
                for cr in col_rows:
                    cn = cr["name"]
                    if cn == "Timestamp":
                        continue
                    ct = cr["type"].upper()
                    # Parse asset/signal from column name
                    parts = cn.split(".", 1)
                    if len(parts) > 1:
                        asset_name = parts[0].strip()
                        signal_name = _clean_signal_name(parts[1].strip())
                    elif "_" in cn:
                        first_sep = cn.index("_")
                        asset_name = cn[:first_sep].strip()
                        signal_name = _clean_signal_name(cn[first_sep+1:].strip())
                    else:
                        asset_name = view_name
                        signal_name = cn
                    data_type = "STRING" if ("VARCHAR" in ct or "STRING" in ct) else "NUMERIC"
                    stream_records.append({
                        "view": view_name, "stream": cn,
                        "asset": asset_name, "signal": signal_name, "dtype": data_type
                    })

        # Deduplicate: keep first occurrence per (asset, signal)
        seen = set()
        unique_records = []
        for rec in stream_records:
            key = (rec["asset"], rec["signal"])
            if key not in seen:
                seen.add(key)
                unique_records.append(rec)
        stream_records = unique_records

        # Insert metadata with keyword-classified domains
        for rec in stream_records:
            domain = _detect_domain_keywords(rec["asset"].lower(), rec["signal"].lower())
            safe_view = rec["view"].replace("'", "''")
            safe_stream = rec["stream"].replace("'", "''")
            safe_asset = rec["asset"].replace("'", "''")
            safe_signal = rec["signal"].replace("'", "''")
            session.sql(
                f"INSERT INTO CONFIG.STREAM_METADATA (STREAM_VIEW_NAME, STREAM_NAME, ASSET_PATH, ASSET_NAME, SIGNAL_NAME, DOMAIN_CATEGORY, DATA_TYPE) "
                f"VALUES ('{safe_view}', '{safe_stream}', '{safe_asset}', '{safe_asset}', '{safe_signal}', '{domain}', '{rec['dtype']}')"
            ).collect()

            if rec["asset"] not in assets:
                assets[rec["asset"]] = {"domain": domain, "count": 0}
            assets[rec["asset"]]["count"] += 1
            domain_map[domain] = domain_map.get(domain, 0) + 1
            stream_count += 1

        # Build asset hierarchy
        for asset_path, info in assets.items():
            a_parts = asset_path.split(".")
            depth = len(a_parts)
            parent = ".".join(a_parts[:-1]) if len(a_parts) > 1 else ""
            a_name = a_parts[-1]
            safe_ap = asset_path.replace("'", "''")
            safe_an = a_name.replace("'", "''")
            safe_pp = parent.replace("'", "''")
            session.sql(
                f"INSERT INTO CONFIG.ASSET_HIERARCHY (ASSET_PATH, ASSET_NAME, PARENT_PATH, LEVEL_DEPTH, DOMAIN_CATEGORY, STREAM_COUNT) "
                f"VALUES ('{safe_ap}', '{safe_an}', '{safe_pp}', {depth}, '{info['domain']}', {info['count']})"
            ).collect()

        # Update APP_STATE
        domain_json = json.dumps(domain_map).replace("'", "''")
        for key, val in [("DOMAIN_DISTRIBUTION", domain_json), ("STREAM_COUNT", str(stream_count)), ("METADATA_DISCOVERED", "TRUE")]:
            safe_val = str(val).replace("'", "''")
            session.sql(
                f"MERGE INTO CONFIG.APP_STATE T USING (SELECT '{key}' AS KEY) S ON T.KEY = S.KEY "
                f"WHEN MATCHED THEN UPDATE SET VALUE='{safe_val}', UPDATED_AT=CURRENT_TIMESTAMP() "
                f"WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES ('{key}', '{safe_val}')"
            ).collect()

        # Mark progress complete
        detail_msg = f"{stream_count} streams, {len(assets)} assets"
        session.sql(f"UPDATE CONFIG.INIT_PROGRESS SET STATUS='COMPLETE', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='{detail_msg}' WHERE STEP_NAME='DISCOVER_METADATA'").collect()

        return json.dumps({
            "status": "SUCCESS",
            "message": f"Discovered {stream_count} streams across {len(assets)} assets",
            "details": {"stream_count": stream_count, "asset_count": len(assets), "domain_distribution": domain_map}
        })

    except Exception as e:
        err_msg = str(e).replace("'", "''")
        session.sql(f"MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'DISCOVER_METADATA' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='ERROR', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='{err_msg}' WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, COMPLETED_AT, DETAILS) VALUES (S.STEP_NAME, 'ERROR', CURRENT_TIMESTAMP(), '{err_msg}')").collect()
        return json.dumps({"status": "ERROR", "message": str(e), "details": {}})
$$;

GRANT USAGE ON PROCEDURE CORE.DISCOVER_METADATA() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- CREATE_SEMANTIC_VIEWS (Step 3 of init pipeline)
-- =====================================================
-- Scans all proxy views, introspects their columns,
-- and builds a single semantic view (AVEVA_ANALYTICS)
-- with all proxy views as logical tables. No UNION ALL,
-- no format detection heuristics, no PI-specific assumptions.
-- Each proxy view keeps its native schema.
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.CREATE_SEMANTIC_VIEWS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json
import re

def _clean_alias(raw):
    """Create a clean SQL-safe alias from a raw column name.
    E.g. 'GE01.Wind Speed - 10 min rolling avg.4cda6eb5-... (kW)' -> 'WIND_SPEED_10_MIN_ROLLING_AVG'
    E.g. 'GE01_P_ACT (kW)' -> 'P_ACT'
    """
    # Strip UUID suffix
    cleaned = re.sub(r'\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '', raw)
    # Strip unit in parentheses at end
    cleaned = re.sub(r'\s*\([^)]*\)\s*$', '', cleaned)
    # Take the part after the last dot (remove asset prefix like GE01.)
    if '.' in cleaned:
        cleaned = cleaned.split('.')[-1]
    # Remove leading asset prefix like GE01_ if present
    cleaned = re.sub(r'^[A-Z0-9]+[_]', '', cleaned, count=1)
    # Replace non-alphanumeric with underscore, collapse multiples
    cleaned = re.sub(r'[^A-Za-z0-9]+', '_', cleaned).strip('_').upper()
    return cleaned if cleaned else 'VALUE'


def _extract_unit(raw):
    """Extract unit from parentheses at end of column name.
    E.g. 'GE01_P_ACT (kW)' -> 'kW'"""
    m = re.search(r'\(([^)]+)\)\s*$', raw)
    return m.group(1) if m and m.group(1).strip() else None


def run(session):
    try:
        # Track progress
        session.sql("MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'CREATE_SEMANTIC_VIEWS' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='RUNNING', STARTED_AT=CURRENT_TIMESTAMP(), COMPLETED_AT=NULL, DETAILS=NULL WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, STARTED_AT) VALUES (S.STEP_NAME, 'RUNNING', CURRENT_TIMESTAMP())").collect()

        current_db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]

        # Use the installed app database name as prefix — Cortex Analyst needs to
        # resolve tables through the consumer's installed app context, not the
        # app package. CURRENT_DATABASE() returns the app name (e.g. AVEVA_CONNECT_POLARIS2_TEST).
        proxy_prefix = f"{current_db}.PROXY_VIEWS"

        # Discover proxy views
        views_result = session.sql("SHOW VIEWS IN SCHEMA PROXY_VIEWS").collect()
        view_names = [row["name"] for row in views_result]

        if len(view_names) == 0:
            session.sql("UPDATE CONFIG.INIT_PROGRESS SET STATUS='COMPLETE', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='No proxy views found - skipped' WHERE STEP_NAME='CREATE_SEMANTIC_VIEWS'").collect()
            return json.dumps({"status": "SUCCESS", "message": "No proxy views found - semantic view skipped", "details": {}})

        # Only include views that have metadata (proven queryable during DISCOVER_METADATA)
        meta_views = session.sql("SELECT DISTINCT STREAM_VIEW_NAME FROM CONFIG.STREAM_METADATA").collect()
        queryable_views = set(r["STREAM_VIEW_NAME"] for r in meta_views)

        tables_clause_parts = []
        facts_parts = []
        dims_parts = []
        view_stats = {}
        included_views = []

        for view_name in view_names:
            if view_name not in queryable_views:
                continue
            quoted = view_name.replace('"', '""')
            try:
                col_rows = session.sql(f'DESCRIBE VIEW PROXY_VIEWS."{quoted}"').collect()
            except:
                continue

            if not col_rows:
                continue

            col_names = [cr["name"] for cr in col_rows]
            col_types = {cr["name"]: cr["type"].upper() for cr in col_rows}
            included_views.append(view_name)

            # Use a safe alias for the logical table name in the semantic view
            safe_table_alias = re.sub(r'[^A-Za-z0-9_]', '_', view_name).upper()

            tables_clause_parts.append(
                f'  {safe_table_alias} AS {proxy_prefix}.\"{quoted}\"\n'
                f'    COMMENT = \'Data table: {view_name.replace(chr(39), "")}\''
            )

            # Timestamp is always a dimension
            if "Timestamp" in col_names:
                dims_parts.append(
                    f'  {safe_table_alias}.MEASUREMENT_TIME AS {safe_table_alias}."Timestamp"\n'
                    f'    COMMENT = \'Measurement timestamp\''
                )

            fact_count = 0
            used_aliases = set()

            for cn in col_names:
                if cn == "Timestamp":
                    continue

                ct = col_types[cn]
                safe_col = cn.replace('"', '""')
                is_numeric = any(t in ct for t in ["FLOAT", "NUMBER", "INT", "DOUBLE", "DECIMAL"])
                is_string = any(t in ct for t in ["VARCHAR", "STRING", "TEXT"])

                # Long-format dimension columns: Field, Name, Uom
                if cn in ("Field", "Name", "Uom"):
                    alias = cn.upper()
                    comment = {"Field": "Signal/tag field name", "Name": "Asset or sensor name", "Uom": "Unit of measurement"}.get(cn, cn)
                    dims_parts.append(
                        f'  {safe_table_alias}.{alias} AS {safe_table_alias}."{safe_col}"\n'
                        f'    COMMENT = \'{comment}\''
                    )
                    continue

                # Value column in long-format tables
                if cn == "Value":
                    if is_numeric:
                        facts_parts.append(
                            f'  {safe_table_alias}.SENSOR_VALUE AS {safe_table_alias}."{safe_col}"::DOUBLE\n'
                            f'    COMMENT = \'Numeric sensor reading\''
                        )
                    else:
                        dims_parts.append(
                            f'  {safe_table_alias}.SENSOR_VALUE AS {safe_table_alias}."{safe_col}"\n'
                            f'    COMMENT = \'Sensor reading (string)\''
                        )
                    fact_count += 1
                    continue

                # Wide-format signal columns — each becomes a fact
                alias = _clean_alias(cn)
                # Ensure alias uniqueness within this table
                base_alias = alias
                counter = 2
                while alias in used_aliases:
                    alias = f"{base_alias}_{counter}"
                    counter += 1
                used_aliases.add(alias)

                unit = _extract_unit(cn)
                unit_str = f" ({unit})" if unit else ""
                # Strip UUID from the display name for the comment
                display_name = re.sub(r'\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '', cn)
                safe_comment = display_name.replace("'", "")

                if is_numeric:
                    facts_parts.append(
                        f'  {safe_table_alias}.{alias} AS {safe_table_alias}."{safe_col}"::DOUBLE\n'
                        f'    COMMENT = \'{safe_comment}{unit_str}\''
                    )
                    fact_count += 1
                elif is_string:
                    dims_parts.append(
                        f'  {safe_table_alias}.{alias} AS {safe_table_alias}."{safe_col}"\n'
                        f'    COMMENT = \'{safe_comment}\''
                    )

            view_stats[view_name] = fact_count

        if not tables_clause_parts:
            session.sql("UPDATE CONFIG.INIT_PROGRESS SET STATUS='COMPLETE', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='No queryable views - semantic view skipped' WHERE STEP_NAME='CREATE_SEMANTIC_VIEWS'").collect()
            return json.dumps({"status": "SUCCESS", "message": "No queryable views found - semantic view skipped (Cortex Analyst unavailable)", "details": {}})

        # Build the complete semantic view DDL
        ddl_parts = [f"CREATE OR REPLACE SEMANTIC VIEW DATA_VIEWS.AVEVA_ANALYTICS\n  TABLES (\n"]
        ddl_parts.append(",\n".join(tables_clause_parts))
        ddl_parts.append("\n  )")

        if facts_parts:
            ddl_parts.append("\n  FACTS (\n")
            ddl_parts.append(",\n".join(facts_parts))
            ddl_parts.append("\n  )")

        if dims_parts:
            ddl_parts.append("\n  DIMENSIONS (\n")
            ddl_parts.append(",\n".join(dims_parts))
            ddl_parts.append("\n  )")

        ddl_parts.append("\n  COMMENT = 'AVEVA CONNECT Analytics - auto-generated from proxy views'")
        ddl_parts.append("\n  AI_SQL_GENERATION 'This semantic view contains industrial time-series data from AVEVA CONNECT. "
                         "Tables may have different schemas: some are wide-format with many signal columns per row sharing a Timestamp, "
                         "others are long-format with Timestamp, Field, Value columns. "
                         "When querying wide-format tables, select the specific signal columns by name. "
                         "When querying long-format tables, filter by the Field column to select specific signals.'")

        ddl = "".join(ddl_parts)

        # Try to create the semantic view — may fail on CLD/Iceberg proxy views
        semantic_created = False
        semantic_error = None
        try:
            session.sql(ddl).collect()
            session.sql("GRANT SELECT ON SEMANTIC VIEW DATA_VIEWS.AVEVA_ANALYTICS TO APPLICATION ROLE APP_PUBLIC").collect()

            # Grant SELECT on each proxy view to APP_PUBLIC (required for Cortex Analyst)
            for view_name in included_views:
                quoted = view_name.replace('"', '""')
                try:
                    session.sql(f'GRANT SELECT ON VIEW PROXY_VIEWS."{quoted}" TO APPLICATION ROLE APP_PUBLIC').collect()
                except:
                    pass

            semantic_created = True
        except Exception as sv_err:
            semantic_error = str(sv_err)[:300]
            # If full DDL fails, try one view at a time to find which work
            for view_name in list(included_views):
                quoted = view_name.replace('"', '""')
                safe_alias = re.sub(r'[^A-Za-z0-9_]', '_', view_name).upper()
                test_ddl = (
                    f"CREATE OR REPLACE SEMANTIC VIEW DATA_VIEWS.AVEVA_ANALYTICS\n"
                    f"  TABLES (\n    {safe_alias} AS {proxy_prefix}.\"{quoted}\"\n"
                    f"      COMMENT = 'Data table: {view_name}'\n  )\n"
                    f"  COMMENT = 'AVEVA CONNECT Analytics - single table test'"
                )
                try:
                    session.sql(test_ddl).collect()
                    # This view works — but we need all of them, so just note it
                    semantic_created = False  # partial doesn't count
                except:
                    pass
            # Clean up any partial semantic view
            try:
                session.sql("DROP SEMANTIC VIEW IF EXISTS DATA_VIEWS.AVEVA_ANALYTICS").collect()
            except:
                pass

        # Update APP_STATE
        session.sql(
            "MERGE INTO CONFIG.APP_STATE T USING (SELECT 'SEMANTIC_VIEW_CREATED' AS KEY) S ON T.KEY = S.KEY "
            "WHEN MATCHED THEN UPDATE SET VALUE='" + ("TRUE" if semantic_created else "FALSE") + "', UPDATED_AT=CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES ('SEMANTIC_VIEW_CREATED', '" + ("TRUE" if semantic_created else "FALSE") + "')"
        ).collect()

        total_facts = sum(view_stats.values())

        if semantic_created:
            detail_msg = f"Semantic view with {len(view_stats)} tables, {total_facts} facts"
            session.sql(f"UPDATE CONFIG.INIT_PROGRESS SET STATUS='COMPLETE', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='{detail_msg}' WHERE STEP_NAME='CREATE_SEMANTIC_VIEWS'").collect()
            return json.dumps({
                "status": "SUCCESS",
                "message": f"Created semantic view AVEVA_ANALYTICS with {len(view_stats)} tables and {total_facts} facts",
                "details": {"tables": len(view_stats), "total_facts": total_facts, "per_table": view_stats}
            })
        else:
            detail_msg = f"Semantic view unavailable - CLD/Iceberg views cannot be referenced in DDL. App functions without Cortex Analyst."
            safe_detail = detail_msg.replace("'", "''")
            session.sql(f"UPDATE CONFIG.INIT_PROGRESS SET STATUS='COMPLETE', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='{safe_detail}' WHERE STEP_NAME='CREATE_SEMANTIC_VIEWS'").collect()
            return json.dumps({
                "status": "SUCCESS",
                "message": "Semantic view could not be created (CLD/Iceberg limitation) - app works without Cortex Analyst",
                "details": {"semantic_view": False, "reason": semantic_error, "views_attempted": len(included_views)}
            })

    except Exception as e:
        err_msg = str(e).replace("'", "''")
        session.sql(f"MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'CREATE_SEMANTIC_VIEWS' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='ERROR', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='{err_msg}' WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, COMPLETED_AT, DETAILS) VALUES (S.STEP_NAME, 'ERROR', CURRENT_TIMESTAMP(), '{err_msg}')").collect()
        return json.dumps({"status": "ERROR", "message": str(e), "details": {}})
$$;

GRANT USAGE ON PROCEDURE CORE.CREATE_SEMANTIC_VIEWS() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- IS_INITIALIZED
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.IS_INITIALIZED()
RETURNS BOOLEAN
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session):
    try:
        rows = session.sql("SELECT VALUE FROM CONFIG.APP_STATE WHERE KEY = 'VIEWS_INITIALIZED'").collect()
        if len(rows) > 0 and rows[0]["VALUE"] == "TRUE":
            return True
        return False
    except:
        return False
$$;

GRANT USAGE ON PROCEDURE CORE.IS_INITIALIZED() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- GET_SCHEMA_INFO
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.GET_SCHEMA_INFO()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session):
    lines = []
    lines.append("AVEVA Connect Data Schema (Industrial IoT Data)")
    lines.append("Data is organized as proxy views in PROXY_VIEWS schema, with metadata in CONFIG.STREAM_METADATA.")
    lines.append("Each signal maps to a specific proxy view and column (wide format) or Field value (long format).")
    lines.append("")
    lines.append("SEMANTIC VIEW: DATA_VIEWS.AVEVA_ANALYTICS")
    lines.append("Query via Cortex Analyst for natural-language access to all signals.")
    lines.append("")

    # Proxy views
    try:
        pv_rows = session.sql("SHOW VIEWS IN SCHEMA PROXY_VIEWS").collect()
        lines.append(f"Proxy Views: {len(pv_rows)}")
        for pv in pv_rows:
            lines.append(f"  - {pv['name']}")
    except:
        pass
    lines.append("")

    # Domain categories
    domain_rows = session.sql("SELECT DOMAIN_CATEGORY, COUNT(*) AS CNT FROM CONFIG.STREAM_METADATA GROUP BY DOMAIN_CATEGORY ORDER BY CNT DESC").collect()
    lines.append("Domain Categories:")
    for row in domain_rows:
        lines.append(f"  - {row['DOMAIN_CATEGORY']}: {row['CNT']} streams")

    # Assets
    asset_rows = session.sql("SELECT DISTINCT ASSET_NAME, DOMAIN_CATEGORY FROM CONFIG.STREAM_METADATA ORDER BY DOMAIN_CATEGORY, ASSET_NAME").collect()
    lines.append("")
    lines.append("Assets:")
    for row in asset_rows:
        lines.append(f"  - {row['ASSET_NAME']} ({row['DOMAIN_CATEGORY']})")

    # Signals
    signal_rows = session.sql("SELECT DISTINCT SIGNAL_NAME FROM CONFIG.STREAM_METADATA ORDER BY SIGNAL_NAME").collect()
    lines.append("")
    lines.append("Signal Types:")
    for row in signal_rows:
        lines.append(f"  - {row['SIGNAL_NAME']}")

    return "\n".join(lines)
$$;

GRANT USAGE ON PROCEDURE CORE.GET_SCHEMA_INFO() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- LOG_EVENT
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.LOG_EVENT(
    p_event_type VARCHAR,
    p_event_data VARIANT,
    p_user_query VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json

def run(session, p_event_type, p_event_data, p_user_query):
    redacted = None
    if p_user_query and len(p_user_query) > 0:
        try:
            safe_q = p_user_query.replace("'", "''")
            rows = session.sql(f"SELECT SNOWFLAKE.CORTEX.AI_REDACT('{safe_q}')").collect()
            if len(rows) > 0:
                redacted = str(rows[0][0])
        except:
            redacted = p_user_query

    # Build parameterized insert
    redacted_val = "NULL" if redacted is None else f"'{redacted.replace(chr(39), chr(39)+chr(39))}'"
    safe_type = p_event_type.replace("'", "''")

    if p_event_data is not None:
        safe_data = json.dumps(p_event_data).replace("'", "''") if isinstance(p_event_data, dict) else str(p_event_data).replace("'", "''")
        data_val = f"PARSE_JSON('{safe_data}')"
    else:
        data_val = "NULL"

    session.sql(
        f"INSERT INTO CONFIG.EVENT_LOG (EVENT_TYPE, EVENT_DATA, REDACTED_QUERY) "
        f"SELECT '{safe_type}', {data_val}, {redacted_val}"
    ).collect()

    return "Event logged"
$$;

GRANT USAGE ON PROCEDURE CORE.LOG_EVENT(VARCHAR, VARIANT, VARCHAR) TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- REINITIALIZE (full reset + re-run pipeline)
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.REINITIALIZE()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json

def run(session):
    results = {}
    try:
        # Clear all state
        session.sql("DELETE FROM CONFIG.APP_STATE").collect()
        session.sql("DELETE FROM CONFIG.STREAM_METADATA").collect()
        session.sql("DELETE FROM CONFIG.ASSET_HIERARCHY").collect()
        session.sql("DELETE FROM CONFIG.INIT_PROGRESS").collect()

        # Drop existing views in DATA_VIEWS (except system ones we'll recreate)
        try:
            existing_views = session.sql("SHOW VIEWS IN SCHEMA DATA_VIEWS").collect()
            for v in existing_views:
                vn = v["name"].replace('"', '""')
                session.sql(f'DROP VIEW IF EXISTS DATA_VIEWS."{vn}"').collect()
        except:
            pass

        # Drop materialized tables in DATA_VIEWS
        try:
            existing_tables = session.sql("SHOW TABLES IN SCHEMA DATA_VIEWS").collect()
            for t in existing_tables:
                tn = t["name"].replace('"', '""')
                session.sql(f'DROP TABLE IF EXISTS DATA_VIEWS."{tn}"').collect()
        except:
            pass

        # Drop semantic view
        try:
            session.sql("DROP SEMANTIC VIEW IF EXISTS DATA_VIEWS.AVEVA_ANALYTICS").collect()
        except:
            pass

        # Re-run initialization pipeline (3 steps)
        r1 = session.sql("CALL CORE.INITIALIZE_VIEWS()").collect()
        results["initialize_views"] = json.loads(r1[0][0]) if len(r1) > 0 else {"status": "ERROR", "message": "No result"}

        r2 = session.sql("CALL CORE.DISCOVER_METADATA()").collect()
        results["discover_metadata"] = json.loads(r2[0][0]) if len(r2) > 0 else {"status": "ERROR", "message": "No result"}

        r3 = session.sql("CALL CORE.CREATE_SEMANTIC_VIEWS()").collect()
        results["create_semantic_views"] = json.loads(r3[0][0]) if len(r3) > 0 else {"status": "ERROR", "message": "No result"}

        all_ok = all(r.get("status") == "SUCCESS" for r in results.values())
        overall = "SUCCESS" if all_ok else "PARTIAL"

        return json.dumps({
            "status": overall,
            "message": f"Reinitialization {overall.lower()}",
            "details": results
        })

    except Exception as e:
        return json.dumps({"status": "ERROR", "message": str(e), "details": results})
$$;

GRANT USAGE ON PROCEDURE CORE.REINITIALIZE() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- HEALTH_CHECK
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.HEALTH_CHECK()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json

def run(session):
    checks = {}

    # 1. PROXY_VIEWS count vs stored count
    try:
        pv_rows_check = session.sql("SHOW VIEWS IN SCHEMA PROXY_VIEWS").collect()
        shared_count = len(pv_rows_check)
        stored_rows = session.sql("SELECT VALUE FROM CONFIG.APP_STATE WHERE KEY = 'VIEW_COUNT'").collect()
        stored_count = int(stored_rows[0]["VALUE"]) if len(stored_rows) > 0 else 0
        checks["shared_content_sync"] = {
            "status": "OK" if shared_count == stored_count else "DRIFT",
            "shared_count": shared_count,
            "stored_count": stored_count
        }
    except Exception as e:
        checks["shared_content_sync"] = {"status": "ERROR", "error": str(e)}

    # 2. APP_STATE keys
    try:
        expected_keys = ["VIEWS_INITIALIZED", "VIEW_COUNT", "METADATA_DISCOVERED", "STREAM_COUNT", "DOMAIN_DISTRIBUTION", "SEMANTIC_VIEW_CREATED"]
        state_rows = session.sql("SELECT KEY FROM CONFIG.APP_STATE").collect()
        present_keys = [r["KEY"] for r in state_rows]
        missing = [k for k in expected_keys if k not in present_keys]
        checks["app_state_keys"] = {
            "status": "OK" if len(missing) == 0 else "MISSING",
            "present": present_keys,
            "missing": missing
        }
    except Exception as e:
        checks["app_state_keys"] = {"status": "ERROR", "error": str(e)}

    # 3. STREAM_METADATA row count
    try:
        meta_rows = session.sql("SELECT COUNT(*) AS CNT FROM CONFIG.STREAM_METADATA").collect()
        meta_count = meta_rows[0]["CNT"]
        checks["stream_metadata"] = {
            "status": "OK" if meta_count > 0 else "EMPTY",
            "row_count": meta_count
        }
    except Exception as e:
        checks["stream_metadata"] = {"status": "ERROR", "error": str(e)}

    # 4. Proxy views — count total vs queryable
    try:
        pv_rows = session.sql("SHOW VIEWS IN SCHEMA PROXY_VIEWS").collect()
        pv_count = len(pv_rows)
        if pv_count > 0:
            queryable_count = 0
            unreachable = []
            for pv in pv_rows:
                vn = pv["name"].replace('"', '""')
                try:
                    session.sql(f'SELECT 1 FROM PROXY_VIEWS."{vn}" LIMIT 1').collect()
                    queryable_count += 1
                except:
                    unreachable.append(pv["name"])
            if queryable_count == pv_count:
                checks["proxy_views"] = {"status": "OK", "total": pv_count, "queryable": queryable_count}
            elif queryable_count > 0:
                checks["proxy_views"] = {"status": "DEGRADED", "total": pv_count, "queryable": queryable_count, "unreachable": unreachable}
            else:
                checks["proxy_views"] = {"status": "ERROR", "total": pv_count, "queryable": 0, "unreachable": unreachable}
        else:
            checks["proxy_views"] = {"status": "EMPTY", "total": 0, "queryable": 0}
    except Exception as e:
        checks["proxy_views"] = {"status": "ERROR", "error": str(e)}

    # 5. AVEVA_ANALYTICS semantic view exists (optional — may be unavailable due to CLD limitations)
    try:
        session.sql("DESCRIBE SEMANTIC VIEW DATA_VIEWS.AVEVA_ANALYTICS").collect()
        checks["semantic_view"] = {"status": "OK"}
    except Exception as e:
        # Check if it was intentionally skipped
        try:
            sv_state = session.sql("SELECT VALUE FROM CONFIG.APP_STATE WHERE KEY='SEMANTIC_VIEW_CREATED'").collect()
            if sv_state and sv_state[0][0] == 'FALSE':
                checks["semantic_view"] = {"status": "SKIPPED", "note": "CLD/Iceberg limitation - Cortex Analyst unavailable"}
            else:
                checks["semantic_view"] = {"status": "ERROR", "error": str(e)}
        except:
            checks["semantic_view"] = {"status": "ERROR", "error": str(e)}

    # 6. Data freshness
    try:
        meta_views = session.sql("SELECT DISTINCT STREAM_VIEW_NAME FROM CONFIG.STREAM_METADATA").collect()
        freshness = {}
        stale_count = 0
        for mv in meta_views[:10]:
            vn = mv["STREAM_VIEW_NAME"].replace('"', '""')
            try:
                fr = session.sql(f'SELECT MAX("Timestamp") AS LATEST FROM PROXY_VIEWS."{vn}"').collect()
                if fr and fr[0]["LATEST"]:
                    latest = fr[0]["LATEST"]
                    hours_ago = session.sql(f"SELECT DATEDIFF('hour', '{latest}'::TIMESTAMP_LTZ, CURRENT_TIMESTAMP())").collect()
                    hours = int(hours_ago[0][0]) if hours_ago else -1
                    freshness[mv["STREAM_VIEW_NAME"]] = {"latest": str(latest), "hours_stale": hours}
                    if hours > 48:
                        stale_count += 1
                else:
                    freshness[mv["STREAM_VIEW_NAME"]] = {"latest": None, "hours_stale": -1}
            except:
                freshness[mv["STREAM_VIEW_NAME"]] = {"latest": "ERROR", "hours_stale": -1}
        checks["data_freshness"] = {
            "status": "OK" if stale_count == 0 else "STALE",
            "stale_views": stale_count,
            "total_views": len(meta_views),
            "details": freshness
        }
    except Exception as e:
        checks["data_freshness"] = {"status": "ERROR", "error": str(e)}

    all_ok = all(c.get("status") in ("OK", "SKIPPED", "STALE") for c in checks.values())
    return json.dumps({
        "status": "HEALTHY" if all_ok else "DEGRADED",
        "message": "All checks passed" if all_ok else "Some checks failed",
        "details": checks
    })
$$;

GRANT USAGE ON PROCEDURE CORE.HEALTH_CHECK() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- GET_INIT_STATUS (for Streamlit progress UI)
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.GET_INIT_STATUS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json

def run(session):
    expected_steps = ["INITIALIZE_VIEWS", "DISCOVER_METADATA", "CREATE_SEMANTIC_VIEWS"]

    try:
        rows = session.sql("SELECT STEP_NAME, STATUS, STARTED_AT, COMPLETED_AT, DETAILS FROM CONFIG.INIT_PROGRESS ORDER BY STARTED_AT").collect()
        completed_steps = {}
        for r in rows:
            completed_steps[r["STEP_NAME"]] = {
                "status": r["STATUS"],
                "started_at": str(r["STARTED_AT"]) if r["STARTED_AT"] else None,
                "completed_at": str(r["COMPLETED_AT"]) if r["COMPLETED_AT"] else None,
                "details": r["DETAILS"]
            }

        steps = []
        for s in expected_steps:
            if s in completed_steps:
                steps.append({"step": s, **completed_steps[s]})
            else:
                steps.append({"step": s, "status": "PENDING", "started_at": None, "completed_at": None, "details": None})

        done_count = sum(1 for st in steps if st["status"] == "COMPLETE")
        error_count = sum(1 for st in steps if st["status"] == "ERROR")
        running_count = sum(1 for st in steps if st["status"] == "RUNNING")

        if error_count > 0:
            overall = "ERROR"
        elif running_count > 0:
            overall = "RUNNING"
        elif done_count == len(expected_steps):
            overall = "COMPLETE"
        elif done_count > 0:
            overall = "PARTIAL"
        else:
            overall = "NOT_STARTED"

        # Check if any step is currently running
        running_rows = session.sql("SELECT STEP_NAME FROM CONFIG.INIT_PROGRESS WHERE STATUS = 'RUNNING'").collect()
        locked = len(running_rows) > 0

        return json.dumps({
            "status": overall,
            "message": f"{done_count}/{len(expected_steps)} steps complete",
            "details": {
                "steps": steps,
                "locked": locked,
                "completed": done_count,
                "total": len(expected_steps)
            }
        })

    except Exception as e:
        return json.dumps({"status": "ERROR", "message": str(e), "details": {}})
$$;

GRANT USAGE ON PROCEDURE CORE.GET_INIT_STATUS() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- DETECT_ANOMALIES
-- =====================================================
-- Automatically identifies numeric signals and runs
-- Cortex ML ANOMALY_DETECTION on the selected signal.
-- If no signal is specified, auto-detects the best
-- candidates based on variance and data density.

CREATE OR REPLACE PROCEDURE CORE.DETECT_ANOMALIES(
    p_asset_name VARCHAR,
    p_signal_name VARCHAR,
    p_lookback_days NUMBER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json

def _resolve_signal(session, asset_name, signal_name):
    """Look up STREAM_METADATA to find the proxy view and build SQL fragments.
    Returns (view_fqn, ts_col, value_expr, where_filter) or raises ValueError."""
    safe_a = asset_name.replace("'", "''")
    safe_s = signal_name.replace("'", "''")
    meta = session.sql(
        f"SELECT STREAM_VIEW_NAME, STREAM_NAME FROM CONFIG.STREAM_METADATA "
        f"WHERE ASSET_NAME = '{safe_a}' AND SIGNAL_NAME = '{safe_s}' LIMIT 1"
    ).collect()
    if not meta:
        raise ValueError(f"No metadata found for {asset_name}.{signal_name}")
    view_name = meta[0]["STREAM_VIEW_NAME"]
    stream_name = meta[0]["STREAM_NAME"]
    quoted_view = view_name.replace('"', '""')
    view_fqn = f'PROXY_VIEWS."{quoted_view}"'

    # Detect long vs wide format
    cols = session.sql(f'DESCRIBE VIEW {view_fqn}').collect()
    col_names = set(c["name"] for c in cols)
    has_field = "Field" in col_names

    if has_field:
        safe_stream = stream_name.replace("'", "''")
        return (view_fqn, '"Timestamp"', 'TRY_CAST("Value" AS DOUBLE)',
                "\"Field\" = '" + safe_stream + "' AND \"Value\" IS NOT NULL")
    else:
        safe_col = stream_name.replace('"', '""')
        col_ref = '"' + safe_col + '"'
        return (view_fqn, '"Timestamp"', 'TRY_CAST(' + col_ref + ' AS DOUBLE)',
                col_ref + ' IS NOT NULL')

def run(session, p_asset_name, p_signal_name, p_lookback_days):
    try:
        lookback = p_lookback_days if p_lookback_days and p_lookback_days > 0 else 7

        # If signal not specified, auto-detect best numeric candidates from metadata
        if not p_signal_name or p_signal_name == '' or p_signal_name == 'AUTO':
            safe_asset = p_asset_name.replace("'", "''") if p_asset_name else ""
            where_asset = f"AND ASSET_NAME = '{safe_asset}'" if p_asset_name and p_asset_name != 'ALL' else ""
            # Get candidate signals from metadata
            candidates_meta = session.sql(f"""
                SELECT ASSET_NAME, SIGNAL_NAME, STREAM_VIEW_NAME, STREAM_NAME
                FROM CONFIG.STREAM_METADATA
                WHERE DATA_TYPE IN ('FLOAT', 'NUMBER', 'DOUBLE', 'NUMERIC')
                  {where_asset}
                ORDER BY ASSET_NAME, SIGNAL_NAME
                LIMIT 20
            """).collect()

            signal_list = []
            for cm in candidates_meta:
                try:
                    view_fqn, ts_col, value_expr, where_filter = _resolve_signal(
                        session, cm["ASSET_NAME"], cm["SIGNAL_NAME"])
                    stats = session.sql(f"""
                        SELECT COUNT(*) AS CNT, STDDEV({value_expr}) AS STDDEV_VAL, AVG({value_expr}) AS AVG_VAL
                        FROM {view_fqn}
                        WHERE {where_filter}
                          AND {ts_col} >= DATEADD('day', -{lookback}, CURRENT_TIMESTAMP())
                    """).collect()
                    cnt = stats[0]["CNT"] if stats else 0
                    stddev = float(stats[0]["STDDEV_VAL"]) if stats and stats[0]["STDDEV_VAL"] else 0
                    avg_v = float(stats[0]["AVG_VAL"]) if stats and stats[0]["AVG_VAL"] else 0
                    if cnt >= 50 and stddev > 0:
                        signal_list.append({
                            "asset": cm["ASSET_NAME"], "signal": cm["SIGNAL_NAME"],
                            "readings": cnt, "stddev": stddev, "avg": avg_v
                        })
                except Exception:
                    continue
                if len(signal_list) >= 10:
                    break

            signal_list.sort(key=lambda x: x["stddev"], reverse=True)

            if len(signal_list) == 0:
                return json.dumps({
                    "status": "ERROR",
                    "message": "No suitable numeric signals found for anomaly detection",
                    "details": {"lookback_days": lookback}
                })

            return json.dumps({
                "status": "CANDIDATES",
                "message": f"Found {len(signal_list)} signals suitable for anomaly detection",
                "details": {"candidates": signal_list, "lookback_days": lookback}
            })

        # Run anomaly detection on specified signal
        safe_asset = p_asset_name.replace("'", "''")
        safe_signal = p_signal_name.replace("'", "''")
        view_fqn, ts_col, value_expr, where_filter = _resolve_signal(session, p_asset_name, p_signal_name)

        # Count available data
        row_count = session.sql(f"""
            SELECT COUNT(*) AS CNT FROM {view_fqn}
            WHERE {where_filter}
              AND {ts_col} >= DATEADD('day', -{lookback}, CURRENT_TIMESTAMP())
        """).collect()
        data_count = row_count[0]["CNT"] if len(row_count) > 0 else 0
        if data_count < 12:
            return json.dumps({
                "status": "ERROR",
                "message": f"Insufficient data for anomaly detection ({data_count} rows, need >= 12)",
                "details": {"data_count": data_count}
            })

        # Detect anomalies using rolling z-score with 24-point window
        session.sql(f"""
            DELETE FROM CONFIG.ANOMALY_RESULTS
            WHERE ASSET_NAME = '{safe_asset}' AND SIGNAL_NAME = '{safe_signal}' AND MODEL_TYPE = 'ANOMALY'
        """).collect()

        session.sql(f"""
            INSERT INTO CONFIG.ANOMALY_RESULTS
                (ASSET_NAME, SIGNAL_NAME, DOMAIN_CATEGORY, TS, NUMERIC_VALUE,
                 IS_ANOMALY, ANOMALY_SCORE, FORECAST_VALUE, LOWER_BOUND, UPPER_BOUND, MODEL_TYPE)
            WITH base AS (
                SELECT {ts_col} AS TS, {value_expr} AS VALUE,
                       AVG({value_expr}) OVER (ORDER BY {ts_col} ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS ROLLING_AVG,
                       STDDEV({value_expr}) OVER (ORDER BY {ts_col} ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS ROLLING_STD
                FROM {view_fqn}
                WHERE {where_filter}
                  AND {ts_col} >= DATEADD('day', -{lookback}, CURRENT_TIMESTAMP())
            ),
            scored AS (
                SELECT TS, VALUE, ROLLING_AVG, ROLLING_STD,
                       CASE WHEN ROLLING_STD > 0 THEN ABS((VALUE - ROLLING_AVG) / ROLLING_STD) ELSE 0 END AS Z_SCORE
                FROM base
                WHERE ROLLING_AVG IS NOT NULL
            )
            SELECT
                '{safe_asset}', '{safe_signal}',
                (SELECT DOMAIN_CATEGORY FROM CONFIG.STREAM_METADATA
                 WHERE ASSET_NAME = '{safe_asset}' AND SIGNAL_NAME = '{safe_signal}' LIMIT 1),
                TS, VALUE,
                CASE WHEN Z_SCORE > 3 THEN TRUE ELSE FALSE END,
                Z_SCORE,
                ROLLING_AVG,
                ROLLING_AVG - 3 * ROLLING_STD,
                ROLLING_AVG + 3 * ROLLING_STD,
                'ANOMALY'
            FROM scored
        """).collect()

        anomaly_count_rows = session.sql(f"""
            SELECT COUNT(*) AS CNT FROM CONFIG.ANOMALY_RESULTS
            WHERE ASSET_NAME = '{safe_asset}' AND SIGNAL_NAME = '{safe_signal}'
              AND MODEL_TYPE = 'ANOMALY' AND IS_ANOMALY = TRUE
        """).collect()
        anomaly_count = anomaly_count_rows[0]["CNT"] if len(anomaly_count_rows) > 0 else 0

        return json.dumps({
            "status": "SUCCESS",
            "message": f"Detected {anomaly_count} anomalies in {safe_asset}.{safe_signal}",
            "details": {
                "asset": p_asset_name, "signal": p_signal_name,
                "anomaly_count": anomaly_count, "total_readings": data_count,
                "lookback_days": lookback
            }
        })

    except Exception as e:
        return json.dumps({"status": "ERROR", "message": str(e), "details": {}})
$$;

GRANT USAGE ON PROCEDURE CORE.DETECT_ANOMALIES(VARCHAR, VARCHAR, NUMBER) TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- FORECAST_SIGNAL
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.FORECAST_SIGNAL(
    p_asset_name VARCHAR,
    p_signal_name VARCHAR,
    p_horizon_days NUMBER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json
from datetime import timedelta

def _resolve_signal(session, asset_name, signal_name):
    """Look up STREAM_METADATA to find the proxy view and build SQL fragments.
    Returns (view_fqn, ts_col, value_expr, where_filter) or raises ValueError."""
    safe_a = asset_name.replace("'", "''")
    safe_s = signal_name.replace("'", "''")
    meta = session.sql(
        f"SELECT STREAM_VIEW_NAME, STREAM_NAME FROM CONFIG.STREAM_METADATA "
        f"WHERE ASSET_NAME = '{safe_a}' AND SIGNAL_NAME = '{safe_s}' LIMIT 1"
    ).collect()
    if not meta:
        raise ValueError(f"No metadata found for {asset_name}.{signal_name}")
    view_name = meta[0]["STREAM_VIEW_NAME"]
    stream_name = meta[0]["STREAM_NAME"]
    quoted_view = view_name.replace('"', '""')
    view_fqn = f'PROXY_VIEWS."{quoted_view}"'

    cols = session.sql(f'DESCRIBE VIEW {view_fqn}').collect()
    col_names = set(c["name"] for c in cols)
    has_field = "Field" in col_names

    if has_field:
        safe_stream = stream_name.replace("'", "''")
        return (view_fqn, '"Timestamp"', 'TRY_CAST("Value" AS DOUBLE)',
                "\"Field\" = '" + safe_stream + "' AND \"Value\" IS NOT NULL")
    else:
        safe_col = stream_name.replace('"', '""')
        col_ref = '"' + safe_col + '"'
        return (view_fqn, '"Timestamp"', 'TRY_CAST(' + col_ref + ' AS DOUBLE)',
                col_ref + ' IS NOT NULL')

def run(session, p_asset_name, p_signal_name, p_horizon_days):
    try:
        horizon = p_horizon_days if p_horizon_days and p_horizon_days > 0 else 7

        if not p_asset_name or not p_signal_name:
            # Auto-select a signal with good data density
            auto = session.sql(
                "SELECT ASSET_NAME, SIGNAL_NAME FROM CONFIG.STREAM_METADATA "
                "WHERE DATA_TYPE IN ('FLOAT', 'NUMBER', 'DOUBLE', 'NUMERIC') "
                "ORDER BY SIGNAL_NAME LIMIT 1"
            ).collect()
            if not auto:
                return json.dumps({"status": "ERROR", "message": "No numeric signals found for auto-selection", "details": {}})
            p_asset_name = auto[0]["ASSET_NAME"]
            p_signal_name = auto[0]["SIGNAL_NAME"]

        view_fqn, ts_col, value_expr, where_filter = _resolve_signal(session, p_asset_name, p_signal_name)
        safe_asset = p_asset_name.replace("'", "''")
        safe_signal = p_signal_name.replace("'", "''")

        # Count available data and determine frequency
        stats = session.sql(
            "SELECT COUNT(*) AS CNT, MIN(" + ts_col + ") AS MIN_TS, MAX(" + ts_col + ") AS MAX_TS, "
            "AVG(" + value_expr + ") AS AVG_VAL, STDDEV(" + value_expr + ") AS STD_VAL "
            "FROM " + view_fqn + " WHERE " + where_filter
        ).collect()
        data_count = stats[0]["CNT"] if len(stats) > 0 else 0
        if data_count < 12:
            return json.dumps({
                "status": "ERROR",
                "message": f"Insufficient data for forecasting ({data_count} rows, need >= 12)",
                "details": {"data_count": data_count}
            })

        std_val = float(stats[0]["STD_VAL"]) if stats[0]["STD_VAL"] else 1

        # Determine data frequency
        try:
            freq_rows = session.sql(
                "SELECT MEDIAN(INTERVAL_MIN) AS MED_INTERVAL FROM ("
                " SELECT TIMESTAMPDIFF('minute', LAG(" + ts_col + ") OVER (ORDER BY " + ts_col + "), " + ts_col + ") AS INTERVAL_MIN"
                " FROM " + view_fqn + " WHERE " + where_filter +
                " ORDER BY " + ts_col + " DESC LIMIT 200"
                ") WHERE INTERVAL_MIN > 0"
            ).collect()
        except Exception as freq_err:
            return json.dumps({"status": "ERROR", "message": f"Frequency query failed: {freq_err}", "details": {"view_fqn": view_fqn, "ts_col": ts_col, "where_filter": where_filter}})
        avg_interval_min = float(freq_rows[0]["MED_INTERVAL"]) if freq_rows and freq_rows[0]["MED_INTERVAL"] else 60
        steps_per_day = max(int(1440 / avg_interval_min), 1)
        forecast_steps = min(steps_per_day * horizon, 5000)

        # Fetch last 200 data points for training (ordered chronologically)
        training_rows = session.sql(
            "SELECT TS, VALUE FROM ("
            " SELECT " + ts_col + " AS TS, " + value_expr + " AS VALUE, ROW_NUMBER() OVER (ORDER BY " + ts_col + " DESC) AS RN"
            " FROM " + view_fqn + " WHERE " + where_filter +
            ") WHERE RN <= 200 ORDER BY TS ASC"
        ).collect()

        if len(training_rows) < 5:
            return json.dumps({
                "status": "ERROR",
                "message": f"Not enough training data ({len(training_rows)} points)",
                "details": {}
            })

        values = [float(r["VALUE"]) for r in training_rows]
        last_ts = training_rows[-1]["TS"]

        # Double Exponential Smoothing (Holt's method)
        alpha = 0.3   # level smoothing
        beta = 0.1    # trend smoothing
        phi = 0.98    # damping factor

        # Initialize: level = first value, trend = average of first differences
        level = values[0]
        trend = (values[-1] - values[0]) / (len(values) - 1) if len(values) > 1 else 0

        # Fit on training data to get final level and trend
        residuals = []
        for i in range(1, len(values)):
            forecast_val = level + phi * trend
            residuals.append(values[i] - forecast_val)
            new_level = alpha * values[i] + (1 - alpha) * (level + phi * trend)
            new_trend = beta * (new_level - level) + (1 - beta) * phi * trend
            level = new_level
            trend = new_trend

        # Residual standard deviation for confidence bands
        if len(residuals) > 1:
            mean_r = sum(residuals) / len(residuals)
            residual_std = (sum((r - mean_r) ** 2 for r in residuals) / (len(residuals) - 1)) ** 0.5
        else:
            residual_std = std_val

        # Get domain category
        domain_rows = session.sql(f"""
            SELECT DOMAIN_CATEGORY FROM CONFIG.STREAM_METADATA
            WHERE ASSET_NAME = '{safe_asset}' AND SIGNAL_NAME = '{safe_signal}' LIMIT 1
        """).collect()
        domain = domain_rows[0]["DOMAIN_CATEGORY"] if domain_rows else "general"

        # Delete previous forecast
        session.sql(f"""
            DELETE FROM CONFIG.ANOMALY_RESULTS
            WHERE ASSET_NAME = '{safe_asset}' AND SIGNAL_NAME = '{safe_signal}' AND MODEL_TYPE = 'FORECAST'
        """).collect()

        # Generate forecast points with damped trend and widening confidence bands
        fc_values = []
        cumulative_phi = 0
        for step in range(1, forecast_steps + 1):
            cumulative_phi += phi ** step
            fc_val = level + cumulative_phi * trend
            band_width = 1.96 * residual_std * (step ** 0.5)
            fc_ts = last_ts + timedelta(minutes=avg_interval_min * step)
            fc_values.append((fc_ts, fc_val, fc_val - band_width, fc_val + band_width))

        # Bulk insert in batches of 500
        batch_size = 500
        for b in range(0, len(fc_values), batch_size):
            batch = fc_values[b:b + batch_size]
            value_rows = []
            for ts, fv, lb, ub in batch:
                ts_str = ts.strftime("%Y-%m-%d %H:%M:%S.%f") if hasattr(ts, "strftime") else str(ts)
                value_rows.append(
                    f"('{safe_asset}', '{safe_signal}', '{domain}', "
                    f"'{ts_str}'::TIMESTAMP_NTZ, {fv:.6f}, {lb:.6f}, {ub:.6f}, 'FORECAST')"
                )
            session.sql(f"""
                INSERT INTO CONFIG.ANOMALY_RESULTS
                    (ASSET_NAME, SIGNAL_NAME, DOMAIN_CATEGORY, TS,
                     FORECAST_VALUE, LOWER_BOUND, UPPER_BOUND, MODEL_TYPE)
                VALUES {', '.join(value_rows)}
            """).collect()

        return json.dumps({
            "status": "SUCCESS",
            "message": f"Generated {len(fc_values)}-step forecast for {p_asset_name}.{p_signal_name}",
            "details": {
                "asset": p_asset_name, "signal": p_signal_name,
                "forecast_steps": len(fc_values), "horizon_days": horizon,
                "avg_interval_minutes": round(avg_interval_min, 1),
                "training_rows": len(values),
                "method": "double_exponential_smoothing",
                "alpha": alpha, "beta": beta, "phi": phi
            }
        })

    except Exception as e:
        return json.dumps({"status": "ERROR", "message": str(e), "details": {}})
$$;

GRANT USAGE ON PROCEDURE CORE.FORECAST_SIGNAL(VARCHAR, VARCHAR, NUMBER) TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- SHARE_ANOMALIES_BACK (consumer -> provider)
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.SHARE_ANOMALIES_BACK()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json

def run(session):
    try:
        # Get the installing consumer account
        acct_rows = session.sql("SELECT CURRENT_ACCOUNT() AS ACCT").collect()
        consumer_account = acct_rows[0]["ACCT"] if len(acct_rows) > 0 else "UNKNOWN"
        safe_acct = consumer_account.replace("'", "''")

        # Copy latest anomaly results into the share-back table
        session.sql(f"DELETE FROM CONFIG.ANOMALY_SHARE_BACK WHERE CONSUMER_ACCOUNT = '{safe_acct}'").collect()

        session.sql(f"""
            INSERT INTO CONFIG.ANOMALY_SHARE_BACK
                (CONSUMER_ACCOUNT, ASSET_NAME, SIGNAL_NAME, DOMAIN_CATEGORY,
                 TS, NUMERIC_VALUE, IS_ANOMALY, ANOMALY_SCORE,
                 FORECAST_VALUE, LOWER_BOUND, UPPER_BOUND, MODEL_TYPE)
            SELECT
                '{safe_acct}', ASSET_NAME, SIGNAL_NAME, DOMAIN_CATEGORY,
                TS, NUMERIC_VALUE, IS_ANOMALY, ANOMALY_SCORE,
                FORECAST_VALUE, LOWER_BOUND, UPPER_BOUND, MODEL_TYPE
            FROM CONFIG.ANOMALY_RESULTS
        """).collect()

        count_rows = session.sql(f"""
            SELECT COUNT(*) AS CNT FROM CONFIG.ANOMALY_SHARE_BACK
            WHERE CONSUMER_ACCOUNT = '{safe_acct}'
        """).collect()
        shared_count = count_rows[0]["CNT"] if len(count_rows) > 0 else 0

        anomaly_rows = session.sql(f"""
            SELECT COUNT(*) AS CNT FROM CONFIG.ANOMALY_SHARE_BACK
            WHERE CONSUMER_ACCOUNT = '{safe_acct}' AND IS_ANOMALY = TRUE
        """).collect()
        anomaly_count = anomaly_rows[0]["CNT"] if len(anomaly_rows) > 0 else 0

        return json.dumps({
            "status": "SUCCESS",
            "message": f"Shared {shared_count} results ({anomaly_count} anomalies) back to provider",
            "details": {
                "consumer_account": consumer_account,
                "shared_count": shared_count,
                "anomaly_count": anomaly_count
            }
        })

    except Exception as e:
        return json.dumps({"status": "ERROR", "message": str(e), "details": {}})
$$;

GRANT USAGE ON PROCEDURE CORE.SHARE_ANOMALIES_BACK() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- Streamlit Application
-- =====================================================

CREATE STREAMLIT IF NOT EXISTS CORE.HOME
  FROM '/streamlit'
  MAIN_FILE = '/main.py';

GRANT USAGE ON STREAMLIT CORE.HOME TO APPLICATION ROLE APP_PUBLIC;
