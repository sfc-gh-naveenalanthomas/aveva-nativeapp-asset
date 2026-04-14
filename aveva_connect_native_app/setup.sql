-- =====================================================
-- AVEVA CONNECT PI Data Historian - Native App Setup
-- =====================================================
--
-- Architecture:
--   CORE       (versioned)     - Stored procedures + Streamlit app
--   CONFIG     (non-versioned) - Runtime state tables (persist across upgrades)
--   DATA_VIEWS (non-versioned) - Unified view (PI_STREAMS_UNIFIED) + semantic view (DYNAMIC_ANALYTICS)
--   PROXY_VIEWS (app package)  - Secure views over CLD Iceberg tables (provider-managed, not in this file)
--
-- Initialization pipeline (called by consumer or Streamlit UI):
--   1. CORE.INITIALIZE_VIEWS()       - Discovers proxy views from PROXY_VIEWS schema
--   2. CORE.DISCOVER_METADATA()      - Inspects each view, extracts asset/signal metadata, classifies domains via Cortex AI
--   3. CORE.CREATE_UNIFIED_VIEW()    - Builds PI_STREAMS_UNIFIED (UNION ALL across all proxy views, normalized schema)
--   4. CORE.GENERATE_SEMANTIC_VIEW() - Creates DYNAMIC_ANALYTICS semantic view for Cortex Analyst
--
-- All stored procedures return structured JSON: {"status": "SUCCESS|ERROR", "message": "...", "details": {...}}
--
-- =====================================================

CREATE APPLICATION ROLE IF NOT EXISTS APP_PUBLIC;

-- CORE: versioned schema - all procs and Streamlit are replaced on each upgrade
CREATE OR ALTER VERSIONED SCHEMA CORE;
GRANT USAGE ON SCHEMA CORE TO APPLICATION ROLE APP_PUBLIC;

-- DATA_VIEWS: non-versioned - holds the unified view and semantic view (recreated by procs)
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
-- Init Lock (idempotency / concurrency guard)
-- =====================================================

CREATE TABLE IF NOT EXISTS CONFIG.INIT_LOCK (
    LOCK_ID VARCHAR PRIMARY KEY,
    ACQUIRED_AT TIMESTAMP_LTZ,
    ACQUIRED_BY VARCHAR
);

GRANT SELECT, INSERT, DELETE ON TABLE CONFIG.INIT_LOCK TO APPLICATION ROLE APP_PUBLIC;

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
        # Acquire init lock
        existing = session.sql("SELECT LOCK_ID FROM CONFIG.INIT_LOCK WHERE LOCK_ID = 'INIT'").collect()
        if len(existing) > 0:
            return json.dumps({"status": "ERROR", "message": "Initialization already in progress", "details": {}})
        session.sql("INSERT INTO CONFIG.INIT_LOCK (LOCK_ID, ACQUIRED_AT, ACQUIRED_BY) SELECT 'INIT', CURRENT_TIMESTAMP(), CURRENT_USER()").collect()

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

        # Release lock
        session.sql("DELETE FROM CONFIG.INIT_LOCK WHERE LOCK_ID = 'INIT'").collect()

        return json.dumps({
            "status": "SUCCESS",
            "message": f"Discovered {view_count} proxy views",
            "details": {
                "view_count": view_count,
                "account": current_account
            }
        })

    except Exception as e:
        # Release lock on error
        try:
            session.sql("DELETE FROM CONFIG.INIT_LOCK WHERE LOCK_ID = 'INIT'").collect()
        except:
            pass
        session.sql("MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'INITIALIZE_VIEWS' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='ERROR', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='" + str(e).replace("'", "''") + "' WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, COMPLETED_AT, DETAILS) VALUES (S.STEP_NAME, 'ERROR', CURRENT_TIMESTAMP(), '" + str(e).replace("'", "''") + "')").collect()
        return json.dumps({"status": "ERROR", "message": str(e), "details": {}})
$$;

GRANT USAGE ON PROCEDURE CORE.INITIALIZE_VIEWS() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- DISCOVER_METADATA (Step 2 of init pipeline)
-- =====================================================
-- Scans all views in PROXY_VIEWS, detects data format
-- (Delta Sharing long, wide-format, or classic PI),
-- extracts asset/signal names, classifies industrial
-- domains via Cortex AI, and populates CONFIG.STREAM_METADATA
-- and CONFIG.ASSET_HIERARCHY.
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

def _detect_domain_keywords(asset_lower, signal_lower):
    """Fallback: detect domain category from asset and signal names using keywords."""
    wind_asset_keys = ["wind", "turbine", "hornsea", "ge0", "ge1", "yorkshire"]
    wind_signal_keys = [
        "wind_speed", "wind_direction", "pitch_angle", "vane_position",
        "gearbox", "hub_temp", "eaf", "active power", "apparent power",
        "expected power", "revenue", "total power", "total turbines"
    ]
    if any(k in asset_lower for k in wind_asset_keys) or any(k in signal_lower for k in wind_signal_keys):
        return "wind_energy"

    rotating_asset_keys = ["rotating", "ps0", "ps1", "ps2", "ps3", "ps4", "ps5", "ps6"]
    rotating_signal_keys = ["bearing", "suction", "discharge", "pump"]
    if any(k in asset_lower for k in rotating_asset_keys) or any(k in signal_lower for k in rotating_signal_keys):
        return "rotating_machinery"

    pq_asset_keys = ["plant", "pq."]
    pq_signal_keys = ["concentration", "granule", "dissolv", "ingredient", "agitator"]
    if any(k in asset_lower for k in pq_asset_keys) or any(k in signal_lower for k in pq_signal_keys):
        return "production_quality"

    pkg_asset_keys = ["pack", "line"]
    pkg_signal_keys = ["carb", "chill", "warmer", "running", "refrigerant"]
    if any(k in asset_lower for k in pkg_asset_keys) or any(k in signal_lower for k in pkg_signal_keys):
        return "packaging"

    return "general"


def _clean_signal_name(raw):
    """Strip UUID suffix and unit annotation from AVEVA signal names.
    E.g. 'Active Power - 10 min rolling avg.4cda6eb5-885a-5fd4-323f-add6141de672 (kW)' -> 'Active Power - 10 min rolling avg'
    E.g. 'BL1_ACT (°)' -> 'BL1_ACT'
    E.g. 'Auto Stop Flag ()' -> 'Auto Stop Flag'"""
    import re
    cleaned = re.sub(r'\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '', raw)
    cleaned = re.sub(r'\s*\([^)]*\)\s*$', '', cleaned)
    return cleaned.strip()


def _classify_domains_ai(session, pairs):
    """Use Cortex Complete to classify asset/signal pairs into industrial domains.
    Returns dict of {(asset, signal): domain}. Falls back to keyword matching on failure."""
    VALID_DOMAINS = {"wind_energy", "rotating_machinery", "production_quality", "packaging", "general"}
    result = {}

    if not pairs:
        return result

    # Build batch list (limit to 200 pairs per call to stay within token limits)
    batch = pairs[:200]
    pair_list = "\n".join(f"- {a} | {s}" for a, s in batch)
    prompt = (
        "Classify these industrial asset/signal pairs into exactly one domain each.\n"
        "Valid domains: wind_energy, rotating_machinery, production_quality, packaging, general\n\n"
        "Asset | Signal pairs:\n"
        f"{pair_list}\n\n"
        "Return ONLY a JSON object where each key is \"asset|signal\" and value is the domain.\n"
        "Example: {\"Turbine_1|Wind_Speed\": \"wind_energy\", \"Pump_A|Bearing_Temp\": \"rotating_machinery\"}\n"
        "Return ONLY valid JSON, no explanation."
    )
    safe_prompt = prompt.replace("'", "''")

    try:
        rows = session.sql(
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{safe_prompt}')"
        ).collect()
        text = rows[0][0] if rows else ""
        # Extract JSON from response
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            ai_result = json.loads(text[start:end])
            for key, domain in ai_result.items():
                parts = key.split("|")
                if len(parts) == 2:
                    a = parts[0].strip()
                    s = parts[1].strip()
                    if domain in VALID_DOMAINS:
                        result[(a, s)] = domain
    except Exception:
        pass

    # Fall back to keyword matching for any pairs not classified by AI
    for a, s in pairs:
        if (a, s) not in result:
            result[(a, s)] = _detect_domain_keywords(a.lower(), s.lower())

    return result


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

        # First pass: collect all stream info (asset, signal, view, data_type) without domain
        stream_records = []
        for view_name in view_names:
            quoted = view_name.replace('"', '""')

            # Detect schema: Delta Sharing (Timestamp,Field,Value,Uom) vs PI stream
            col_rows = []
            try:
                col_rows = session.sql(f'DESCRIBE VIEW PROXY_VIEWS."{quoted}"').collect()
            except:
                pass
            col_names = set(cr["name"] for cr in col_rows)
            is_delta_sharing = "Field" in col_names and "Uom" in col_names

            if is_delta_sharing:
                # Delta Sharing long format: one view, many signals in "Field" column
                field_rows = session.sql(f'SELECT DISTINCT "Field" FROM PROXY_VIEWS."{quoted}"').collect()
                for fr in field_rows:
                    field_val = fr["Field"]
                    parts = field_val.split(".")
                    asset_name = ".".join(parts[:-1]) if len(parts) > 1 else field_val
                    signal_name = _clean_signal_name(parts[-1]) if len(parts) > 1 else "VALUE"
                    stream_records.append({
                        "view": view_name, "stream": field_val,
                        "asset": asset_name, "signal": signal_name, "dtype": "NUMERIC"
                    })
            else:
                # Check if this is wide-format (Timestamp + many signal columns, no Value/Field/Uom)
                has_value_col = any(cr["name"] in ("VALUE", "Value") for cr in col_rows)
                is_wide_format = "Timestamp" in col_names and not has_value_col and len(col_names) > 2

                if is_wide_format:
                    # Wide format: each non-Timestamp column is a separate signal
                    table_base = view_name.split("_")[0] if "_" in view_name else view_name
                    for cr in col_rows:
                        cn = cr["name"]
                        if cn == "Timestamp":
                            continue
                        ct = cr["type"].upper()
                        # Parse signal name: "GE01_P_ACT (kW)" -> asset="GE01", signal="P_ACT (kW)"
                        parts = cn.split(".", 1)
                        if len(parts) > 1:
                            asset_name = parts[0].strip()
                            signal_name = _clean_signal_name(parts[1].strip())
                        elif "_" in cn:
                            first_sep = cn.index("_")
                            asset_name = cn[:first_sep].strip()
                            signal_name = _clean_signal_name(cn[first_sep+1:].strip())
                        else:
                            asset_name = table_base
                            signal_name = cn
                        data_type = "STRING" if ("VARCHAR" in ct or "STRING" in ct) else "NUMERIC"
                        stream_records.append({
                            "view": view_name, "stream": cn,
                            "asset": asset_name, "signal": signal_name, "dtype": data_type,
                            "format": "wide"
                        })
                else:
                    # Classic PI stream format: one view per signal
                    stream_name = view_name.replace("_", " ")
                    parts = stream_name.split(".")
                    asset_name = ".".join(parts[:-1]) if len(parts) > 1 else stream_name
                    signal_name = _clean_signal_name(parts[-1]) if len(parts) > 1 else "VALUE"

                    data_type = "NUMERIC"
                    for cr in col_rows:
                        cn = cr["name"]
                        ct = cr["type"].upper()
                        if cn in ("VALUE", "Value"):
                            if "VARCHAR" in ct or "STRING" in ct:
                                data_type = "STRING"
                            elif "INT" in ct:
                                data_type = "INTEGER"

                    stream_records.append({
                        "view": view_name, "stream": stream_name,
                        "asset": asset_name, "signal": signal_name, "dtype": data_type
                    })

        # Batch classify domains using AI with keyword fallback
        all_pairs = [(r["asset"], r["signal"]) for r in stream_records]
        domain_lookup = _classify_domains_ai(session, all_pairs)

        # Second pass: insert metadata with classified domains
        for rec in stream_records:
            domain = domain_lookup.get((rec["asset"], rec["signal"]), "general")
            safe_view = rec["view"].replace("'", "''")
            safe_stream = rec["stream"].replace("'", "''")
            safe_asset = rec["asset"].replace("'", "''")
            safe_signal = rec["signal"].replace("'", "''")
            # Wide-format streams store DATA_TYPE='WIDE' for unified view detection
            stored_dtype = "WIDE" if rec.get("format") == "wide" else rec["dtype"]
            session.sql(
                f"INSERT INTO CONFIG.STREAM_METADATA (STREAM_VIEW_NAME, STREAM_NAME, ASSET_PATH, ASSET_NAME, SIGNAL_NAME, DOMAIN_CATEGORY, DATA_TYPE) "
                f"VALUES ('{safe_view}', '{safe_stream}', '{safe_asset}', '{safe_asset}', '{safe_signal}', '{domain}', '{stored_dtype}')"
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
            "message": f"Discovered {stream_count} PI streams across {len(assets)} assets",
            "details": {"stream_count": stream_count, "asset_count": len(assets), "domain_distribution": domain_map}
        })

    except Exception as e:
        err_msg = str(e).replace("'", "''")
        session.sql(f"MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'DISCOVER_METADATA' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='ERROR', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='{err_msg}' WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, COMPLETED_AT, DETAILS) VALUES (S.STEP_NAME, 'ERROR', CURRENT_TIMESTAMP(), '{err_msg}')").collect()
        return json.dumps({"status": "ERROR", "message": str(e), "details": {}})
$$;

GRANT USAGE ON PROCEDURE CORE.DISCOVER_METADATA() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- CREATE_UNIFIED_VIEW (Step 3 of init pipeline)
-- =====================================================
-- Reads STREAM_METADATA, builds a UNION ALL across all
-- proxy views (normalizing Delta Sharing, wide-format,
-- and classic PI schemas into a single schema), and
-- creates DATA_VIEWS.PI_STREAMS_UNIFIED.
-- Batches into sub-views if >50 streams.
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.CREATE_UNIFIED_VIEW()
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
        # Track progress
        session.sql("MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'CREATE_UNIFIED_VIEW' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='RUNNING', STARTED_AT=CURRENT_TIMESTAMP(), COMPLETED_AT=NULL, DETAILS=NULL WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, STARTED_AT) VALUES (S.STEP_NAME, 'RUNNING', CURRENT_TIMESTAMP())").collect()

        rows = session.sql(
            "SELECT STREAM_VIEW_NAME, STREAM_NAME, ASSET_NAME, SIGNAL_NAME, DOMAIN_CATEGORY, DATA_TYPE "
            "FROM CONFIG.STREAM_METADATA ORDER BY ASSET_NAME, SIGNAL_NAME"
        ).collect()

        if len(rows) == 0:
            return json.dumps({"status": "ERROR", "message": "No stream views found in STREAM_METADATA", "details": {}})

        # Detect which views are Delta Sharing (long format) by checking columns once per view
        view_is_delta = {}
        for row in rows:
            vn = row["STREAM_VIEW_NAME"]
            if vn not in view_is_delta:
                quoted_vn = vn.replace('"', '""')
                try:
                    cols = session.sql(f'DESCRIBE VIEW PROXY_VIEWS."{quoted_vn}"').collect()
                    col_names = set(c["name"] for c in cols)
                    view_is_delta[vn] = "Field" in col_names and "Uom" in col_names
                except:
                    view_is_delta[vn] = False

        unions = []
        for row in rows:
            view_name = row["STREAM_VIEW_NAME"]
            stream_name = row["STREAM_NAME"]
            asset = row["ASSET_NAME"].replace("'", "''")
            signal = row["SIGNAL_NAME"].replace("'", "''")
            domain = row["DOMAIN_CATEGORY"]
            dtype = row["DATA_TYPE"]
            quoted_vn = view_name.replace('"', '""')

            if view_is_delta.get(view_name, False):
                # Delta Sharing: filter by Field, use NULLs for PI-specific columns
                safe_stream = stream_name.replace("'", "''")
                unions.append(
                    f'SELECT "Timestamp" AS TS, "Value"::DOUBLE AS NUMERIC_VALUE, "Value"::VARCHAR AS STRING_VALUE, '
                    f'NULL::BOOLEAN AS IS_QUESTIONABLE, NULL::BOOLEAN AS IS_SUBSTITUTED, '
                    f'NULL::BOOLEAN AS IS_ANNOTATED, NULL::NUMBER AS SYSTEM_STATE_CODE, '
                    f'NULL::VARCHAR AS DIGITAL_STATE_NAME, '
                    f"'{asset}' AS ASSET_NAME, '{signal}' AS SIGNAL_NAME, '{domain}' AS DOMAIN_CATEGORY "
                    f'FROM PROXY_VIEWS."{quoted_vn}" WHERE "Field" = \'{safe_stream}\''
                )
            elif dtype == "WIDE":
                # Wide-format: each stream_name is a column name in the view
                safe_col = stream_name.replace('"', '""')
                unions.append(
                    f'SELECT "Timestamp" AS TS, "{safe_col}"::DOUBLE AS NUMERIC_VALUE, "{safe_col}"::VARCHAR AS STRING_VALUE, '
                    f'NULL::BOOLEAN AS IS_QUESTIONABLE, NULL::BOOLEAN AS IS_SUBSTITUTED, '
                    f'NULL::BOOLEAN AS IS_ANNOTATED, NULL::NUMBER AS SYSTEM_STATE_CODE, '
                    f'NULL::VARCHAR AS DIGITAL_STATE_NAME, '
                    f"'{asset}' AS ASSET_NAME, '{signal}' AS SIGNAL_NAME, '{domain}' AS DOMAIN_CATEGORY "
                    f'FROM PROXY_VIEWS."{quoted_vn}" WHERE "{safe_col}" IS NOT NULL'
                )
            else:
                # Classic PI stream format
                value_cast = "NULL" if dtype == "STRING" else '"Value"::DOUBLE'
                value_str = '"Value"::VARCHAR'
                unions.append(
                    f'SELECT "Timestamp" AS TS, {value_cast} AS NUMERIC_VALUE, {value_str} AS STRING_VALUE, '
                    f'"IsQuestionable" AS IS_QUESTIONABLE, "IsSubstituted" AS IS_SUBSTITUTED, '
                    f'"IsAnnotated" AS IS_ANNOTATED, "SystemStateCode" AS SYSTEM_STATE_CODE, '
                    f'"DigitalStateName" AS DIGITAL_STATE_NAME, '
                    f"'{asset}' AS ASSET_NAME, '{signal}' AS SIGNAL_NAME, '{domain}' AS DOMAIN_CATEGORY "
                    f'FROM PROXY_VIEWS."{quoted_vn}"'
                )

        batch_size = 50
        if len(unions) <= batch_size:
            ddl = "CREATE OR REPLACE VIEW DATA_VIEWS.PI_STREAMS_UNIFIED AS\n" + "\nUNION ALL\n".join(unions)
            session.sql(ddl).collect()
        else:
            temp_views = []
            for b in range(0, len(unions), batch_size):
                batch = unions[b:b + batch_size]
                batch_name = f"PI_STREAMS_BATCH_{b // batch_size}"
                batch_ddl = f"CREATE OR REPLACE VIEW DATA_VIEWS.{batch_name} AS\n" + "\nUNION ALL\n".join(batch)
                session.sql(batch_ddl).collect()
                session.sql(f"GRANT SELECT ON VIEW DATA_VIEWS.{batch_name} TO APPLICATION ROLE APP_PUBLIC").collect()
                temp_views.append(f"SELECT * FROM DATA_VIEWS.{batch_name}")
            final_ddl = "CREATE OR REPLACE VIEW DATA_VIEWS.PI_STREAMS_UNIFIED AS\n" + "\nUNION ALL\n".join(temp_views)
            session.sql(final_ddl).collect()

        session.sql("GRANT SELECT ON VIEW DATA_VIEWS.PI_STREAMS_UNIFIED TO APPLICATION ROLE APP_PUBLIC").collect()

        # Mark progress complete
        detail_msg = f"{len(unions)} streams unified"
        session.sql(f"UPDATE CONFIG.INIT_PROGRESS SET STATUS='COMPLETE', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='{detail_msg}' WHERE STEP_NAME='CREATE_UNIFIED_VIEW'").collect()

        return json.dumps({
            "status": "SUCCESS",
            "message": f"Created unified PI streams view with {len(unions)} streams",
            "details": {"stream_count": len(unions), "batched": len(unions) > batch_size}
        })

    except Exception as e:
        err_msg = str(e).replace("'", "''")
        session.sql(f"MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'CREATE_UNIFIED_VIEW' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='ERROR', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='{err_msg}' WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, COMPLETED_AT, DETAILS) VALUES (S.STEP_NAME, 'ERROR', CURRENT_TIMESTAMP(), '{err_msg}')").collect()
        return json.dumps({"status": "ERROR", "message": str(e), "details": {}})
$$;

GRANT USAGE ON PROCEDURE CORE.CREATE_UNIFIED_VIEW() TO APPLICATION ROLE APP_PUBLIC;

-- =====================================================
-- GENERATE_SEMANTIC_VIEW (Step 4 of init pipeline)
-- =====================================================
-- Creates DATA_VIEWS.DYNAMIC_ANALYTICS semantic view
-- on top of PI_STREAMS_UNIFIED. This enables Cortex
-- Analyst natural language queries against the data.
-- =====================================================

CREATE OR REPLACE PROCEDURE CORE.GENERATE_SEMANTIC_VIEW()
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
        # Track progress
        session.sql("MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'GENERATE_SEMANTIC_VIEW' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='RUNNING', STARTED_AT=CURRENT_TIMESTAMP(), COMPLETED_AT=NULL, DETAILS=NULL WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, STARTED_AT) VALUES (S.STEP_NAME, 'RUNNING', CURRENT_TIMESTAMP())").collect()

        # Build deterministic semantic view DDL based on PI_STREAMS_UNIFIED
        # Use CURRENT_DATABASE() so it works regardless of the installed app name
        current_db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
        ddl = f"""CREATE OR REPLACE SEMANTIC VIEW DATA_VIEWS.DYNAMIC_ANALYTICS
  TABLES (
    {current_db}.DATA_VIEWS.PI_STREAMS_UNIFIED
      COMMENT = 'AVEVA CONNECT PI Data Historian unified time-series stream data'
  )
  FACTS (
    PI_STREAMS_UNIFIED.SENSOR_VALUE AS PI_STREAMS_UNIFIED.NUMERIC_VALUE
      COMMENT = 'Numeric sensor reading from PI stream'
  )
  DIMENSIONS (
    PI_STREAMS_UNIFIED.MEASUREMENT_TIME AS PI_STREAMS_UNIFIED.TS
      COMMENT = 'Measurement timestamp',
    PI_STREAMS_UNIFIED.STATUS_VALUE AS PI_STREAMS_UNIFIED.STRING_VALUE
      COMMENT = 'String value for status and digital PI streams',
    PI_STREAMS_UNIFIED.QUESTIONABLE AS PI_STREAMS_UNIFIED.IS_QUESTIONABLE
      COMMENT = 'PI data quality flag questionable reading',
    PI_STREAMS_UNIFIED.SUBSTITUTED AS PI_STREAMS_UNIFIED.IS_SUBSTITUTED
      COMMENT = 'PI data quality flag substituted reading',
    PI_STREAMS_UNIFIED.ANNOTATED AS PI_STREAMS_UNIFIED.IS_ANNOTATED
      COMMENT = 'PI annotation flag',
    PI_STREAMS_UNIFIED.STATE_CODE AS PI_STREAMS_UNIFIED.SYSTEM_STATE_CODE
      COMMENT = 'PI system state code',
    PI_STREAMS_UNIFIED.DIGITAL_STATE AS PI_STREAMS_UNIFIED.DIGITAL_STATE_NAME
      COMMENT = 'PI digital state name',
    PI_STREAMS_UNIFIED.ASSET AS PI_STREAMS_UNIFIED.ASSET_NAME
      COMMENT = 'Asset identifier from PI hierarchy',
    PI_STREAMS_UNIFIED.SIGNAL AS PI_STREAMS_UNIFIED.SIGNAL_NAME
      COMMENT = 'Signal or tag name',
    PI_STREAMS_UNIFIED.DOMAIN AS PI_STREAMS_UNIFIED.DOMAIN_CATEGORY
      COMMENT = 'Auto-detected domain: wind_energy, rotating_machinery, production_quality, packaging, general'
  )
  COMMENT = 'AVEVA CONNECT PI Data Historian Analytics'"""

        session.sql(ddl).collect()
        session.sql("GRANT SELECT ON SEMANTIC VIEW DATA_VIEWS.DYNAMIC_ANALYTICS TO APPLICATION ROLE APP_PUBLIC").collect()

        # Update APP_STATE
        session.sql(
            "MERGE INTO CONFIG.APP_STATE T USING (SELECT 'SEMANTIC_VIEW_CREATED' AS KEY) S ON T.KEY = S.KEY "
            "WHEN MATCHED THEN UPDATE SET VALUE='TRUE', UPDATED_AT=CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (KEY, VALUE) VALUES ('SEMANTIC_VIEW_CREATED', 'TRUE')"
        ).collect()

        # Mark progress complete
        session.sql("UPDATE CONFIG.INIT_PROGRESS SET STATUS='COMPLETE', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='Semantic view DYNAMIC_ANALYTICS created' WHERE STEP_NAME='GENERATE_SEMANTIC_VIEW'").collect()

        return json.dumps({
            "status": "SUCCESS",
            "message": "Semantic view DYNAMIC_ANALYTICS created for PI streams",
            "details": {"semantic_view": "DATA_VIEWS.DYNAMIC_ANALYTICS"}
        })

    except Exception as e:
        err_msg = str(e).replace("'", "''")
        session.sql(f"MERGE INTO CONFIG.INIT_PROGRESS T USING (SELECT 'GENERATE_SEMANTIC_VIEW' AS STEP_NAME) S ON T.STEP_NAME = S.STEP_NAME WHEN MATCHED THEN UPDATE SET STATUS='ERROR', COMPLETED_AT=CURRENT_TIMESTAMP(), DETAILS='{err_msg}' WHEN NOT MATCHED THEN INSERT (STEP_NAME, STATUS, COMPLETED_AT, DETAILS) VALUES (S.STEP_NAME, 'ERROR', CURRENT_TIMESTAMP(), '{err_msg}')").collect()
        return json.dumps({"status": "ERROR", "message": str(e), "details": {}})
$$;

GRANT USAGE ON PROCEDURE CORE.GENERATE_SEMANTIC_VIEW() TO APPLICATION ROLE APP_PUBLIC;

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
    lines.append("PI Stream Data Schema (AVEVA CONNECT Data Historian)")
    lines.append("Each stream has: Timestamp, Value, IsQuestionable, IsSubstituted, IsAnnotated, SystemStateCode, DigitalStateName")
    lines.append("")
    lines.append("UNIFIED VIEW: DATA_VIEWS.PI_STREAMS_UNIFIED")
    lines.append("Columns: TS (timestamp), NUMERIC_VALUE (double), STRING_VALUE (varchar), IS_QUESTIONABLE (boolean), IS_SUBSTITUTED (boolean), IS_ANNOTATED (boolean), SYSTEM_STATE_CODE (number), DIGITAL_STATE_NAME (varchar), ASSET_NAME (varchar), SIGNAL_NAME (varchar), DOMAIN_CATEGORY (varchar)")
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
        session.sql("DELETE FROM CONFIG.INIT_LOCK").collect()

        # Drop existing views in DATA_VIEWS (except system ones we'll recreate)
        try:
            existing_views = session.sql("SHOW VIEWS IN SCHEMA DATA_VIEWS").collect()
            for v in existing_views:
                vn = v["name"].replace('"', '""')
                session.sql(f'DROP VIEW IF EXISTS DATA_VIEWS."{vn}"').collect()
        except:
            pass

        # Drop semantic view
        try:
            session.sql("DROP SEMANTIC VIEW IF EXISTS DATA_VIEWS.DYNAMIC_ANALYTICS").collect()
        except:
            pass

        # Re-run initialization pipeline
        r1 = session.sql("CALL CORE.INITIALIZE_VIEWS()").collect()
        results["initialize_views"] = json.loads(r1[0][0]) if len(r1) > 0 else {"status": "ERROR", "message": "No result"}

        r2 = session.sql("CALL CORE.DISCOVER_METADATA()").collect()
        results["discover_metadata"] = json.loads(r2[0][0]) if len(r2) > 0 else {"status": "ERROR", "message": "No result"}

        r3 = session.sql("CALL CORE.CREATE_UNIFIED_VIEW()").collect()
        results["create_unified_view"] = json.loads(r3[0][0]) if len(r3) > 0 else {"status": "ERROR", "message": "No result"}

        r4 = session.sql("CALL CORE.GENERATE_SEMANTIC_VIEW()").collect()
        results["generate_semantic_view"] = json.loads(r4[0][0]) if len(r4) > 0 else {"status": "ERROR", "message": "No result"}

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

    # 1. SHARED_CONTENT + PROXY_VIEWS count vs stored count
    try:
        shared_count = 0
        try:
            sc_rows = session.sql("SHOW VIEWS IN SCHEMA SHARED_CONTENT").collect()
            shared_count += len(sc_rows)
        except:
            pass
        try:
            pv_rows = session.sql("SHOW VIEWS IN SCHEMA PROXY_VIEWS").collect()
            shared_count += len(pv_rows)
        except:
            pass
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

    # 4. PI_STREAMS_UNIFIED exists and is queryable
    try:
        session.sql("SELECT 1 FROM DATA_VIEWS.PI_STREAMS_UNIFIED LIMIT 1").collect()
        checks["unified_view"] = {"status": "OK"}
    except Exception as e:
        checks["unified_view"] = {"status": "ERROR", "error": str(e)}

    # 5. DYNAMIC_ANALYTICS semantic view exists
    try:
        session.sql("DESCRIBE SEMANTIC VIEW DATA_VIEWS.DYNAMIC_ANALYTICS").collect()
        checks["semantic_view"] = {"status": "OK"}
    except Exception as e:
        checks["semantic_view"] = {"status": "ERROR", "error": str(e)}

    all_ok = all(c.get("status") == "OK" for c in checks.values())
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
    expected_steps = ["INITIALIZE_VIEWS", "DISCOVER_METADATA", "CREATE_UNIFIED_VIEW", "GENERATE_SEMANTIC_VIEW"]

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

        # Check lock
        lock_rows = session.sql("SELECT LOCK_ID, ACQUIRED_AT, ACQUIRED_BY FROM CONFIG.INIT_LOCK").collect()
        locked = len(lock_rows) > 0

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

def run(session, p_asset_name, p_signal_name, p_lookback_days):
    try:
        lookback = p_lookback_days if p_lookback_days and p_lookback_days > 0 else 7

        # If signal not specified, auto-detect best numeric candidates
        if not p_signal_name or p_signal_name == '' or p_signal_name == 'AUTO':
            safe_asset = p_asset_name.replace("'", "''") if p_asset_name else ""
            where_asset = f"AND ASSET_NAME = '{safe_asset}'" if p_asset_name and p_asset_name != 'ALL' else ""
            candidates = session.sql(f"""
                SELECT ASSET_NAME, SIGNAL_NAME,
                       COUNT(*) AS CNT,
                       STDDEV(NUMERIC_VALUE) AS STDDEV_VAL,
                       AVG(NUMERIC_VALUE) AS AVG_VAL
                FROM DATA_VIEWS.PI_STREAMS_UNIFIED
                WHERE NUMERIC_VALUE IS NOT NULL
                  AND TS >= DATEADD('day', -{lookback}, CURRENT_TIMESTAMP())
                  {where_asset}
                GROUP BY ASSET_NAME, SIGNAL_NAME
                HAVING CNT >= 50 AND STDDEV_VAL > 0
                ORDER BY STDDEV_VAL DESC
                LIMIT 10
            """).collect()

            if len(candidates) == 0:
                return json.dumps({
                    "status": "ERROR",
                    "message": "No suitable numeric signals found for anomaly detection",
                    "details": {"lookback_days": lookback}
                })

            # Return the auto-detected candidates
            signal_list = [{"asset": r["ASSET_NAME"], "signal": r["SIGNAL_NAME"],
                           "readings": r["CNT"], "stddev": float(r["STDDEV_VAL"]) if r["STDDEV_VAL"] else 0,
                           "avg": float(r["AVG_VAL"]) if r["AVG_VAL"] else 0}
                          for r in candidates]
            return json.dumps({
                "status": "CANDIDATES",
                "message": f"Found {len(signal_list)} signals suitable for anomaly detection",
                "details": {"candidates": signal_list, "lookback_days": lookback}
            })

        # Run anomaly detection on specified signal using rolling z-score (no ML classes in Native Apps)
        safe_asset = p_asset_name.replace("'", "''")
        safe_signal = p_signal_name.replace("'", "''")

        # Count available data
        row_count = session.sql(f"""
            SELECT COUNT(*) AS CNT FROM DATA_VIEWS.PI_STREAMS_UNIFIED
            WHERE ASSET_NAME = '{safe_asset}' AND SIGNAL_NAME = '{safe_signal}'
              AND NUMERIC_VALUE IS NOT NULL
              AND TS >= DATEADD('day', -{lookback}, CURRENT_TIMESTAMP())
        """).collect()
        data_count = row_count[0]["CNT"] if len(row_count) > 0 else 0
        if data_count < 12:
            return json.dumps({
                "status": "ERROR",
                "message": f"Insufficient data for anomaly detection ({data_count} rows, need >= 12)",
                "details": {"data_count": data_count}
            })

        # Detect anomalies using rolling z-score with 24-point window
        # Points with |z-score| > 3 are flagged as anomalies
        session.sql(f"""
            DELETE FROM CONFIG.ANOMALY_RESULTS
            WHERE ASSET_NAME = '{safe_asset}' AND SIGNAL_NAME = '{safe_signal}' AND MODEL_TYPE = 'ANOMALY'
        """).collect()

        session.sql(f"""
            INSERT INTO CONFIG.ANOMALY_RESULTS
                (ASSET_NAME, SIGNAL_NAME, DOMAIN_CATEGORY, TS, NUMERIC_VALUE,
                 IS_ANOMALY, ANOMALY_SCORE, FORECAST_VALUE, LOWER_BOUND, UPPER_BOUND, MODEL_TYPE)
            WITH base AS (
                SELECT TS, NUMERIC_VALUE AS VALUE,
                       AVG(NUMERIC_VALUE) OVER (ORDER BY TS ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS ROLLING_AVG,
                       STDDEV(NUMERIC_VALUE) OVER (ORDER BY TS ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS ROLLING_STD
                FROM DATA_VIEWS.PI_STREAMS_UNIFIED
                WHERE ASSET_NAME = '{safe_asset}' AND SIGNAL_NAME = '{safe_signal}'
                  AND NUMERIC_VALUE IS NOT NULL
                  AND TS >= DATEADD('day', -{lookback}, CURRENT_TIMESTAMP())
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

def run(session, p_asset_name, p_signal_name, p_horizon_days):
    try:
        horizon = p_horizon_days if p_horizon_days and p_horizon_days > 0 else 7
        safe_asset = p_asset_name.replace("'", "''")
        safe_signal = p_signal_name.replace("'", "''")

        # Count available data and determine frequency
        stats = session.sql(f"""
            SELECT COUNT(*) AS CNT,
                   MIN(TS) AS MIN_TS, MAX(TS) AS MAX_TS,
                   AVG(NUMERIC_VALUE) AS AVG_VAL,
                   STDDEV(NUMERIC_VALUE) AS STD_VAL
            FROM DATA_VIEWS.PI_STREAMS_UNIFIED
            WHERE ASSET_NAME = '{safe_asset}' AND SIGNAL_NAME = '{safe_signal}'
              AND NUMERIC_VALUE IS NOT NULL
        """).collect()
        data_count = stats[0]["CNT"] if len(stats) > 0 else 0
        if data_count < 12:
            return json.dumps({
                "status": "ERROR",
                "message": f"Insufficient data for forecasting ({data_count} rows, need >= 12)",
                "details": {"data_count": data_count}
            })

        std_val = float(stats[0]["STD_VAL"]) if stats[0]["STD_VAL"] else 1

        # Determine data frequency
        freq_rows = session.sql(f"""
            SELECT MEDIAN(INTERVAL_MIN) AS MED_INTERVAL FROM (
                SELECT TIMESTAMPDIFF('minute', LAG(TS) OVER (ORDER BY TS), TS) AS INTERVAL_MIN
                FROM DATA_VIEWS.PI_STREAMS_UNIFIED
                WHERE ASSET_NAME = '{safe_asset}' AND SIGNAL_NAME = '{safe_signal}'
                  AND NUMERIC_VALUE IS NOT NULL
                ORDER BY TS DESC LIMIT 200
            ) WHERE INTERVAL_MIN > 0
        """).collect()
        avg_interval_min = float(freq_rows[0]["MED_INTERVAL"]) if freq_rows and freq_rows[0]["MED_INTERVAL"] else 60
        steps_per_day = max(int(1440 / avg_interval_min), 1)
        forecast_steps = min(steps_per_day * horizon, 5000)

        # Fetch last 200 data points for training (ordered chronologically)
        training_rows = session.sql(f"""
            SELECT TS, NUMERIC_VALUE AS VALUE FROM (
                SELECT TS, NUMERIC_VALUE, ROW_NUMBER() OVER (ORDER BY TS DESC) AS RN
                FROM DATA_VIEWS.PI_STREAMS_UNIFIED
                WHERE ASSET_NAME = '{safe_asset}' AND SIGNAL_NAME = '{safe_signal}'
                  AND NUMERIC_VALUE IS NOT NULL
            ) WHERE RN <= 200
            ORDER BY TS ASC
        """).collect()

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
