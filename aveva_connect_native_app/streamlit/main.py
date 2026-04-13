###############################################################################
# AVEVA CONNECT Analytics — Native App Streamlit UI (Production)
# Runs inside AVEVA_CONNECT_APP via Snowpark session.
###############################################################################

import streamlit as st
import pandas as pd
import json
import _snowflake
from snowflake.snowpark.context import get_active_session
import snowflake.permissions as permissions

# ---------------------------------------------------------------------------
# Page config & CSS
# ---------------------------------------------------------------------------
st.set_page_config(page_title="AVEVA CONNECT Analytics", layout="wide", page_icon="⚡")

st.markdown("""
<style>
:root { --primary: #00D4AA; --accent: #6366F1; }
.main-header {
    background: linear-gradient(135deg, #0F172A 0%, #1E293B 50%, #0F172A 100%);
    padding: 2rem; border-radius: 16px; margin-bottom: 2rem;
    border: 1px solid rgba(0, 212, 170, 0.2); text-align: center;
}
.main-header h1 {
    background: linear-gradient(90deg, #00D4AA, #6366F1, #00D4AA);
    background-size: 200% auto; -webkit-background-clip: text;
    -webkit-text-fill-color: transparent; font-size: 2.5rem; font-weight: 800; margin: 0;
}
.main-header p { color: #CBD5E1; font-size: 1.1rem; margin-top: 0.5rem; }
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1E293B 0%, #334155 100%);
    border: 1px solid rgba(0, 212, 170, 0.15); border-radius: 12px; padding: 1rem;
}
[data-testid="stMetricLabel"] { color: #E2E8F0 !important; font-weight: 600; font-size: 0.9rem; }
[data-testid="stMetricValue"] { color: #00D4AA !important; font-weight: 700; }
.stButton > button {
    background: linear-gradient(135deg, #00D4AA 0%, #00A88A 100%);
    color: #0F172A; border: none; border-radius: 8px; font-weight: 600;
}
.domain-card {
    background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
    border-radius: 12px; padding: 1rem; margin: 0.5rem 0;
    border-left: 4px solid #00D4AA;
}
.privacy-box {
    background: linear-gradient(135deg, #1a472a 0%, #2d5a3d 100%);
    border: 1px solid #4ade80; border-radius: 12px; padding: 1rem; margin: 1rem 0;
}
.privacy-box h4 { color: #4ade80; margin: 0 0 0.5rem 0; }
.privacy-box p { color: #bbf7d0; margin: 0; font-size: 0.9rem; }
</style>
""", unsafe_allow_html=True)

session = get_active_session()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOMAIN_ICONS = {
    "wind_energy": "Wind", "rotating_machinery": "Rotating",
    "production_quality": "Production", "packaging": "Packaging", "general": "General",
}
DOMAIN_LABELS = {
    "wind_energy": "Wind Energy", "rotating_machinery": "Rotating Machinery",
    "production_quality": "Production & Quality", "packaging": "Packaging Line",
    "general": "General",
}
MAX_QUESTION_LENGTH = 1000

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_current_database():
    try:
        result = session.sql("SELECT CURRENT_DATABASE()").collect()
        return result[0][0] if result else None
    except Exception:
        return None


def db_prefix():
    db = get_current_database()
    return f"{db}." if db else ""


def safe_sql_string(value: str) -> str:
    """Escape single quotes for safe SQL interpolation."""
    return value.replace("'", "''")


def log_event(event_type, event_data=None, user_query=None):
    try:
        data_json = json.dumps(event_data) if event_data else "NULL"
        if user_query:
            escaped = safe_sql_string(user_query[:MAX_QUESTION_LENGTH])
            query_str = f"'{escaped}'"
        else:
            query_str = "NULL"
        session.sql(f"""
            CALL CORE.LOG_EVENT(
                '{safe_sql_string(event_type)}',
                PARSE_JSON('{safe_sql_string(data_json)}'),
                {query_str}
            )
        """).collect()
    except Exception:
        pass


def run_sql(sql: str):
    """Execute SQL and return pandas DataFrame, or empty DataFrame on error."""
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600)
def get_stream_metadata(_session):
    try:
        return session.sql(
            "SELECT * FROM CONFIG.STREAM_METADATA ORDER BY DOMAIN_CATEGORY, ASSET_NAME, SIGNAL_NAME"
        ).to_pandas()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600)
def get_asset_hierarchy(_session):
    try:
        return session.sql(
            "SELECT * FROM CONFIG.ASSET_HIERARCHY ORDER BY DOMAIN_CATEGORY, ASSET_PATH"
        ).to_pandas()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600)
def get_domain_distribution(_session):
    try:
        result = session.sql(
            "SELECT VALUE FROM CONFIG.APP_STATE WHERE KEY = 'DOMAIN_DISTRIBUTION'"
        ).collect()
        if result:
            return json.loads(result[0][0])
    except Exception:
        pass
    return {}


@st.cache_data(ttl=600)
def get_schema_info(_session):
    try:
        result = session.sql("CALL CORE.GET_SCHEMA_INFO()").collect()
        return result[0][0] if result else ""
    except Exception:
        return ""


@st.cache_data(ttl=3600)
def generate_sample_questions(_session):
    schema_info = get_schema_info(_session)
    prompt = (
        "Based on this AVEVA PI data historian schema, generate 6 analytical questions "
        "a user might ask. Focus on: asset comparisons, signal trends, quality flags, "
        "domain-specific insights.\n\n"
        f"{schema_info}\n\n"
        "Return ONLY a JSON array of question strings."
    )
    try:
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{safe_sql_string(prompt)}')
        """).collect()
        text = result[0][0] if result else "[]"
        start = text.find("[")
        end = text.rfind("]") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])[:6]
    except Exception:
        pass
    return [
        "What is the average wind speed by turbine?",
        "Which asset has the highest bearing temperature?",
        "Show me the total power generation over time",
        "How many data quality issues (questionable values) exist?",
        "Compare pump discharge pressure across all rotating machinery",
        "What is the latest reading for each signal?",
    ]

# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

def check_initialized():
    try:
        result = session.sql(
            "SELECT VALUE FROM CONFIG.APP_STATE WHERE KEY = 'VIEWS_INITIALIZED'"
        ).collect()
        return result and result[0][0] == "TRUE"
    except Exception:
        return False



def run_initialization():
    """Run full init pipeline with progress tracking and retry logic."""
    steps = [
        ("Initializing data views...", "CALL CORE.INITIALIZE_VIEWS()"),
        ("Discovering stream metadata...", "CALL CORE.DISCOVER_METADATA()"),
        ("Creating unified view...", "CALL CORE.CREATE_UNIFIED_VIEW()"),
        ("Generating semantic view...", "CALL CORE.GENERATE_SEMANTIC_VIEW()"),
    ]

    progress_bar = st.progress(0)
    status_text = st.empty()

    for i, (label, sql) in enumerate(steps):
        status_text.text(label)
        success = False
        for attempt in range(3):
            try:
                result = session.sql(sql).collect()
                response = json.loads(result[0][0]) if result else {}
                if response.get("status") == "SUCCESS":
                    success = True
                    break
                elif attempt == 2:
                    st.error(
                        f"Failed after 3 attempts: {response.get('message', 'Unknown error')}"
                    )
                    return False
            except Exception as e:
                if attempt == 2:
                    st.error(f"Step failed: {e}")
                    return False
        if not success:
            return False
        progress_bar.progress((i + 1) / len(steps))

    status_text.text("Initialization complete!")
    return True


def show_init_page():
    """Dedicated initialization page shown when app is not yet set up."""
    st.markdown("""
    <div class="main-header">
        <h1>AVEVA CONNECT Analytics</h1>
        <p>First-time setup &mdash; initializing PI stream discovery</p>
    </div>
    """, unsafe_allow_html=True)

    st.info(
        "This app needs to discover and configure your AVEVA CONNECT PI data streams. "
        "This runs once and typically takes 1-2 minutes."
    )

    if st.button("Start Initialization", type="primary"):
        ok = run_initialization()
        if ok:
            st.success("Setup complete. Reloading...")
            st.rerun()
        else:
            if st.button("Retry Initialization"):
                st.rerun()
    st.stop()

# ---------------------------------------------------------------------------
# External data permission gate
# ---------------------------------------------------------------------------

def check_external_data():
    try:
        return permissions.is_external_data_enabled()
    except Exception:
        return False


if not check_external_data():
    st.warning("External Data Access Required")
    st.markdown(
        "This app uses AVEVA CONNECT PI data historian streams. "
        "Please grant permission to access the shared data."
    )
    if st.button("Grant External Data Access", type="primary"):
        permissions.request_external_data()
    st.stop()

# ---------------------------------------------------------------------------
# Cortex AI privilege gate
# ---------------------------------------------------------------------------

missing_privs = permissions.get_missing_account_privileges(["IMPORTED PRIVILEGES ON SNOWFLAKE DB"])
if missing_privs:
    st.warning("Cortex AI Access Required")
    st.markdown(
        "This app uses Snowflake Cortex AI for analytics, anomaly detection, and natural language queries. "
        "Please grant access to the SNOWFLAKE database."
    )
    if st.button("Grant Cortex AI Access", type="primary"):
        permissions.request_account_privileges(["IMPORTED PRIVILEGES ON SNOWFLAKE DB"])
    st.stop()

# ---------------------------------------------------------------------------
# Initialization gate
# ---------------------------------------------------------------------------

if not check_initialized():
    show_init_page()

# ---------------------------------------------------------------------------
# Session logging
# ---------------------------------------------------------------------------

if "session_logged" not in st.session_state:
    log_event("SESSION_START", {"page": "home"})
    st.session_state.session_logged = True

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown("""
<div class="main-header">
    <h1>AVEVA CONNECT Analytics</h1>
    <p>PI Data Historian &bull; Dynamic Stream Discovery &bull; Snowflake Cortex AI</p>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Load shared data
# ---------------------------------------------------------------------------

metadata_df = get_stream_metadata(session)
domains = get_domain_distribution(session)

# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

page = st.sidebar.radio("Navigation", [
    "Dashboard",
    "Asset Explorer",
    "Talk to Your Data",
    "Stream Browser",
    "Data Statistics",
    "Anomaly Detection",
    "Forecasting",
    "Data Dictionary",
    "Privacy & Telemetry",
])

if "last_page" not in st.session_state or st.session_state.last_page != page:
    log_event("PAGE_VIEW", {"page": page})
    st.session_state.last_page = page

st.sidebar.divider()
st.sidebar.success("PI Streams: Connected")
st.sidebar.caption(f"Streams: {len(metadata_df)} | Domains: {len(domains)}")
for d, c in domains.items():
    label = DOMAIN_LABELS.get(d, d)
    st.sidebar.caption(f"{label}: {c} streams")

# Health status in sidebar (cached, visible on all pages)
@st.cache_data(ttl=300)
def get_health_status(_session):
    try:
        result = _session.sql("CALL CORE.HEALTH_CHECK()").collect()
        return json.loads(result[0][0]) if result else {"status": "UNKNOWN"}
    except Exception:
        return {"status": "UNKNOWN"}

health = get_health_status(session)
st.sidebar.divider()
if health.get("status") == "HEALTHY":
    st.sidebar.success("Health: All checks passed")
elif health.get("status") == "UNKNOWN":
    st.sidebar.warning("Health: Unable to check")
else:
    failed = [k for k, v in health.get("details", {}).items() if v.get("status") != "OK"]
    st.sidebar.error(f"Health: {', '.join(failed) if failed else 'Issues detected'}")

# ===========================================================================
# PAGE: Dashboard
# ===========================================================================

if page == "Dashboard":
    st.header("Dashboard")
    st.caption("Auto-generated from discovered AVEVA CONNECT PI streams")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Streams", len(metadata_df))
    c2.metric("Assets", metadata_df["ASSET_NAME"].nunique() if not metadata_df.empty else 0)
    c3.metric("Domains", len(domains))
    c4.metric("Signal Types", metadata_df["SIGNAL_NAME"].nunique() if not metadata_df.empty else 0)

    st.divider()

    for domain, count in sorted(domains.items(), key=lambda x: -x[1]):
        label = DOMAIN_LABELS.get(domain, domain)
        with st.expander(f"{label} ({count} streams)", expanded=(count == max(domains.values()))):
            domain_streams = (
                metadata_df[metadata_df["DOMAIN_CATEGORY"] == domain]
                if not metadata_df.empty
                else pd.DataFrame()
            )
            if not domain_streams.empty:
                assets = domain_streams["ASSET_NAME"].unique()
                acols = st.columns(min(len(assets), 4))
                for i, asset in enumerate(assets[:4]):
                    with acols[i]:
                        n = len(domain_streams[domain_streams["ASSET_NAME"] == asset])
                        st.metric(asset, f"{n} signals")

                try:
                    sample_asset = safe_sql_string(assets[0])
                    sample_signal = safe_sql_string(
                        domain_streams[domain_streams["ASSET_NAME"] == assets[0]]["SIGNAL_NAME"].iloc[0]
                    )
                    chart_df = run_sql(f"""
                        SELECT DATE_TRUNC('hour', TS) AS HOUR, AVG(NUMERIC_VALUE) AS AVG_VALUE
                        FROM {db_prefix()}DATA_VIEWS.PI_STREAMS_UNIFIED
                        WHERE ASSET_NAME = '{sample_asset}' AND SIGNAL_NAME = '{sample_signal}'
                          AND NUMERIC_VALUE IS NOT NULL
                        GROUP BY 1 ORDER BY 1 LIMIT 48
                    """)
                    if not chart_df.empty and len(chart_df) > 1:
                        st.caption(f"Trend: {assets[0]} - {domain_streams[domain_streams['ASSET_NAME'] == assets[0]]['SIGNAL_NAME'].iloc[0]}")
                        st.line_chart(chart_df.set_index("HOUR")["AVG_VALUE"])
                except Exception:
                    pass

    if st.button("Regenerate Dashboard"):
        st.cache_data.clear()
        st.rerun()

# ===========================================================================
# PAGE: Asset Explorer
# ===========================================================================

elif page == "Asset Explorer":
    st.header("Asset Hierarchy Explorer")
    st.markdown("Navigate the AVEVA CONNECT PI asset tree")

    hierarchy_df = get_asset_hierarchy(session)

    if not hierarchy_df.empty:
        domain_filter = st.multiselect(
            "Filter by Domain",
            options=list(domains.keys()),
            default=list(domains.keys()),
            format_func=lambda x: DOMAIN_LABELS.get(x, x),
        )
        filtered = hierarchy_df[hierarchy_df["DOMAIN_CATEGORY"].isin(domain_filter)]

        for _, row in filtered.iterrows():
            indent = "-> " * int(row.get("LEVEL_DEPTH", 1) - 1)
            stream_count = int(row.get("STREAM_COUNT", 0))

            with st.expander(f"{indent}{row['ASSET_NAME']} ({stream_count} signals)"):
                asset_streams = (
                    metadata_df[metadata_df["ASSET_NAME"] == row["ASSET_PATH"]]
                    if not metadata_df.empty
                    else pd.DataFrame()
                )
                if not asset_streams.empty:
                    st.dataframe(
                        asset_streams[["SIGNAL_NAME", "DATA_TYPE", "DOMAIN_CATEGORY"]],
                        use_container_width=True,
                        hide_index=True,
                    )
                    selected_signal = st.selectbox(
                        "View signal trend",
                        asset_streams["SIGNAL_NAME"].tolist(),
                        key=f"signal_{row['ASSET_PATH']}",
                    )
                    if st.button("Show Trend", key=f"trend_{row['ASSET_PATH']}"):
                        safe_asset = safe_sql_string(row["ASSET_PATH"])
                        safe_signal = safe_sql_string(selected_signal)
                        try:
                            trend_df = run_sql(f"""
                                SELECT TS, NUMERIC_VALUE
                                FROM {db_prefix()}DATA_VIEWS.PI_STREAMS_UNIFIED
                                WHERE ASSET_NAME = '{safe_asset}'
                                  AND SIGNAL_NAME = '{safe_signal}'
                                  AND NUMERIC_VALUE IS NOT NULL
                                ORDER BY TS LIMIT 500
                            """)
                            if not trend_df.empty:
                                st.line_chart(trend_df.set_index("TS")["NUMERIC_VALUE"])
                                mc1, mc2, mc3, mc4 = st.columns(4)
                                mc1.metric("Min", f"{trend_df['NUMERIC_VALUE'].min():.2f}")
                                mc2.metric("Max", f"{trend_df['NUMERIC_VALUE'].max():.2f}")
                                mc3.metric("Mean", f"{trend_df['NUMERIC_VALUE'].mean():.2f}")
                                mc4.metric("Std Dev", f"{trend_df['NUMERIC_VALUE'].std():.2f}")
                        except Exception as e:
                            st.error(f"Error loading trend: {e}")
    else:
        st.warning("No asset hierarchy data found")

# ===========================================================================
# PAGE: Talk to Your Data (Cortex AI)
# ===========================================================================

elif page == "Talk to Your Data":
    st.header("Talk to Your Data")
    st.markdown("Ask questions about your AVEVA PI data using Cortex AI")

    # --- Cortex Analyst (grounded SQL via semantic view) ---
    def call_cortex_analyst(question: str) -> dict:
        """Call Cortex Analyst via REST API for grounded SQL generation."""
        db = get_current_database()
        semantic_view_fqn = f"{db}.DATA_VIEWS.DYNAMIC_ANALYTICS" if db else "DATA_VIEWS.DYNAMIC_ANALYTICS"
        try:
            body = {
                "messages": [
                    {"role": "user", "content": [{"type": "text", "text": question}]}
                ],
                "semantic_view": semantic_view_fqn,
            }
            resp = _snowflake.send_snow_api_request(
                "POST",
                "/api/v2/cortex/analyst/message",
                {},
                {},
                body,
                {},
                30000,
            )
            if resp["status"] == 200:
                return json.loads(resp["content"])
            else:
                detail = ""
                try:
                    detail = resp.get("content", "")
                    if isinstance(detail, str) and len(detail) > 500:
                        detail = detail[:500]
                except Exception:
                    pass
                return {"error": f"Analyst returned status {resp['status']}: {detail}"}
        except Exception as e:
            return {"error": str(e)}

    # --- Sample questions ---
    st.subheader("Sample Questions")
    sample_questions = generate_sample_questions(session)
    cols = st.columns(3)
    for i, q in enumerate(sample_questions):
        with cols[i % 3]:
            if st.button(q, key=f"sample_{i}"):
                st.session_state.current_question = q

    st.divider()

    user_question = st.text_input(
        "Ask a question about your PI data:",
        value=st.session_state.get("current_question", ""),
        max_chars=MAX_QUESTION_LENGTH,
        placeholder="e.g., What is the average wind speed for Turbine 1?",
    )

    if st.button("Ask Cortex AI", type="primary") and user_question:
        question = user_question[:MAX_QUESTION_LENGTH]
        log_event("CORTEX_QUERY", {"question": question[:100]}, user_query=question)

        with st.spinner("Analyzing your question..."):
            # Try Cortex Analyst first
            analyst_resp = call_cortex_analyst(question)

            sql = None
            source = "analyst"

            if "error" not in analyst_resp:
                # Parse Analyst response for SQL
                # Response can have "message" (singular) or "messages" (plural)
                try:
                    msgs = analyst_resp.get("messages", [])
                    if not msgs and "message" in analyst_resp:
                        msgs = [analyst_resp["message"]]
                    for msg in msgs:
                        for content in msg.get("content", []):
                            if content.get("type") == "sql":
                                sql = content["statement"]
                                break
                        if sql:
                            break
                except Exception:
                    pass

            if not sql:
                error_detail = analyst_resp.get("error", "No SQL generated by Cortex Analyst")
                st.error(f"Cortex Analyst error: {error_detail}")
                st.info("Ensure the semantic view DYNAMIC_ANALYTICS exists. Try re-running initialization.")

            if sql:
                with st.expander("Generated SQL", expanded=False):
                    st.code(sql, language="sql")
                try:
                    result_df = session.sql(sql).to_pandas()
                    st.dataframe(result_df, use_container_width=True)

                    if len(result_df) > 1 and len(result_df.columns) >= 2:
                        numeric_cols = result_df.select_dtypes(include=["number"]).columns
                        if len(numeric_cols) > 0:
                            st.bar_chart(result_df.set_index(result_df.columns[0])[numeric_cols])
                except Exception as e:
                    st.error(f"Query execution error: {e}")

# ===========================================================================
# PAGE: Stream Browser
# ===========================================================================

elif page == "Stream Browser":
    st.header("PI Stream Browser")
    st.markdown("Browse all discovered PI data streams")

    if not metadata_df.empty:
        col1, col2 = st.columns(2)
        with col1:
            domain_filter = st.selectbox(
                "Domain",
                ["All"] + list(domains.keys()),
                format_func=lambda x: DOMAIN_LABELS.get(x, x) if x != "All" else "All Domains",
            )
        with col2:
            asset_options = metadata_df["ASSET_NAME"].unique().tolist()
            if domain_filter != "All":
                asset_options = metadata_df[metadata_df["DOMAIN_CATEGORY"] == domain_filter]["ASSET_NAME"].unique().tolist()
            asset_filter = st.selectbox("Asset", ["All"] + asset_options)

        filtered = metadata_df.copy()
        if domain_filter != "All":
            filtered = filtered[filtered["DOMAIN_CATEGORY"] == domain_filter]
        if asset_filter != "All":
            filtered = filtered[filtered["ASSET_NAME"] == asset_filter]

        st.success(f"Showing {len(filtered)} streams")
        display_cols = [c for c in ["STREAM_VIEW_NAME", "ASSET_NAME", "SIGNAL_NAME", "DOMAIN_CATEGORY", "DATA_TYPE"] if c in filtered.columns]
        st.dataframe(filtered[display_cols], use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("Stream Data Preview")

        selected_stream = st.selectbox("Select a stream to preview", filtered["STREAM_VIEW_NAME"].tolist())
        sample_size = st.slider("Sample size", 10, 500, 100)

        if st.button("Load Data", type="primary"):
            try:
                safe_view = selected_stream.replace('"', '""')
                df = session.sql(
                    f'SELECT * FROM {db_prefix()}PROXY_VIEWS."{safe_view}" LIMIT {sample_size}'
                ).to_pandas()
                st.success(f"Loaded {len(df)} rows")
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Error loading stream: {e}")
    else:
        st.warning("No stream metadata found")

# ===========================================================================
# PAGE: Data Statistics
# ===========================================================================

elif page == "Data Statistics":
    st.header("PI Stream Statistics")
    st.markdown("Comprehensive statistics across all PI data streams")

    pfx = db_prefix()

    try:
        overview = run_sql(f"""
            SELECT
                COUNT(*) AS TOTAL_RECORDS,
                COUNT(DISTINCT ASSET_NAME) AS ASSETS,
                COUNT(DISTINCT SIGNAL_NAME) AS SIGNALS,
                COUNT(DISTINCT DOMAIN_CATEGORY) AS DOMAINS,
                MIN(TS) AS EARLIEST,
                MAX(TS) AS LATEST,
                SUM(CASE WHEN IS_QUESTIONABLE THEN 1 ELSE 0 END) AS QUESTIONABLE_COUNT,
                SUM(CASE WHEN IS_SUBSTITUTED THEN 1 ELSE 0 END) AS SUBSTITUTED_COUNT
            FROM {pfx}DATA_VIEWS.PI_STREAMS_UNIFIED
        """)

        if not overview.empty:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Records", f"{overview['TOTAL_RECORDS'].iloc[0]:,}")
            c2.metric("Assets", overview["ASSETS"].iloc[0])
            c3.metric("Signals", overview["SIGNALS"].iloc[0])
            c4.metric("Domains", overview["DOMAINS"].iloc[0])

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Earliest", str(overview["EARLIEST"].iloc[0])[:19])
            c2.metric("Latest", str(overview["LATEST"].iloc[0])[:19])
            c3.metric("Questionable", f"{overview['QUESTIONABLE_COUNT'].iloc[0]:,}")
            c4.metric("Substituted", f"{overview['SUBSTITUTED_COUNT'].iloc[0]:,}")

        st.divider()
        st.subheader("Records by Domain")
        domain_stats = run_sql(f"""
            SELECT DOMAIN_CATEGORY, COUNT(*) AS RECORDS,
                   COUNT(DISTINCT ASSET_NAME) AS ASSETS,
                   AVG(NUMERIC_VALUE) AS AVG_VALUE
            FROM {pfx}DATA_VIEWS.PI_STREAMS_UNIFIED
            WHERE NUMERIC_VALUE IS NOT NULL
            GROUP BY DOMAIN_CATEGORY ORDER BY RECORDS DESC
        """)
        if not domain_stats.empty:
            st.bar_chart(domain_stats.set_index("DOMAIN_CATEGORY")["RECORDS"])
            st.dataframe(domain_stats, use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("Signal Statistics by Asset")
        signal_stats = run_sql(f"""
            SELECT ASSET_NAME, SIGNAL_NAME,
                   COUNT(*) AS READINGS,
                   ROUND(AVG(NUMERIC_VALUE), 2) AS AVG_VALUE,
                   ROUND(MIN(NUMERIC_VALUE), 2) AS MIN_VALUE,
                   ROUND(MAX(NUMERIC_VALUE), 2) AS MAX_VALUE,
                   ROUND(STDDEV(NUMERIC_VALUE), 2) AS STD_DEV
            FROM {pfx}DATA_VIEWS.PI_STREAMS_UNIFIED
            WHERE NUMERIC_VALUE IS NOT NULL
            GROUP BY ASSET_NAME, SIGNAL_NAME
            ORDER BY ASSET_NAME, SIGNAL_NAME
            LIMIT 200
        """)
        if not signal_stats.empty:
            st.dataframe(signal_stats, use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("Data Quality Overview")
        quality_df = run_sql(f"""
            SELECT ASSET_NAME,
                   COUNT(*) AS TOTAL,
                   SUM(CASE WHEN IS_QUESTIONABLE THEN 1 ELSE 0 END) AS QUESTIONABLE,
                   SUM(CASE WHEN IS_SUBSTITUTED THEN 1 ELSE 0 END) AS SUBSTITUTED,
                   SUM(CASE WHEN IS_ANNOTATED THEN 1 ELSE 0 END) AS ANNOTATED,
                   ROUND(100.0 * SUM(CASE WHEN IS_QUESTIONABLE THEN 1 ELSE 0 END) / COUNT(*), 2) AS PCT_QUESTIONABLE
            FROM {pfx}DATA_VIEWS.PI_STREAMS_UNIFIED
            GROUP BY ASSET_NAME ORDER BY PCT_QUESTIONABLE DESC
        """)
        if not quality_df.empty:
            st.dataframe(quality_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Error loading statistics: {e}")

# ===========================================================================
# PAGE: Anomaly Detection
# ===========================================================================

elif page == "Anomaly Detection":
    st.header("Anomaly Detection")
    st.markdown("Automatically detect anomalies in PI sensor streams using rolling z-score analysis")

    pfx = db_prefix()

    # Auto-discover suitable numeric signals
    if "anomaly_candidates" not in st.session_state:
        st.session_state.anomaly_candidates = None

    tab_auto, tab_manual = st.tabs(["Auto-Detect", "Manual Selection"])

    with tab_auto:
        st.markdown("**Auto-detect** scans all numeric signals and identifies those with sufficient "
                    "variance and data density for meaningful anomaly detection.")
        col1, col2 = st.columns(2)
        with col1:
            auto_asset = st.selectbox(
                "Filter by Asset",
                ["ALL"] + sorted(metadata_df["ASSET_NAME"].unique().tolist()) if not metadata_df.empty else ["ALL"],
                key="anomaly_auto_asset",
            )
        with col2:
            auto_lookback = st.slider("Lookback (days)", 1, 30, 7, key="anomaly_auto_lookback")

        if st.button("Scan for Anomaly Candidates", type="primary"):
            with st.spinner("Scanning signals for anomaly detection suitability..."):
                try:
                    result = session.sql(
                        f"CALL CORE.DETECT_ANOMALIES('{auto_asset}', 'AUTO', {auto_lookback})"
                    ).collect()
                    resp = json.loads(result[0][0]) if result else {}
                    if resp.get("status") == "CANDIDATES":
                        candidates = resp["details"]["candidates"]
                        st.session_state.anomaly_candidates = candidates
                        st.success(f"Found {len(candidates)} signals suitable for anomaly detection")
                    elif resp.get("status") == "ERROR":
                        st.warning(resp.get("message", "No suitable signals found"))
                    else:
                        st.info(resp.get("message", "Unexpected response"))
                except Exception as e:
                    st.error(f"Error scanning signals: {e}")

        if st.session_state.anomaly_candidates:
            candidates = st.session_state.anomaly_candidates
            cand_df = pd.DataFrame(candidates)
            cand_df.columns = ["Asset", "Signal", "Readings", "Std Dev", "Avg Value"]
            st.dataframe(cand_df, use_container_width=True, hide_index=True)

            selected_idx = st.selectbox(
                "Select signal to analyze",
                range(len(candidates)),
                format_func=lambda i: f"{candidates[i]['asset']}.{candidates[i]['signal']} "
                                      f"(stddev={candidates[i]['stddev']:.2f}, n={candidates[i]['readings']})",
            )

            if st.button("Run Anomaly Detection", key="run_auto_anomaly"):
                sel = candidates[selected_idx]
                with st.spinner(f"Running anomaly detection on {sel['asset']}.{sel['signal']}..."):
                    try:
                        safe_a = sel["asset"].replace("'", "''")
                        safe_s = sel["signal"].replace("'", "''")
                        r = session.sql(
                            f"CALL CORE.DETECT_ANOMALIES('{safe_a}', '{safe_s}', {auto_lookback})"
                        ).collect()
                        resp = json.loads(r[0][0]) if r else {}
                        if resp.get("status") == "SUCCESS":
                            st.success(resp["message"])
                            log_event("ANOMALY_DETECTION", resp.get("details", {}))
                        else:
                            st.error(resp.get("message", "Detection failed"))
                    except Exception as e:
                        st.error(f"Error: {e}")

    with tab_manual:
        st.markdown("Manually select an asset and signal for anomaly detection.")
        col1, col2, col3 = st.columns(3)
        with col1:
            if not metadata_df.empty:
                manual_asset = st.selectbox("Asset", sorted(metadata_df["ASSET_NAME"].unique().tolist()), key="anomaly_manual_asset")
            else:
                manual_asset = st.text_input("Asset Name", key="anomaly_manual_asset")
        with col2:
            if not metadata_df.empty and manual_asset:
                signals = metadata_df[metadata_df["ASSET_NAME"] == manual_asset]["SIGNAL_NAME"].unique().tolist()
                manual_signal = st.selectbox("Signal", sorted(signals), key="anomaly_manual_signal")
            else:
                manual_signal = st.text_input("Signal Name", key="anomaly_manual_signal")
        with col3:
            manual_lookback = st.slider("Lookback (days)", 1, 30, 7, key="anomaly_manual_lookback")

        if st.button("Run Anomaly Detection", key="run_manual_anomaly"):
            if manual_asset and manual_signal:
                with st.spinner(f"Running anomaly detection on {manual_asset}.{manual_signal}..."):
                    try:
                        safe_a = manual_asset.replace("'", "''")
                        safe_s = manual_signal.replace("'", "''")
                        r = session.sql(
                            f"CALL CORE.DETECT_ANOMALIES('{safe_a}', '{safe_s}', {manual_lookback})"
                        ).collect()
                        resp = json.loads(r[0][0]) if r else {}
                        if resp.get("status") == "SUCCESS":
                            st.success(resp["message"])
                            log_event("ANOMALY_DETECTION", resp.get("details", {}))
                        else:
                            st.error(resp.get("message", "Detection failed"))
                    except Exception as e:
                        st.error(f"Error: {e}")
            else:
                st.warning("Select both asset and signal")

    # Display anomaly results
    st.divider()
    st.subheader("Anomaly Results")

    try:
        anomaly_df = run_sql(f"""
            SELECT ASSET_NAME, SIGNAL_NAME, TS, NUMERIC_VALUE,
                   IS_ANOMALY, ANOMALY_SCORE, FORECAST_VALUE, LOWER_BOUND, UPPER_BOUND
            FROM {pfx}CONFIG.ANOMALY_RESULTS
            WHERE MODEL_TYPE = 'ANOMALY'
            ORDER BY TS DESC
            LIMIT 5000
        """)

        if not anomaly_df.empty:
            # Summary metrics
            total = len(anomaly_df)
            anomalies = anomaly_df["IS_ANOMALY"].sum() if "IS_ANOMALY" in anomaly_df.columns else 0
            signals_analyzed = anomaly_df[["ASSET_NAME", "SIGNAL_NAME"]].drop_duplicates().shape[0]

            c1, c2, c3 = st.columns(3)
            c1.metric("Total Readings Analyzed", f"{total:,}")
            c2.metric("Anomalies Detected", f"{int(anomalies):,}")
            c3.metric("Signals Analyzed", signals_analyzed)

            # Chart: time-series with anomalies highlighted
            for (asset, signal), group in anomaly_df.groupby(["ASSET_NAME", "SIGNAL_NAME"]):
                with st.expander(f"{asset}.{signal}", expanded=(anomalies > 0)):
                    chart_data = group[["TS", "NUMERIC_VALUE", "FORECAST_VALUE", "LOWER_BOUND", "UPPER_BOUND"]].copy()
                    chart_data = chart_data.set_index("TS").sort_index()
                    st.line_chart(chart_data[["NUMERIC_VALUE", "FORECAST_VALUE"]])

                    anom_only = group[group["IS_ANOMALY"] == True]
                    if not anom_only.empty:
                        st.warning(f"{len(anom_only)} anomalies detected")
                        st.dataframe(
                            anom_only[["TS", "NUMERIC_VALUE", "ANOMALY_SCORE", "LOWER_BOUND", "UPPER_BOUND"]],
                            use_container_width=True, hide_index=True,
                        )
                    else:
                        st.success("No anomalies detected in this signal")

            # Share back button
            st.divider()
            if st.button("Share Anomalies Back to AVEVA", type="secondary"):
                with st.spinner("Sharing anomaly results back to provider..."):
                    try:
                        r = session.sql("CALL CORE.SHARE_ANOMALIES_BACK()").collect()
                        resp = json.loads(r[0][0]) if r else {}
                        if resp.get("status") == "SUCCESS":
                            st.success(resp["message"])
                            log_event("ANOMALY_SHARE_BACK", resp.get("details", {}))
                        else:
                            st.error(resp.get("message", "Share-back failed"))
                    except Exception as e:
                        st.error(f"Error: {e}")
        else:
            st.info("No anomaly results yet. Use the controls above to run anomaly detection.")
    except Exception as e:
        st.info("No anomaly results yet. Run anomaly detection to see results.")

# ===========================================================================
# PAGE: Forecasting
# ===========================================================================

elif page == "Forecasting":
    st.header("Signal Forecasting")
    st.markdown("Generate time-series forecasts for PI sensor streams using trend analysis")

    pfx = db_prefix()

    col1, col2, col3 = st.columns(3)
    with col1:
        if not metadata_df.empty:
            fc_asset = st.selectbox("Asset", sorted(metadata_df["ASSET_NAME"].unique().tolist()), key="fc_asset")
        else:
            fc_asset = st.text_input("Asset Name", key="fc_asset")
    with col2:
        if not metadata_df.empty and fc_asset:
            fc_signals = metadata_df[metadata_df["ASSET_NAME"] == fc_asset]["SIGNAL_NAME"].unique().tolist()
            fc_signal = st.selectbox("Signal", sorted(fc_signals), key="fc_signal")
        else:
            fc_signal = st.text_input("Signal Name", key="fc_signal")
    with col3:
        fc_horizon = st.slider("Forecast Horizon (days)", 1, 30, 7, key="fc_horizon")

    if st.button("Generate Forecast", type="primary"):
        if fc_asset and fc_signal:
            with st.spinner(f"Generating forecast for {fc_asset}.{fc_signal}..."):
                try:
                    safe_a = fc_asset.replace("'", "''")
                    safe_s = fc_signal.replace("'", "''")
                    r = session.sql(
                        f"CALL CORE.FORECAST_SIGNAL('{safe_a}', '{safe_s}', {fc_horizon})"
                    ).collect()
                    resp = json.loads(r[0][0]) if r else {}
                    if resp.get("status") == "SUCCESS":
                        st.success(resp["message"])
                        details = resp.get("details", {})
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Forecast Steps", details.get("forecast_steps", "N/A"))
                        c2.metric("Training Rows", details.get("training_rows", "N/A"))
                        c3.metric("Data Interval", f"{details.get('avg_interval_minutes', '?')} min")
                        log_event("FORECAST", details)
                    else:
                        st.error(resp.get("message", "Forecast failed"))
                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.warning("Select both asset and signal")

    # Display forecast results
    st.divider()
    st.subheader("Forecast Results")

    try:
        # Get historical data for the selected signal
        if fc_asset and fc_signal:
            safe_a = fc_asset.replace("'", "''")
            safe_s = fc_signal.replace("'", "''")

            historical_df = run_sql(f"""
                SELECT TS, NUMERIC_VALUE
                FROM {pfx}DATA_VIEWS.PI_STREAMS_UNIFIED
                WHERE ASSET_NAME = '{safe_a}' AND SIGNAL_NAME = '{safe_s}'
                  AND NUMERIC_VALUE IS NOT NULL
                ORDER BY TS
            """)

            forecast_df = run_sql(f"""
                SELECT TS, FORECAST_VALUE, LOWER_BOUND, UPPER_BOUND
                FROM {pfx}CONFIG.ANOMALY_RESULTS
                WHERE ASSET_NAME = '{safe_a}' AND SIGNAL_NAME = '{safe_s}'
                  AND MODEL_TYPE = 'FORECAST'
                ORDER BY TS
            """)

            if not forecast_df.empty:
                st.success(f"Showing forecast for {fc_asset}.{fc_signal}")

                # Combine historical and forecast for charting
                hist_chart = historical_df[["TS", "NUMERIC_VALUE"]].copy()
                hist_chart = hist_chart.rename(columns={"NUMERIC_VALUE": "Historical"})
                hist_chart = hist_chart.set_index("TS")

                fc_chart = forecast_df[["TS", "FORECAST_VALUE", "LOWER_BOUND", "UPPER_BOUND"]].copy()
                fc_chart = fc_chart.rename(columns={
                    "FORECAST_VALUE": "Forecast",
                    "LOWER_BOUND": "Lower Bound",
                    "UPPER_BOUND": "Upper Bound",
                })
                fc_chart = fc_chart.set_index("TS")

                combined = pd.concat([hist_chart, fc_chart], axis=1).sort_index()
                st.line_chart(combined)

                st.subheader("Forecast Data")
                st.dataframe(forecast_df, use_container_width=True, hide_index=True)

                # Download option
                csv = forecast_df.to_csv(index=False)
                st.download_button(
                    "Download Forecast CSV", csv,
                    f"forecast_{fc_asset}_{fc_signal}.csv", "text/csv",
                )
            elif not historical_df.empty:
                st.info("No forecast generated yet. Click 'Generate Forecast' above.")
                st.line_chart(historical_df.set_index("TS")["NUMERIC_VALUE"])
            else:
                st.info("No data available for this signal.")
        else:
            # Show all forecast results if no specific signal selected
            all_fc = run_sql(f"""
                SELECT ASSET_NAME, SIGNAL_NAME, COUNT(*) AS FORECAST_POINTS,
                       MIN(TS) AS FORECAST_FROM, MAX(TS) AS FORECAST_TO
                FROM {pfx}CONFIG.ANOMALY_RESULTS
                WHERE MODEL_TYPE = 'FORECAST'
                GROUP BY ASSET_NAME, SIGNAL_NAME
                ORDER BY ASSET_NAME, SIGNAL_NAME
            """)
            if not all_fc.empty:
                st.dataframe(all_fc, use_container_width=True, hide_index=True)
            else:
                st.info("No forecasts generated yet.")
    except Exception as e:
        st.info("Select an asset and signal, then generate a forecast.")

elif page == "Data Dictionary":
    st.header("AVEVA PI Data Dictionary")
    st.markdown("Auto-generated documentation for all PI streams and signals")

    tab1, tab2, tab3 = st.tabs(["Field Definitions", "Stream Metadata", "App State"])

    with tab1:
        @st.cache_data(ttl=3600)
        def generate_pi_dictionary(_session):
            schema_info = get_schema_info(_session)
            prompt = (
                "Based on this AVEVA PI data historian schema, generate a data dictionary. "
                "For each signal type, provide a business-friendly description.\n\n"
                f"{schema_info}\n\n"
                "Also include entries for the standard PI quality columns:\n"
                "- IS_QUESTIONABLE, IS_SUBSTITUTED, IS_ANNOTATED, SYSTEM_STATE_CODE, DIGITAL_STATE_NAME\n\n"
                'Return as a JSON array: [{"field": "NAME", "category": "Signal|Quality|Metadata", '
                '"description": "...", "unit": "...", "example": "..."}]\n'
                "Return ONLY valid JSON array."
            )
            try:
                result = session.sql(f"""
                    SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{safe_sql_string(prompt)}')
                """).collect()
                text = result[0][0] if result else "[]"
                start = text.find("[")
                end = text.rfind("]") + 1
                if start >= 0 and end > start:
                    return json.loads(text[start:end])
            except Exception:
                pass
            return None

        with st.spinner("Generating data dictionary via Cortex AI..."):
            dictionary = generate_pi_dictionary(session)

        if dictionary:
            st.success(f"Found {len(dictionary)} field definitions")
            search = st.text_input("Search fields", placeholder="Type to filter...")
            filtered_dict = (
                [d for d in dictionary
                 if search.lower() in d.get("field", "").lower()
                 or search.lower() in d.get("description", "").lower()]
                if search
                else dictionary
            )
            for item in filtered_dict:
                with st.expander(f"**{item.get('field', 'Unknown')}** - _{item.get('category', 'N/A')}_"):
                    st.markdown(f"**Description:** {item.get('description', 'N/A')}")
                    if item.get("unit"):
                        st.markdown(f"**Unit:** {item.get('unit')}")
                    st.markdown(f"**Example:** `{item.get('example', 'N/A')}`")

            st.divider()
            dict_df = pd.DataFrame(filtered_dict)
            st.dataframe(dict_df, use_container_width=True, hide_index=True)
            csv = dict_df.to_csv(index=False)
            st.download_button("Download as CSV", csv, "pi_data_dictionary.csv", "text/csv")
        else:
            st.warning("Could not generate dictionary. Showing raw stream metadata.")
            st.dataframe(metadata_df, use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("Stream Metadata")
        if not metadata_df.empty:
            st.dataframe(metadata_df, use_container_width=True, hide_index=True)
        else:
            st.info("No stream metadata available")

    with tab3:
        st.subheader("App State")
        try:
            state_df = run_sql("SELECT KEY, VALUE, UPDATED_AT FROM CONFIG.APP_STATE ORDER BY KEY")
            if not state_df.empty:
                st.dataframe(state_df, use_container_width=True, hide_index=True)
            else:
                st.info("No app state entries")
        except Exception:
            st.info("Could not load app state")

        st.subheader("Asset Hierarchy")
        hierarchy_df = get_asset_hierarchy(session)
        if not hierarchy_df.empty:
            st.dataframe(hierarchy_df, use_container_width=True, hide_index=True)
        else:
            st.info("No asset hierarchy data")

# ===========================================================================
# PAGE: Privacy & Telemetry
# ===========================================================================

elif page == "Privacy & Telemetry":
    st.header("Privacy & Telemetry Dashboard")

    st.markdown("""
    <div class="privacy-box">
        <h4>Privacy Protection with Cortex AI_REDACT</h4>
        <p>All user queries are processed through <strong>SNOWFLAKE.CORTEX.AI_REDACT</strong> before logging.
        PII including names, emails, phone numbers, SSNs, and addresses is automatically removed.</p>
    </div>
    """, unsafe_allow_html=True)

    st.divider()
    st.subheader("Usage Analytics")

    try:
        stats_df = run_sql("""
            SELECT EVENT_TYPE, COUNT(*) AS EVENT_COUNT,
                   MIN(EVENT_TS) AS FIRST_EVENT, MAX(EVENT_TS) AS LAST_EVENT
            FROM CONFIG.EVENT_LOG GROUP BY EVENT_TYPE ORDER BY EVENT_COUNT DESC
        """)
        if not stats_df.empty:
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Events", f"{stats_df['EVENT_COUNT'].sum():,}")
            c2.metric("Event Types", len(stats_df))
            latest = stats_df["LAST_EVENT"].max()
            c3.metric("Most Recent", latest.strftime("%m/%d %H:%M") if pd.notna(latest) else "N/A")
            st.bar_chart(stats_df.set_index("EVENT_TYPE")["EVENT_COUNT"])
        else:
            st.info("No events logged yet")
    except Exception as e:
        st.warning(f"Could not load events: {e}")

    st.divider()
    st.subheader("Recent Activity")

    try:
        events_df = run_sql("""
            SELECT EVENT_TS AS "Timestamp", EVENT_TYPE AS "Event",
                   REDACTED_QUERY AS "Query (Redacted)", EVENT_DATA AS "Details"
            FROM CONFIG.EVENT_LOG ORDER BY EVENT_TS DESC LIMIT 20
        """)
        if not events_df.empty:
            st.dataframe(events_df, use_container_width=True, hide_index=True)
    except Exception:
        st.info("No events yet")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption("AVEVA CONNECT Analytics | PI Data Historian | Snowflake Cortex AI | Privacy Protected")
