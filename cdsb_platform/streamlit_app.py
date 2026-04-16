import streamlit as st

st.set_page_config(
    page_title="CDSB Extraction Platform",
    page_icon=":material/cloud_download:",
    layout="wide",
)

import os

conn = st.connection("snowflake")

if "cdsb_database" not in st.session_state:
    try:
        ctx = conn.query("SELECT CURRENT_DATABASE() AS db, CURRENT_SCHEMA() AS sch")
        st.session_state.cdsb_database = ctx.iloc[0]["DB"] or os.getenv("CDSB_DATABASE", "CDSB_DEMO")
        st.session_state.cdsb_schema = ctx.iloc[0]["SCH"] or os.getenv("CDSB_SCHEMA", "RAW")
    except Exception:
        st.session_state.cdsb_database = os.getenv("CDSB_DATABASE", "CDSB_DEMO")
        st.session_state.cdsb_schema = os.getenv("CDSB_SCHEMA", "RAW")

page = st.navigation(
    {
        "": [
            st.Page("app_pages/sources.py", title="Manage Sources", icon=":material/language:"),
            st.Page("app_pages/extract.py", title="Run Extraction", icon=":material/play_arrow:"),
            st.Page("app_pages/monitor.py", title="Monitor", icon=":material/monitoring:"),
        ],
        "Search": [
            st.Page("app_pages/search.py", title="Search & Chat", icon=":material/search:"),
        ],
        "Admin": [
            st.Page("app_pages/pipeline.py", title="Pipeline", icon=":material/build:"),
        ],
    },
    position="sidebar",
)

with st.sidebar:
    st.divider()
    st.caption("CDSB Extraction Platform")
    st.caption("Powered by Snowflake Cortex")

page.run()
