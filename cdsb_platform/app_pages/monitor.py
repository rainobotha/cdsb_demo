import streamlit as st
from datetime import timedelta

conn = st.connection("snowflake")
DB = st.session_state.get("cdsb_database", "CDSB_DEMO")
SCHEMA = st.session_state.get("cdsb_schema", "RAW")


def fqn(name):
    return f"{DB}.{SCHEMA}.{name}"


col1, col2 = st.columns([3, 1])
with col2:
    if st.button("Refresh", icon=":material/refresh:"):
        st.cache_data.clear()
        st.rerun()

tab_overview, tab_runs, tab_data = st.tabs(["Overview", "Run History", "Data Inventory"])

with tab_overview:
    st.subheader("Source Status")
    sources = conn.query(
        f"""SELECT source_name, source_url, crawl_method, enabled,
                   last_run_at, last_run_status, last_run_pages, last_run_files
            FROM {fqn('EXTRACTION_SOURCES')} ORDER BY source_name""",
        ttl=timedelta(seconds=30),
    )
    if not sources.empty:
        st.dataframe(
            sources,
            use_container_width=True,
            column_config={
                "ENABLED": st.column_config.CheckboxColumn("Active"),
                "LAST_RUN_AT": st.column_config.DatetimeColumn("Last Run", format="YYYY-MM-DD HH:mm"),
            },
        )

    st.divider()
    st.subheader("Pipeline Health")
    mc1, mc2, mc3, mc4 = st.columns(4)

    try:
        unified_count = conn.query(
            f"SELECT COUNT(*) as cnt FROM {fqn('CDSB_DOCUMENTS_UNIFIED')}", ttl=60
        ).iloc[0]["CNT"]
    except Exception:
        unified_count = 0

    try:
        chunk_count = conn.query(
            f"SELECT COUNT(*) as cnt FROM {fqn('CDSB_CHUNKS')}", ttl=60
        ).iloc[0]["CNT"]
    except Exception:
        chunk_count = 0

    try:
        search_status = conn.query(
            f"SHOW CORTEX SEARCH SERVICES IN SCHEMA {DB}.{SCHEMA}", ttl=60
        )
        if not search_status.empty:
            ss_status = search_status.iloc[0].get("service_status", "UNKNOWN")
        else:
            ss_status = "NOT FOUND"
    except Exception:
        ss_status = "UNKNOWN"

    with mc1:
        st.metric("Sources", len(sources) if not sources.empty else 0)
    with mc2:
        st.metric("Unified Docs", f"{unified_count:,}")
    with mc3:
        st.metric("Chunks", f"{chunk_count:,}")
    with mc4:
        st.metric("Search Service", ss_status)

with tab_runs:
    runs = conn.query(
        f"""SELECT run_id, source_name, started_at, completed_at, status,
                   pages_crawled, files_downloaded, files_parsed, error_message
            FROM {fqn('EXTRACTION_RUNS')}
            ORDER BY started_at DESC LIMIT 50""",
        ttl=timedelta(seconds=15),
    )
    if runs.empty:
        st.info("No extraction runs yet.")
    else:
        st.dataframe(
            runs,
            use_container_width=True,
            column_config={
                "STATUS": st.column_config.TextColumn("Status"),
                "STARTED_AT": st.column_config.DatetimeColumn("Started", format="YYYY-MM-DD HH:mm"),
                "COMPLETED_AT": st.column_config.DatetimeColumn("Completed", format="YYYY-MM-DD HH:mm"),
            },
        )

with tab_data:
    st.subheader("Data Inventory")

    sources_inv = conn.query(
        f"SELECT source_name, web_table_name, parsed_table_name, stage_name "
        f"FROM {fqn('EXTRACTION_SOURCES')} WHERE enabled = TRUE ORDER BY source_name",
        ttl=60,
    )

    if sources_inv.empty:
        st.info("No sources configured.")
    else:
        inventory_data = []
        for _, src in sources_inv.iterrows():
            web_t = fqn(src["WEB_TABLE_NAME"])
            parsed_t = fqn(src["PARSED_TABLE_NAME"])
            stage_at = f"@{DB}.{SCHEMA}.{src['STAGE_NAME']}"

            try:
                wc = conn.query(f"SELECT COUNT(*) as cnt FROM {web_t}", ttl=60).iloc[0]["CNT"]
            except Exception:
                wc = 0
            try:
                pc = conn.query(f"SELECT COUNT(*) as cnt FROM {parsed_t}", ttl=60).iloc[0]["CNT"]
            except Exception:
                pc = 0
            try:
                fc = conn.query(
                    f"SELECT COUNT(*) as cnt FROM DIRECTORY({stage_at})", ttl=60
                ).iloc[0]["CNT"]
            except Exception:
                fc = 0

            inventory_data.append({
                "Source": src["SOURCE_NAME"],
                "Web Pages": wc,
                "Parsed Docs": pc,
                "Stage Files": fc,
            })

        st.dataframe(inventory_data, use_container_width=True)

    st.divider()
    st.subheader("Chunks by Domain")
    try:
        domain_stats = conn.query(
            f"SELECT domain, COUNT(*) as chunks FROM {fqn('CDSB_CHUNKS')} GROUP BY domain ORDER BY chunks DESC",
            ttl=60,
        )
        if not domain_stats.empty:
            st.bar_chart(domain_stats.set_index("DOMAIN"))
    except Exception:
        st.info("Chunks table not yet available.")
