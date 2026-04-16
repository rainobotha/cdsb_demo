import streamlit as st
from datetime import timedelta

conn = st.connection("snowflake")
DB = st.session_state.get("cdsb_database", "CDSB_DEMO")
SCHEMA = st.session_state.get("cdsb_schema", "RAW")


def fqn(name):
    return f"{DB}.{SCHEMA}.{name}"


st.markdown("Run individual pipeline stages or rebuild the full pipeline.")

st.divider()

sources = conn.query(
    f"SELECT source_name, stage_name, web_table_name, parsed_table_name "
    f"FROM {fqn('EXTRACTION_SOURCES')} WHERE enabled = TRUE ORDER BY source_name",
    ttl=timedelta(seconds=30),
)

tab_parse, tab_unify, tab_full, tab_clear = st.tabs(["Parse Documents", "Unify & Chunk", "Full Rebuild", "Clear Stage Files"])

with tab_parse:
    st.markdown("Parse documents from stages using AI_PARSE_DOCUMENT.")

    if sources.empty:
        st.warning("No enabled sources.")
    else:
        selected = st.multiselect(
            "Sources to parse",
            sources["SOURCE_NAME"].tolist(),
            default=sources["SOURCE_NAME"].tolist(),
            key="parse_select",
        )

        if st.button("Run Parse", type="primary", key="run_parse"):
            session = conn.session()
            for name in selected:
                row = sources[sources["SOURCE_NAME"] == name].iloc[0]
                stage_at = f"@{DB}.{SCHEMA}.{row['STAGE_NAME']}"
                parsed_t = fqn(row["PARSED_TABLE_NAME"])
                with st.spinner(f"Parsing {name}..."):
                    try:
                        session.sql(f"""
                            CREATE OR REPLACE TABLE {parsed_t} AS
                            SELECT
                                d.RELATIVE_PATH as filename,
                                LOWER(d.RELATIVE_PATH) as filename_lower,
                                SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(
                                    TO_FILE('{stage_at}', d.RELATIVE_PATH),
                                    {{'mode': 'LAYOUT'}}
                                ):content::VARCHAR as parsed_content,
                                d.SIZE as file_size,
                                CURRENT_TIMESTAMP() as parsed_at
                            FROM DIRECTORY({stage_at}) d
                            WHERE LOWER(d.RELATIVE_PATH) LIKE '%.pdf'
                               OR LOWER(d.RELATIVE_PATH) LIKE '%.docx'
                               OR LOWER(d.RELATIVE_PATH) LIKE '%.doc'
                               OR LOWER(d.RELATIVE_PATH) LIKE '%.tiff'
                               OR LOWER(d.RELATIVE_PATH) LIKE '%.tif'
                        """).collect()
                        count = session.sql(f"SELECT COUNT(*) FROM {parsed_t}").collect()[0][0]
                        st.success(f"{name}: {count} documents parsed")
                    except Exception as e:
                        st.error(f"{name}: {e}")

with tab_unify:
    st.markdown("Incrementally merge into unified documents and chunk only changed docs.")

    if st.button("Run Unify + Chunk", type="primary", key="run_unify"):
        session = conn.session()
        unified = fqn("CDSB_DOCUMENTS_UNIFIED")
        chunks = fqn("CDSB_CHUNKS")
        staging = fqn("_UNIFIED_STAGING")

        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {unified} (
                source_type VARCHAR, source_url VARCHAR, title VARCHAR,
                content VARCHAR, domain VARCHAR, content_hash VARCHAR,
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {chunks} (
                chunk_id VARCHAR, chunk_content VARCHAR, source_type VARCHAR,
                title VARCHAR, source_url VARCHAR, domain VARCHAR
            )
        """).collect()

        with st.spinner("Building staging table..."):
            union_parts = []
            for _, src in sources.iterrows():
                web_t = fqn(src["WEB_TABLE_NAME"])
                parsed_t = fqn(src["PARSED_TABLE_NAME"])
                domain = src["SOURCE_NAME"].lower()
                union_parts.append(f"""
                    SELECT 'web' as source_type, url as source_url, title, content, domain, MD5(content) as content_hash
                    FROM {web_t} WHERE content IS NOT NULL AND LEN(content) > 50
                """)
                union_parts.append(f"""
                    SELECT 'document' as source_type, filename as source_url,
                           REPLACE(filename, '.pdf', '') as title, parsed_content as content,
                           '{domain}' as domain, MD5(parsed_content) as content_hash
                    FROM {parsed_t} WHERE parsed_content IS NOT NULL AND LEN(parsed_content) > 50
                """)
            session.sql(
                f"CREATE OR REPLACE TABLE {staging} AS\n" + "\nUNION ALL\n".join(union_parts)
            ).collect()

        with st.spinner("Merging into unified (incremental)..."):
            session.sql(f"""
                MERGE INTO {unified} t USING {staging} s
                ON t.source_url = s.source_url AND t.source_type = s.source_type
                WHEN MATCHED AND t.content_hash != s.content_hash THEN UPDATE SET
                    title = s.title, content = s.content, domain = s.domain,
                    content_hash = s.content_hash, updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                    (source_type, source_url, title, content, domain, content_hash, updated_at)
                VALUES (s.source_type, s.source_url, s.title, s.content, s.domain, s.content_hash, CURRENT_TIMESTAMP())
            """).collect()
            session.sql(f"""
                DELETE FROM {unified} t WHERE NOT EXISTS (
                    SELECT 1 FROM {staging} s WHERE s.source_url = t.source_url AND s.source_type = t.source_type
                )
            """).collect()
            unified_count = session.sql(f"SELECT COUNT(*) FROM {unified}").collect()[0][0]
            st.text(f"Unified: {unified_count:,} documents")

        with st.spinner("Chunking changed documents..."):
            session.sql(f"""
                CREATE OR REPLACE TEMPORARY TABLE _changed_sources AS
                SELECT source_url, source_type FROM {unified}
                WHERE updated_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
            """).collect()
            changed = session.sql("SELECT COUNT(*) FROM _changed_sources").collect()[0][0]

            if changed > 0:
                session.sql(f"""
                    DELETE FROM {chunks} WHERE EXISTS (
                        SELECT 1 FROM _changed_sources c
                        WHERE c.source_url = {chunks}.source_url AND c.source_type = {chunks}.source_type
                    )
                """).collect()
                session.sql(f"""
                    INSERT INTO {chunks} (chunk_id, chunk_content, source_type, title, source_url, domain)
                    WITH RECURSIVE src AS (
                        SELECT ROW_NUMBER() OVER (ORDER BY source_type, source_url) as doc_id,
                               source_type, source_url, title, content, domain
                        FROM {unified} WHERE EXISTS (
                            SELECT 1 FROM _changed_sources c
                            WHERE c.source_url = {unified}.source_url AND c.source_type = {unified}.source_type
                        )
                    ),
                    chnk AS (
                        SELECT doc_id, source_type, source_url, title, domain, 1 as chunk_num,
                               SUBSTR(content, 1, 1500) as chunk_content, LEN(content) as total_len
                        FROM src
                        UNION ALL
                        SELECT c.doc_id, c.source_type, c.source_url, c.title, c.domain, c.chunk_num + 1,
                               SUBSTR(d.content, 1 + (c.chunk_num * 1000), 1500), c.total_len
                        FROM chnk c JOIN src d ON c.doc_id = d.doc_id
                        WHERE 1 + (c.chunk_num * 1000) <= c.total_len
                    )
                    SELECT doc_id || '-' || chunk_num, chunk_content, source_type, title, source_url, domain
                    FROM chnk WHERE LEN(chunk_content) > 50
                """).collect()

            session.sql(f"DROP TABLE IF EXISTS {staging}").collect()
            chunk_count = session.sql(f"SELECT COUNT(*) FROM {chunks}").collect()[0][0]
            st.success(f"Done! {changed:,} docs changed, {chunk_count:,} total chunks. Search service picks up changes incrementally.")

with tab_full:
    st.markdown("Run the complete pipeline: Parse → Unify → Chunk (incremental)")
    st.info("The pipeline now uses MERGE + incremental chunking. Only new/changed documents are re-chunked.")

    if st.button("Full Pipeline Rebuild", type="primary", key="run_full"):
        st.markdown("Use the **Run Extraction** page → 'Run Full Pipeline Only' button for the combined flow.")

with tab_clear:
    st.markdown(
        "Remove downloaded documents from Snowflake stages after they have been "
        "parsed and merged into the unified table. This frees storage while keeping "
        "the extracted text in the pipeline tables."
    )
    st.warning(
        "Only clear stages **after** running the Parse and Unify stages. "
        "If you clear before parsing, the documents will be lost and you will "
        "need to re-extract them."
    )

    if sources.empty:
        st.info("No enabled sources.")
    else:
        session = conn.session()
        stage_info = []
        for _, src in sources.iterrows():
            stage_name = src["STAGE_NAME"]
            stage_fqn = f"@{DB}.{SCHEMA}.{stage_name}"
            try:
                count_df = conn.query(
                    f"SELECT COUNT(*) as cnt, COALESCE(SUM(SIZE), 0) as total_bytes "
                    f"FROM DIRECTORY({stage_fqn})",
                    ttl=timedelta(seconds=30),
                )
                file_count = int(count_df.iloc[0]["CNT"])
                total_mb = round(count_df.iloc[0]["TOTAL_BYTES"] / (1024 * 1024), 1)
            except Exception:
                file_count = 0
                total_mb = 0.0
            stage_info.append({
                "Source": src["SOURCE_NAME"],
                "Stage": stage_name,
                "Files": file_count,
                "Size (MB)": total_mb,
            })

        import pandas as pd
        stage_df = pd.DataFrame(stage_info)
        st.dataframe(stage_df, use_container_width=True, hide_index=True)

        total_files = stage_df["Files"].sum()
        total_mb = stage_df["Size (MB)"].sum()
        st.metric("Total across all stages", f"{int(total_files):,} files — {total_mb:,.1f} MB")

        st.divider()

        clear_sources = st.multiselect(
            "Select sources to clear",
            sources["SOURCE_NAME"].tolist(),
            key="clear_select",
        )

        if clear_sources:
            st.error(
                f"This will permanently delete all files from "
                f"{len(clear_sources)} stage(s). The parsed text in your "
                f"pipeline tables will remain intact."
            )

        if st.button(
            "Clear Selected Stages",
            type="primary",
            disabled=len(clear_sources) == 0,
            key="run_clear",
        ):
            cleared = []
            for name in clear_sources:
                row = sources[sources["SOURCE_NAME"] == name].iloc[0]
                stage_fqn = f"{DB}.{SCHEMA}.{row['STAGE_NAME']}"
                with st.spinner(f"Clearing {row['STAGE_NAME']}..."):
                    try:
                        session.sql(f"REMOVE @{stage_fqn}").collect()
                        cleared.append(name)
                        st.text(f"Cleared {row['STAGE_NAME']}")
                    except Exception as e:
                        st.error(f"Failed to clear {row['STAGE_NAME']}: {e}")
            if cleared:
                st.success(f"Cleared {len(cleared)} stage(s): {', '.join(cleared)}")
                st.rerun()
