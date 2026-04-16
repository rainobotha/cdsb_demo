import streamlit as st
import json
import time
import threading
from datetime import timedelta

conn = st.connection("snowflake")
DB = st.session_state.get("cdsb_database", "CDSB_DEMO")
SCHEMA = st.session_state.get("cdsb_schema", "RAW")


def fqn(name):
    return f"{DB}.{SCHEMA}.{name}"


def load_sources():
    return conn.query(
        f"SELECT * FROM {fqn('EXTRACTION_SOURCES')} WHERE enabled = TRUE ORDER BY source_name",
        ttl=timedelta(seconds=30),
    )


def row_to_config(row):
    cfg = {}
    for col in row.index:
        cfg[col.lower()] = row[col]
    for arr_col in ("allowed_domains", "file_types", "direct_urls"):
        val = cfg.get(arr_col)
        if val is None:
            cfg[arr_col] = [] if arr_col != "file_types" else ["pdf"]
        elif isinstance(val, list):
            cfg[arr_col] = val
        elif isinstance(val, str):
            try:
                val = json.loads(val)
            except Exception:
                val = [v.strip() for v in val.split(",") if v.strip()]
            cfg[arr_col] = val if isinstance(val, list) else [val]
        else:
            cfg[arr_col] = list(val) if hasattr(val, '__iter__') else [str(val)]
    return cfg


sources_df = load_sources()

st.markdown("Select sources and run extraction. Files are streamed directly to Snowflake stages.")
st.divider()

if sources_df.empty:
    st.warning("No enabled sources. Configure sources in the Manage Sources page.")
    st.stop()

source_names = sources_df["SOURCE_NAME"].tolist()
selected_sources = st.multiselect(
    "Sources to Extract",
    source_names,
    default=source_names,
    help="Select which sources to crawl and extract",
)

col1, col2, col3 = st.columns(3)
with col1:
    run_pipeline = st.checkbox("Run Parse + Unify + Chunk pipeline after extraction", value=True)
with col2:
    dry_run = st.checkbox("Dry run (show config only)")

st.divider()

if selected_sources:
    with st.expander("Extraction Configuration", expanded=False):
        for name in selected_sources:
            row = sources_df[sources_df["SOURCE_NAME"] == name].iloc[0]
            ft = row.get("FILE_TYPES")
            if isinstance(ft, str):
                try:
                    ft = json.loads(ft)
                except Exception:
                    ft = [ft]
            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown(f"**{name}** — `{row['CRAWL_METHOD']}`")
            with c2:
                st.caption(f"Pages: {row.get('MAX_PAGES') or '—'} | Depth: {row.get('MAX_DEPTH') or '—'}")
            with c3:
                st.caption(f"Files: {ft} | CF: {'Yes' if row.get('CLOUDFLARE_BYPASS') else 'No'}")

if dry_run:
    if st.button("Show Config JSON", type="secondary"):
        for name in selected_sources:
            row = sources_df[sources_df["SOURCE_NAME"] == name].iloc[0]
            st.json({k: str(v) for k, v in row.items()})
    st.stop()

if st.button("Start Extraction", type="primary", icon=":material/play_arrow:", use_container_width=True):
    from engine import CrawlProgress, run_extraction

    session = conn.session()
    raw_conn = session.connection

    def get_conn():
        return raw_conn

    def init_session(cur):
        pass

    total_sources = len(selected_sources)
    overall_progress = st.progress(0, text=f"Starting extraction of {total_sources} source(s)...")

    for src_idx, name in enumerate(selected_sources):
        row = sources_df[sources_df["SOURCE_NAME"] == name].iloc[0]
        cfg = row_to_config(row)
        max_pages = int(cfg.get("max_pages") or 5000)
        max_files_est = max_pages

        session.sql(f"""
            INSERT INTO {fqn('EXTRACTION_RUNS')}
            (source_id, source_name, status) VALUES ({row['SOURCE_ID']}, '{name}', 'RUNNING')
        """).collect()

        progress = CrawlProgress()
        result_holder = [None]
        error_holder = [None]

        def run_in_bg():
            try:
                result_holder[0] = run_extraction(
                    cfg, get_conn, init_session, DB, SCHEMA, progress,
                    single_connection=True,
                )
            except Exception as exc:
                error_holder[0] = exc

        worker = threading.Thread(target=run_in_bg, daemon=True)
        worker.start()

        source_container = st.container()
        with source_container:
            st.subheader(f":material/language: {name}", divider="blue")
            phase_text = st.empty()
            source_bar = st.progress(0)
            metrics_row = st.columns(4)
            pages_metric = metrics_row[0].empty()
            files_found_metric = metrics_row[1].empty()
            files_up_metric = metrics_row[2].empty()
            status_metric = metrics_row[3].empty()
            log_expander = st.expander("Live Log", expanded=False)
            log_area = log_expander.empty()

        prev_log_len = 0
        while worker.is_alive():
            pages = progress.pages_crawled
            found = progress.files_found
            uploaded = progress.files_uploaded
            status = progress.status

            if status == "RUNNING":
                if uploaded > 0 and found > 0:
                    pct = min(uploaded / max(found, 1), 1.0)
                    phase_text.markdown("**Phase 3/3** — Downloading & uploading files")
                elif pages > 0:
                    pct = min(pages / max(max_pages, 1), 0.95)
                    phase_text.markdown("**Phase 1/3** — Crawling pages")
                else:
                    pct = 0.0
                    phase_text.markdown("**Phase 1/3** — Starting crawl...")
            else:
                pct = 0.0

            source_bar.progress(min(pct, 1.0))
            pages_metric.metric("Pages", f"{pages:,}")
            files_found_metric.metric("Files Found", f"{found:,}")
            files_up_metric.metric("Uploaded", f"{uploaded:,}")
            status_metric.metric("Status", status)

            cur_log = progress.get_log_text()
            if len(cur_log) != prev_log_len:
                log_area.code(cur_log, language="text")
                prev_log_len = len(cur_log)

            time.sleep(0.5)

        worker.join()

        final_log = progress.get_log_text()
        log_area.code(final_log, language="text")

        if error_holder[0] is not None:
            err = error_holder[0]
            source_bar.progress(1.0)
            phase_text.markdown("**FAILED**")
            status_metric.metric("Status", "FAILED")
            source_container.error(str(err))
            session.sql(f"""
                UPDATE {fqn('EXTRACTION_RUNS')}
                SET status = 'FAILED',
                    completed_at = CURRENT_TIMESTAMP(),
                    error_message = '{str(err)[:500].replace(chr(39), "")}'
                WHERE source_name = '{name}' AND status = 'RUNNING'
            """).collect()
            session.sql(f"""
                UPDATE {fqn('EXTRACTION_SOURCES')}
                SET last_run_at = CURRENT_TIMESTAMP(), last_run_status = 'FAILED'
                WHERE source_id = {row['SOURCE_ID']}
            """).collect()
        else:
            result = result_holder[0] or {}
            source_bar.progress(1.0)
            phase_text.markdown("**Complete**")
            pages_metric.metric("Pages", f"{result.get('pages', 0):,}")
            files_up_metric.metric("Uploaded", f"{result.get('uploaded', 0):,}")
            status_metric.metric("Status", "SUCCESS")

            session.sql(f"""
                UPDATE {fqn('EXTRACTION_SOURCES')}
                SET last_run_at = CURRENT_TIMESTAMP(),
                    last_run_status = 'SUCCESS',
                    last_run_pages = {result.get('pages', 0)},
                    last_run_files = {result.get('uploaded', 0)}
                WHERE source_id = {row['SOURCE_ID']}
            """).collect()
            session.sql(f"""
                UPDATE {fqn('EXTRACTION_RUNS')}
                SET status = 'SUCCESS',
                    completed_at = CURRENT_TIMESTAMP(),
                    pages_crawled = {result.get('pages', 0)},
                    files_downloaded = {result.get('uploaded', 0)}
                WHERE source_name = '{name}' AND status = 'RUNNING'
            """).collect()

        base_pct = (src_idx + 1) / total_sources
        overall_progress.progress(base_pct, text=f"Completed {src_idx + 1}/{total_sources} sources")

    overall_progress.progress(1.0, text=f"All {total_sources} source(s) extracted!")

    if run_pipeline:
        st.divider()
        st.subheader(":material/manufacturing: Pipeline", divider="orange")
        pipe_bar = st.progress(0, text="Initialising pipeline...")
        pipe_log = st.container()

        selected_list = ", ".join(f"'{n}'" for n in selected_sources)
        sources_for_pipeline = conn.query(
            f"SELECT source_name, stage_name, web_table_name, parsed_table_name "
            f"FROM {fqn('EXTRACTION_SOURCES')} WHERE source_name IN ({selected_list})",
            ttl=0,
        )
        def object_exists(obj_type, obj_name):
            try:
                session.sql(f"DESCRIBE {obj_type} {obj_name}").collect()
                return True
            except Exception:
                return False

        source_file_counts = {}
        total_files = 0
        for _, src in sources_for_pipeline.iterrows():
            sfqn = f"{DB}.{SCHEMA}.{src['STAGE_NAME']}"
            if object_exists("STAGE", sfqn):
                try:
                    cnt = session.sql(f"""
                        SELECT COUNT(*) FROM DIRECTORY(@{sfqn}) d
                        WHERE LOWER(d.RELATIVE_PATH) LIKE '%.pdf'
                           OR LOWER(d.RELATIVE_PATH) LIKE '%.docx'
                           OR LOWER(d.RELATIVE_PATH) LIKE '%.doc'
                           OR LOWER(d.RELATIVE_PATH) LIKE '%.tiff'
                           OR LOWER(d.RELATIVE_PATH) LIKE '%.tif'
                    """).collect()[0][0]
                except Exception:
                    cnt = 0
            else:
                cnt = 0
            source_file_counts[src['SOURCE_NAME']] = cnt
            total_files += cnt

        web_row_counts = {}
        total_web_rows = 0
        for _, src in sources_for_pipeline.iterrows():
            wt = fqn(src['WEB_TABLE_NAME'])
            if object_exists("TABLE", wt):
                try:
                    rc = session.sql(f"SELECT COUNT(*) FROM {wt} WHERE content IS NOT NULL AND LEN(content) > 50").collect()[0][0]
                except Exception:
                    rc = 0
            else:
                rc = 0
            web_row_counts[src['SOURCE_NAME']] = rc
            total_web_rows += rc

        steps_total = len(sources_for_pipeline) + 4
        step = 0

        for idx, (_, src) in enumerate(sources_for_pipeline.iterrows()):
            step += 1
            stage_fqn = f"{DB}.{SCHEMA}.{src['STAGE_NAME']}"
            stage_at = f"@{stage_fqn}"
            parsed_t = fqn(src['PARSED_TABLE_NAME'])
            fc = source_file_counts.get(src['SOURCE_NAME'], 0)
            wc = web_row_counts.get(src['SOURCE_NAME'], 0)
            if not object_exists("STAGE", stage_fqn) or fc == 0:
                pipe_bar.progress(step / steps_total, text=f"Step {step}/{steps_total} — Skipping {src['SOURCE_NAME']} (no files)")
                pipe_log.text(f"⏭ {src['SOURCE_NAME']} — {wc:,} web pages, 0 documents (no stage)")
                continue
            session.sql(f"""
                CREATE TABLE IF NOT EXISTS {parsed_t} (
                    filename VARCHAR, filename_lower VARCHAR, parsed_content VARCHAR,
                    file_size NUMBER, parsed_at TIMESTAMP_NTZ
                )
            """).collect()
            already_parsed = session.sql(f"SELECT COUNT(*) FROM {parsed_t}").collect()[0][0]
            new_files = fc - already_parsed
            if new_files <= 0:
                pipe_bar.progress(step / steps_total, text=f"Step {step}/{steps_total} — {src['SOURCE_NAME']}: all {fc:,} files already parsed")
                pipe_log.text(f"⏭ {src['SOURCE_NAME']} — {fc:,} files already parsed, 0 new")
                continue
            pipe_bar.progress(step / steps_total, text=f"Step {step}/{steps_total} — Parsing {new_files:,} new files from {src['SOURCE_NAME']} ({already_parsed:,} already done)...")
            pipe_log.text(f"Parsing {src['SOURCE_NAME']} — {new_files:,} new + {already_parsed:,} already parsed of {fc:,} total")
            try:
                session.sql(f"""
                    INSERT INTO {parsed_t} (filename, filename_lower, parsed_content, file_size, parsed_at)
                    SELECT
                        d.RELATIVE_PATH,
                        LOWER(d.RELATIVE_PATH),
                        SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(
                            TO_FILE('{stage_at}', d.RELATIVE_PATH),
                            {{'mode': 'LAYOUT'}}
                        ):content::VARCHAR,
                        d.SIZE,
                        CURRENT_TIMESTAMP()
                    FROM DIRECTORY({stage_at}) d
                    WHERE (LOWER(d.RELATIVE_PATH) LIKE '%.pdf'
                       OR LOWER(d.RELATIVE_PATH) LIKE '%.docx'
                       OR LOWER(d.RELATIVE_PATH) LIKE '%.doc'
                       OR LOWER(d.RELATIVE_PATH) LIKE '%.tiff'
                       OR LOWER(d.RELATIVE_PATH) LIKE '%.tif')
                      AND NOT EXISTS (
                          SELECT 1 FROM {parsed_t} p WHERE p.filename = d.RELATIVE_PATH
                      )
                """).collect()
            except Exception as e:
                pipe_log.warning(f"Parse warning for {src['SOURCE_NAME']}: {e}")

        unified = fqn("CDSB_DOCUMENTS_UNIFIED")
        chunks = fqn("CDSB_CHUNKS")
        staging = fqn("_UNIFIED_STAGING")

        step += 1
        pipe_bar.progress(step / steps_total, text=f"Step {step}/{steps_total} — Preparing tables ({total_web_rows:,} web pages + {total_files:,} documents)...")
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {unified} (
                source_type VARCHAR, source_url VARCHAR, title VARCHAR,
                content VARCHAR, domain VARCHAR, content_hash VARCHAR,
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
        for col_name, col_type in [("content_hash", "VARCHAR"), ("updated_at", "TIMESTAMP_NTZ")]:
            try:
                session.sql(f"ALTER TABLE {unified} ADD COLUMN {col_name} {col_type}").collect()
            except Exception:
                pass
        try:
            session.sql(f"ALTER TABLE {unified} MODIFY COLUMN source_type VARCHAR(20)").collect()
        except Exception:
            pass
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {chunks} (
                chunk_id VARCHAR, chunk_content VARCHAR, source_type VARCHAR,
                title VARCHAR, source_url VARCHAR, domain VARCHAR
            )
        """).collect()
        try:
            session.sql(f"ALTER TABLE {chunks} MODIFY COLUMN source_type VARCHAR(20)").collect()
        except Exception:
            pass

        step += 1
        pipe_bar.progress(step / steps_total, text=f"Step {step}/{steps_total} — Building staging table from {total_web_rows:,} pages + {total_files:,} docs...")
        union_parts = []
        for _, src in sources_for_pipeline.iterrows():
            web_t = fqn(src["WEB_TABLE_NAME"])
            parsed_t = fqn(src["PARSED_TABLE_NAME"])
            domain = src["SOURCE_NAME"].lower()
            if object_exists("TABLE", web_t):
                union_parts.append(f"""
                    SELECT 'web' as source_type, url as source_url, title, content, domain, MD5(content) as content_hash
                    FROM {web_t} WHERE content IS NOT NULL AND LEN(content) > 50
                """)
            if object_exists("TABLE", parsed_t):
                union_parts.append(f"""
                    SELECT 'document' as source_type, filename as source_url,
                           REPLACE(filename, '.pdf', '') as title, parsed_content as content,
                           '{domain}' as domain, MD5(parsed_content) as content_hash
                    FROM {parsed_t} WHERE parsed_content IS NOT NULL AND LEN(parsed_content) > 50
                """)
        if not union_parts:
            pipe_bar.progress(1.0, text="Pipeline skipped — no data tables found.")
            pipe_log.warning("No web or document tables found for selected sources.")
        else:
            session.sql(
                f"CREATE OR REPLACE TABLE {staging} AS\n" + "\nUNION ALL\n".join(union_parts)
            ).collect()

            step += 1
            pipe_bar.progress(step / steps_total, text=f"Step {step}/{steps_total} — Merging into unified documents...")
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
            domains_in_scope = ", ".join(f"'{n.lower()}'" for n in selected_sources)
            session.sql(f"""
                DELETE FROM {unified} t WHERE t.domain IN ({domains_in_scope}) AND NOT EXISTS (
                    SELECT 1 FROM {staging} s WHERE s.source_url = t.source_url AND s.source_type = t.source_type
                )
            """).collect()

            step += 1
            pipe_bar.progress(step / steps_total, text=f"Step {step}/{steps_total} — Chunking changed documents...")
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

            pipe_bar.progress(1.0, text=f"Pipeline complete! {total_web_rows:,} pages + {total_files:,} docs processed — {changed:,} changed, {chunk_count:,} chunks.")
            pipe_log.success(f"Done — {total_web_rows:,} web pages + {total_files:,} documents processed. {changed:,} changed, {chunk_count:,} total chunks. Search service picks up changes incrementally.")

st.divider()

st.subheader("Quick Actions")
qc1, qc2 = st.columns(2)
with qc1:
    if st.button("Run Full Pipeline Only (Parse → Unify → Chunk)", use_container_width=True):
        session = conn.session()
        qa_bar = st.progress(0, text="Initialising pipeline...")
        qa_log = st.container()

        def _obj_exists(obj_type, obj_name):
            try:
                session.sql(f"DESCRIBE {obj_type} {obj_name}").collect()
                return True
            except Exception:
                return False

        sources_for_pipeline = conn.query(
            f"SELECT source_name, stage_name, web_table_name, parsed_table_name "
            f"FROM {fqn('EXTRACTION_SOURCES')} WHERE enabled = TRUE",
            ttl=0,
        )

        qa_file_counts = {}
        qa_total_files = 0
        for _, src in sources_for_pipeline.iterrows():
            sfqn = f"{DB}.{SCHEMA}.{src['STAGE_NAME']}"
            if _obj_exists("STAGE", sfqn):
                try:
                    cnt = session.sql(f"""
                        SELECT COUNT(*) FROM DIRECTORY(@{sfqn}) d
                        WHERE LOWER(d.RELATIVE_PATH) LIKE '%.pdf'
                           OR LOWER(d.RELATIVE_PATH) LIKE '%.docx'
                           OR LOWER(d.RELATIVE_PATH) LIKE '%.doc'
                           OR LOWER(d.RELATIVE_PATH) LIKE '%.tiff'
                           OR LOWER(d.RELATIVE_PATH) LIKE '%.tif'
                    """).collect()[0][0]
                except Exception:
                    cnt = 0
            else:
                cnt = 0
            qa_file_counts[src['SOURCE_NAME']] = cnt
            qa_total_files += cnt

        qa_web_counts = {}
        qa_total_web = 0
        for _, src in sources_for_pipeline.iterrows():
            wt = fqn(src['WEB_TABLE_NAME'])
            if _obj_exists("TABLE", wt):
                try:
                    rc = session.sql(f"SELECT COUNT(*) FROM {wt} WHERE content IS NOT NULL AND LEN(content) > 50").collect()[0][0]
                except Exception:
                    rc = 0
            else:
                rc = 0
            qa_web_counts[src['SOURCE_NAME']] = rc
            qa_total_web += rc

        total_sp = len(sources_for_pipeline)
        qa_steps = total_sp + 4
        qa_step = 0

        for idx, (_, src) in enumerate(sources_for_pipeline.iterrows()):
            qa_step += 1
            stage_fqn = f"{DB}.{SCHEMA}.{src['STAGE_NAME']}"
            stage_at = f"@{stage_fqn}"
            parsed_t = fqn(src['PARSED_TABLE_NAME'])
            fc = qa_file_counts.get(src['SOURCE_NAME'], 0)
            wc = qa_web_counts.get(src['SOURCE_NAME'], 0)
            if not _obj_exists("STAGE", stage_fqn) or fc == 0:
                qa_bar.progress(qa_step / qa_steps, text=f"Step {qa_step}/{qa_steps} — Skipping {src['SOURCE_NAME']} (no files)")
                qa_log.text(f"⏭ {src['SOURCE_NAME']} — {wc:,} web pages, 0 documents (no stage)")
                continue
            session.sql(f"""
                CREATE TABLE IF NOT EXISTS {parsed_t} (
                    filename VARCHAR, filename_lower VARCHAR, parsed_content VARCHAR,
                    file_size NUMBER, parsed_at TIMESTAMP_NTZ
                )
            """).collect()
            already_parsed = session.sql(f"SELECT COUNT(*) FROM {parsed_t}").collect()[0][0]
            new_files = fc - already_parsed
            if new_files <= 0:
                qa_bar.progress(qa_step / qa_steps, text=f"Step {qa_step}/{qa_steps} — {src['SOURCE_NAME']}: all {fc:,} files already parsed")
                qa_log.text(f"⏭ {src['SOURCE_NAME']} — {fc:,} files already parsed, 0 new")
                continue
            qa_bar.progress(qa_step / qa_steps, text=f"Step {qa_step}/{qa_steps} — Parsing {new_files:,} new files from {src['SOURCE_NAME']} ({already_parsed:,} already done)...")
            qa_log.text(f"Parsing {src['SOURCE_NAME']} — {new_files:,} new + {already_parsed:,} already parsed of {fc:,} total")
            try:
                session.sql(f"""
                    INSERT INTO {parsed_t} (filename, filename_lower, parsed_content, file_size, parsed_at)
                    SELECT
                        d.RELATIVE_PATH,
                        LOWER(d.RELATIVE_PATH),
                        SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(
                            TO_FILE('{stage_at}', d.RELATIVE_PATH),
                            {{'mode': 'LAYOUT'}}
                        ):content::VARCHAR,
                        d.SIZE,
                        CURRENT_TIMESTAMP()
                    FROM DIRECTORY({stage_at}) d
                    WHERE (LOWER(d.RELATIVE_PATH) LIKE '%.pdf'
                       OR LOWER(d.RELATIVE_PATH) LIKE '%.docx'
                       OR LOWER(d.RELATIVE_PATH) LIKE '%.doc'
                       OR LOWER(d.RELATIVE_PATH) LIKE '%.tiff'
                       OR LOWER(d.RELATIVE_PATH) LIKE '%.tif')
                      AND NOT EXISTS (
                          SELECT 1 FROM {parsed_t} p WHERE p.filename = d.RELATIVE_PATH
                      )
                """).collect()
            except Exception as e:
                qa_log.warning(f"Parse warning for {src['SOURCE_NAME']}: {e}")

        unified = fqn("CDSB_DOCUMENTS_UNIFIED")
        chunks = fqn("CDSB_CHUNKS")
        staging = fqn("_UNIFIED_STAGING")

        qa_step += 1
        qa_bar.progress(qa_step / qa_steps, text=f"Step {qa_step}/{qa_steps} — Preparing tables ({qa_total_web:,} web pages + {qa_total_files:,} documents)...")
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {unified} (
                source_type VARCHAR, source_url VARCHAR, title VARCHAR,
                content VARCHAR, domain VARCHAR, content_hash VARCHAR,
                updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
        for col_name, col_type in [("content_hash", "VARCHAR"), ("updated_at", "TIMESTAMP_NTZ")]:
            try:
                session.sql(f"ALTER TABLE {unified} ADD COLUMN {col_name} {col_type}").collect()
            except Exception:
                pass
        try:
            session.sql(f"ALTER TABLE {unified} MODIFY COLUMN source_type VARCHAR(20)").collect()
        except Exception:
            pass
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {chunks} (
                chunk_id VARCHAR, chunk_content VARCHAR, source_type VARCHAR,
                title VARCHAR, source_url VARCHAR, domain VARCHAR
            )
        """).collect()
        try:
            session.sql(f"ALTER TABLE {chunks} MODIFY COLUMN source_type VARCHAR(20)").collect()
        except Exception:
            pass

        qa_step += 1
        qa_bar.progress(qa_step / qa_steps, text=f"Step {qa_step}/{qa_steps} — Building staging table from {qa_total_web:,} pages + {qa_total_files:,} docs...")
        union_parts = []
        for _, src in sources_for_pipeline.iterrows():
            web_t = fqn(src["WEB_TABLE_NAME"])
            parsed_t = fqn(src["PARSED_TABLE_NAME"])
            domain = src["SOURCE_NAME"].lower()
            if _obj_exists("TABLE", web_t):
                union_parts.append(f"""
                    SELECT 'web' as source_type, url as source_url, title, content, domain, MD5(content) as content_hash
                    FROM {web_t} WHERE content IS NOT NULL AND LEN(content) > 50
                """)
            if _obj_exists("TABLE", parsed_t):
                union_parts.append(f"""
                    SELECT 'document' as source_type, filename as source_url,
                           REPLACE(filename, '.pdf', '') as title, parsed_content as content,
                           '{domain}' as domain, MD5(parsed_content) as content_hash
                    FROM {parsed_t} WHERE parsed_content IS NOT NULL AND LEN(parsed_content) > 50
                """)

        if not union_parts:
            qa_bar.progress(1.0, text="Pipeline skipped — no data tables found.")
            qa_log.warning("No data tables found. Run extraction first.")
        else:
            session.sql(
                f"CREATE OR REPLACE TABLE {staging} AS\n" + "\nUNION ALL\n".join(union_parts)
            ).collect()

            qa_step += 1
            qa_bar.progress(qa_step / qa_steps, text=f"Step {qa_step}/{qa_steps} — Merging into unified documents...")
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

            qa_step += 1
            qa_bar.progress(qa_step / qa_steps, text=f"Step {qa_step}/{qa_steps} — Chunking changed documents...")
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

            qa_bar.progress(1.0, text=f"Pipeline complete! {qa_total_web:,} pages + {qa_total_files:,} docs processed — {changed:,} changed, {chunk_count:,} chunks.")
            qa_log.success(f"Done — {qa_total_web:,} web pages + {qa_total_files:,} documents processed. {changed:,} changed, {chunk_count:,} total chunks. Search service picks up changes incrementally.")

with qc2:
    if st.button("Refresh All Stages", use_container_width=True):
        with st.spinner("Refreshing stages..."):
            session = conn.session()
            stages = conn.query(
                f"SELECT DISTINCT stage_name FROM {fqn('EXTRACTION_SOURCES')} WHERE enabled = TRUE",
                ttl=0,
            )
            for _, row in stages.iterrows():
                try:
                    session.sql(f"ALTER STAGE {DB}.{SCHEMA}.{row['STAGE_NAME']} REFRESH").collect()
                    st.text(f"Refreshed {row['STAGE_NAME']}")
                except Exception as e:
                    st.warning(f"Failed {row['STAGE_NAME']}: {e}")
            st.success("All stages refreshed!")
