import streamlit as st
import json
from datetime import timedelta

conn = st.connection("snowflake")
DB = st.session_state.get("cdsb_database", "CDSB_DEMO")
SCHEMA = st.session_state.get("cdsb_schema", "RAW")


def fqn(name):
    return f"{DB}.{SCHEMA}.{name}"


def load_sources():
    return conn.query(
        f"SELECT * FROM {fqn('EXTRACTION_SOURCES')} ORDER BY source_id",
        ttl=timedelta(seconds=30),
    )


def format_array(arr):
    if arr is None:
        return ""
    if isinstance(arr, str):
        try:
            arr = json.loads(arr)
        except Exception:
            return arr
    if isinstance(arr, list):
        return ", ".join(str(x) for x in arr)
    return str(arr)


sources_df = load_sources()

col1, col2 = st.columns([3, 1])
with col1:
    st.markdown("Configure extraction targets — each source defines a website or API to crawl.")
with col2:
    if st.button("Refresh", icon=":material/refresh:"):
        st.cache_data.clear()
        st.rerun()

st.divider()

tab_list, tab_add, tab_edit = st.tabs(["All Sources", "Add Source", "Edit Source"])

with tab_list:
    if sources_df.empty:
        st.info("No sources configured yet. Add one below.")
    else:
        for _, row in sources_df.iterrows():
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
                with c1:
                    status_icon = ":material/check_circle:" if row["ENABLED"] else ":material/cancel:"
                    st.markdown(f"**{row['SOURCE_NAME']}** {status_icon}")
                    st.caption(row["SOURCE_URL"])
                with c2:
                    st.markdown(f"Method: `{row['CRAWL_METHOD']}`")
                    ft = format_array(row.get("FILE_TYPES"))
                    st.caption(f"Files: {ft}")
                with c3:
                    mp = row.get("MAX_PAGES") or "—"
                    md = row.get("MAX_DEPTH") or "—"
                    st.caption(f"Pages: {mp} | Depth: {md}")
                    if row.get("CLOUDFLARE_BYPASS"):
                        st.caption(":material/shield: Cloudflare bypass")
                with c4:
                    last = row.get("LAST_RUN_STATUS") or "Never"
                    st.caption(f"Last: {last}")

with tab_add:
    with st.form("add_source", clear_on_submit=True):
        st.subheader("Add New Source")

        a_name = st.text_input("Source Name", placeholder="e.g. Transport QLD")
        a_url = st.text_input("Source URL", placeholder="https://www.example.qld.gov.au")

        ac1, ac2 = st.columns(2)
        with ac1:
            a_method = st.selectbox(
                "Crawl Method",
                ["BFS", "CKAN", "TARGETED"],
                help=(
                    "**BFS** — Breadth-first web crawl. Starts at the Source URL and "
                    "follows links across pages, staying within the Allowed Domains. "
                    "Discovers and downloads documents as it goes. Best for sites you "
                    "want to explore broadly.\n\n"
                    "**CKAN** — Queries a CKAN data-catalogue API (e.g. publications.qld.gov.au) "
                    "to enumerate every published dataset and its resource files. Requires "
                    "the CKAN API URL field below.\n\n"
                    "**TARGETED** — Downloads a fixed list of URLs you provide in the "
                    "Direct URLs field. No crawling — just fetches exactly the files you specify."
                ),
            )
        with ac2:
            a_cffi = st.checkbox(
                "Cloudflare Bypass (curl_cffi)",
                help="Enable for sites protected by Cloudflare. Uses browser impersonation to avoid being blocked.",
            )

        a_domains = st.text_input(
            "Allowed Domains (comma-separated)",
            placeholder="www.example.qld.gov.au, example.qld.gov.au",
        )
        a_file_types = st.multiselect(
            "File Types to Download",
            ["pdf", "docx", "doc", "csv", "xlsx", "xls", "tiff"],
            default=["pdf"],
        )

        ac3, ac4 = st.columns(2)
        with ac3:
            a_max_pages = st.number_input("Max Pages", min_value=0, value=5000, step=500)
        with ac4:
            a_max_depth = st.number_input("Max Depth", min_value=0, value=5, step=1)

        a_stage = st.text_input("Stage Name", placeholder="MY_DOCUMENTS")
        a_web_table = st.text_input("Web Pages Table", placeholder="MY_WEB_PAGES")
        a_parsed_table = st.text_input("Parsed Table", placeholder="MY_PDF_PARSED")

        a_sitemap = st.text_input("Sitemap URL (optional)", placeholder="https://...")
        a_ckan = st.text_input("CKAN API URL (optional)", placeholder="https://...")

        submitted = st.form_submit_button("Add Source", type="primary")

        if submitted:
            missing = []
            if not a_name:
                missing.append("Source Name")
            if not a_url:
                missing.append("Source URL")
            if not a_stage:
                missing.append("Stage Name")
            if not a_web_table:
                missing.append("Web Pages Table")
            if not a_parsed_table:
                missing.append("Parsed Table")
            if missing:
                st.error(f"Please fill in the required fields: {', '.join(missing)}")
            else:
                domains_list = [d.strip() for d in a_domains.split(",") if d.strip()] if a_domains else []
                domains_json = json.dumps(domains_list) if domains_list else None
                ft_json = json.dumps(a_file_types) if a_file_types else '["pdf"]'

                session = conn.session()
                session.sql(f"""
                    INSERT INTO {fqn('EXTRACTION_SOURCES')}
                    (source_name, source_url, crawl_method, allowed_domains, cloudflare_bypass,
                     file_types, max_pages, max_depth, stage_name, web_table_name, parsed_table_name,
                     sitemap_url, ckan_api_url)
                    SELECT :1, :2, :3, PARSE_JSON(:4), :5,
                           PARSE_JSON(:6), :7, :8,
                           :9, :10, :11,
                           :12, :13
                """, params=[
                    a_name, a_url, a_method, domains_json, a_cffi,
                    ft_json, a_max_pages or None, a_max_depth or None,
                    a_stage, a_web_table, a_parsed_table,
                    a_sitemap or None, a_ckan or None,
                ]).collect()
                st.success(f"Source '{a_name}' added!")
                st.cache_data.clear()
                st.rerun()

with tab_edit:
    if sources_df.empty:
        st.info("No sources to edit.")
    else:
        source_names = sources_df["SOURCE_NAME"].tolist()
        selected = st.selectbox("Select Source", source_names, key="edit_source_select")
        row = sources_df[sources_df["SOURCE_NAME"] == selected].iloc[0]
        sid = int(row["SOURCE_ID"])

        with st.form(f"edit_source_form_{sid}"):
            st.subheader(f"Edit: {selected}")

            ec1, ec2 = st.columns(2)
            with ec1:
                new_enabled = st.toggle("Enabled", value=bool(row["ENABLED"]), key=f"edit_enabled_{sid}")
                new_url = st.text_input("Source URL", value=row["SOURCE_URL"] or "", key=f"edit_url_{sid}")
            with ec2:
                methods = ["BFS", "CKAN", "TARGETED"]
                cur_method = row["CRAWL_METHOD"] if row["CRAWL_METHOD"] in methods else "BFS"
                new_method = st.selectbox(
                    "Crawl Method", methods, index=methods.index(cur_method), key=f"edit_method_{sid}",
                    help=(
                        "**BFS** — Breadth-first web crawl.\n\n"
                        "**CKAN** — Queries a CKAN data-catalogue API.\n\n"
                        "**TARGETED** — Downloads a fixed list of URLs."
                    ),
                )
                new_cffi = st.checkbox(
                    "Cloudflare Bypass", value=bool(row.get("CLOUDFLARE_BYPASS")), key=f"edit_cffi_{sid}",
                )

            cur_domains = format_array(row.get("ALLOWED_DOMAINS"))
            new_domains = st.text_input(
                "Allowed Domains (comma-separated)", value=cur_domains, key=f"edit_domains_{sid}",
                help="Required for BFS. e.g. www.example.com, example.com",
            )

            all_ft = ["pdf", "docx", "doc", "csv", "xlsx", "xls", "tiff"]
            cur_ft_raw = row.get("FILE_TYPES")
            if isinstance(cur_ft_raw, str):
                try:
                    cur_ft = json.loads(cur_ft_raw)
                except Exception:
                    cur_ft = [cur_ft_raw]
            elif isinstance(cur_ft_raw, list):
                cur_ft = cur_ft_raw
            else:
                cur_ft = ["pdf"]
            new_file_types = st.multiselect(
                "File Types", all_ft, default=[f for f in cur_ft if f in all_ft], key=f"edit_ft_{sid}",
            )

            ec3, ec4 = st.columns(2)
            with ec3:
                new_max_pages = st.number_input(
                    "Max Pages", value=int(row["MAX_PAGES"] or 5000), key=f"edit_max_pages_{sid}",
                )
            with ec4:
                new_max_depth = st.number_input(
                    "Max Depth", value=int(row["MAX_DEPTH"] or 5), key=f"edit_max_depth_{sid}",
                )

            new_sitemap = st.text_input(
                "Sitemap URL (optional)", value=row.get("SITEMAP_URL") or "", key=f"edit_sitemap_{sid}",
            )
            new_ckan = st.text_input(
                "CKAN API URL (optional)", value=row.get("CKAN_API_URL") or "", key=f"edit_ckan_{sid}",
            )

            cur_direct = format_array(row.get("DIRECT_URLS"))
            new_direct = st.text_area(
                "Direct URLs (one per line, for TARGETED)", value=cur_direct.replace(", ", "\n"),
                key=f"edit_direct_{sid}", height=100,
            )

            with st.expander("Storage Names", expanded=False):
                new_stage = st.text_input("Stage Name", value=row.get("STAGE_NAME") or "", key=f"edit_stage_{sid}")
                new_web_t = st.text_input("Web Table", value=row.get("WEB_TABLE_NAME") or "", key=f"edit_web_t_{sid}")
                new_parsed_t = st.text_input("Parsed Table", value=row.get("PARSED_TABLE_NAME") or "", key=f"edit_parsed_t_{sid}")

            fc1, fc2 = st.columns(2)
            with fc1:
                save_clicked = st.form_submit_button("Save Changes", type="primary")
            with fc2:
                delete_clicked = st.form_submit_button("Delete Source")

        if save_clicked:
            session = conn.session()
            domains_list = [d.strip() for d in new_domains.split(",") if d.strip()] if new_domains else []
            domains_json = json.dumps(domains_list) if domains_list else None
            ft_json = json.dumps(new_file_types) if new_file_types else '["pdf"]'
            direct_list = [u.strip() for u in new_direct.split("\n") if u.strip()] if new_direct else []
            direct_json = json.dumps(direct_list) if direct_list else None

            session.sql(f"""
                UPDATE {fqn('EXTRACTION_SOURCES')}
                SET source_url = :1, crawl_method = :2, allowed_domains = :3,
                    cloudflare_bypass = :4, file_types = :5,
                    max_pages = :6, max_depth = :7,
                    sitemap_url = :8, ckan_api_url = :9, direct_urls = :10,
                    stage_name = :11, web_table_name = :12, parsed_table_name = :13,
                    enabled = :14, updated_at = CURRENT_TIMESTAMP()
                WHERE source_id = :15
            """, params=[
                new_url, new_method, domains_json, new_cffi, ft_json,
                new_max_pages, new_max_depth,
                new_sitemap or None, new_ckan or None, direct_json,
                new_stage, new_web_t, new_parsed_t,
                new_enabled, int(row["SOURCE_ID"]),
            ]).collect()
            st.success("Updated!")
            st.cache_data.clear()
            st.rerun()

        if delete_clicked:
            session = conn.session()
            session.sql(
                f"DELETE FROM {fqn('EXTRACTION_SOURCES')} WHERE source_id = {row['SOURCE_ID']}"
            ).collect()
            st.success("Deleted!")
            st.cache_data.clear()
            st.rerun()
