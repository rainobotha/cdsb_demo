import os
import sys
import json
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
from engine import CrawlProgress, run_extraction, create_connection_pool, close_pool

CONN_NAME = os.getenv("SNOWFLAKE_CONNECTION_NAME", "AU_DEMO50")
DATABASE = os.getenv("CDSB_DATABASE", "CDSB_DEMO")
SCHEMA = os.getenv("CDSB_SCHEMA", "RAW")
WAREHOUSE = os.getenv("CDSB_WAREHOUSE", "COMPUTE_WH")


def get_conn():
    return snowflake.connector.connect(connection_name=CONN_NAME)


def init_session(cur):
    cur.execute(f"USE DATABASE {DATABASE}")
    cur.execute(f"USE SCHEMA {SCHEMA}")
    cur.execute(f"USE WAREHOUSE {WAREHOUSE}")


def fqn(name):
    return f"{DATABASE}.{SCHEMA}.{name}"


def load_sources(cur, source_filter=None):
    cur.execute(f"SELECT * FROM {fqn('EXTRACTION_SOURCES')} WHERE enabled = TRUE ORDER BY source_name")
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    sources = []
    for row in rows:
        d = dict(zip(cols, row))
        d_lower = {k.lower(): v for k, v in d.items()}
        if d_lower.get("allowed_domains"):
            val = d_lower["allowed_domains"]
            if isinstance(val, str):
                try:
                    val = json.loads(val)
                except Exception:
                    val = [val]
            d_lower["allowed_domains"] = val if isinstance(val, list) else [val]
        else:
            d_lower["allowed_domains"] = []

        if d_lower.get("file_types"):
            val = d_lower["file_types"]
            if isinstance(val, str):
                try:
                    val = json.loads(val)
                except Exception:
                    val = [val]
            d_lower["file_types"] = val if isinstance(val, list) else [val]
        else:
            d_lower["file_types"] = ["pdf"]

        if d_lower.get("direct_urls"):
            val = d_lower["direct_urls"]
            if isinstance(val, str):
                try:
                    val = json.loads(val)
                except Exception:
                    val = [val]
            d_lower["direct_urls"] = val if isinstance(val, list) else [val]

        sources.append(d_lower)

    if source_filter:
        names = [n.strip().lower() for n in source_filter.split(",")]
        sources = [s for s in sources if s["source_name"].lower() in names]

    return sources


def main():
    source_filter = None
    run_pipeline_flag = True
    for arg in sys.argv[1:]:
        if arg.startswith("--sources="):
            source_filter = arg.split("=", 1)[1]
        elif arg == "--no-pipeline":
            run_pipeline_flag = False

    conn = get_conn()
    cur = conn.cursor()
    init_session(cur)

    sources = load_sources(cur, source_filter)
    cur.close()
    conn.close()

    if not sources:
        print("No matching enabled sources found.")
        return

    print("=" * 60)
    print(f"CDSB Extraction Platform — {len(sources)} source(s)")
    print("=" * 60)
    for s in sources:
        print(f"  {s['source_name']}: {s['crawl_method']} → {s['source_url']}")
    print()

    def extract_source(src):
        name = src['source_name']
        progress = CrawlProgress()
        try:
            result = run_extraction(src, get_conn, init_session, DATABASE, SCHEMA, progress)
            print(f"  [{name}] Result: {result}")

            uc = get_conn()
            ucur = uc.cursor()
            init_session(ucur)
            ucur.execute(f"""
                UPDATE {fqn('EXTRACTION_SOURCES')}
                SET last_run_at = CURRENT_TIMESTAMP(),
                    last_run_status = 'SUCCESS',
                    last_run_pages = {result.get('pages', 0)},
                    last_run_files = {result.get('uploaded', 0)}
                WHERE LOWER(source_name) = LOWER('{name}')
            """)
            ucur.close()
            uc.close()
            return name, "SUCCESS", result

        except Exception as e:
            print(f"  [{name}] ERROR: {e}")
            uc = get_conn()
            ucur = uc.cursor()
            init_session(ucur)
            ucur.execute(f"""
                UPDATE {fqn('EXTRACTION_SOURCES')}
                SET last_run_at = CURRENT_TIMESTAMP(),
                    last_run_status = 'FAILED'
                WHERE LOWER(source_name) = LOWER('{name}')
            """)
            ucur.close()
            uc.close()
            return name, "FAILED", str(e)

    parallel_sources = min(len(sources), 4)
    print(f"Running {len(sources)} source(s) with {parallel_sources} parallel workers...\n")

    with ThreadPoolExecutor(max_workers=parallel_sources) as executor:
        futures = {executor.submit(extract_source, src): src for src in sources}
        for fut in as_completed(futures):
            name, status, detail = fut.result()
            print(f"  [{name}] {status}")

    if run_pipeline_flag:
        print("\nRunning pipeline (parse → unify → chunk)...")
        from pipeline import run_pipeline as do_pipeline
        conn = get_conn()
        cur = conn.cursor()
        init_session(cur)
        try:
            do_pipeline(cur, sources, DATABASE, SCHEMA, progress)
        finally:
            cur.close()
            conn.close()

    print("\nDone!")


if __name__ == "__main__":
    main()
