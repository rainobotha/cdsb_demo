import os
import re
import hashlib
import time
import tempfile
import requests
from curl_cffi import requests as cffi_requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from collections import deque
import snowflake.connector

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}

SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".mp3", ".mp4", ".avi", ".mov", ".wmv",
    ".zip", ".tar", ".gz", ".rar",
    ".xml", ".json", ".rss", ".csv", ".xlsx",
}

SKIP_URL_PATTERNS = [
    r"smartjobs\.qld\.gov\.au",
    r"facebook\.com", r"linkedin\.com", r"youtube\.com",
    r"twitter\.com", r"instagram\.com",
    r"legislation\.qld\.gov\.au", r"parliament\.qld\.gov\.au",
    r"mailto:", r"tel:", r"javascript:",
    r"/login", r"/signup", r"/search\?",
    r"mode=results", r"result_page=",
    r"apps\.des\.qld\.gov\.au", r"oss\.qld\.gov\.au",
    r"sso\.qld\.gov\.au", r"api\.", r"cdn\.", r"static\.",
]

TMR_DOMAINS = {"www.tmr.qld.gov.au", "tmr.qld.gov.au"}
HEALTH_DOMAINS = {"www.health.qld.gov.au", "health.qld.gov.au"}
DPI_DOMAINS = {"www.dpi.qld.gov.au", "dpi.qld.gov.au"}
EDU_DOMAINS = {"education.qld.gov.au", "www.education.qld.gov.au"}
POLICE_DOMAINS = {"www.police.qld.gov.au", "police.qld.gov.au"}

CLOUDFLARE_DOMAINS = TMR_DOMAINS | HEALTH_DOMAINS | DPI_DOMAINS

DATABASE = os.getenv("CDSB_DATABASE", "CDSB_DEMO")
SCHEMA = os.getenv("CDSB_SCHEMA", "RAW")
WAREHOUSE = os.getenv("CDSB_WAREHOUSE", "COMPUTE_WH")

CKAN_API = "https://www.publications.qld.gov.au/api/3/action/package_search"


def fqn(name):
    return f"{DATABASE}.{SCHEMA}.{name}"


def stage_ref(name):
    return f"@{DATABASE}.{SCHEMA}.{name}"


def should_skip_url(url):
    parsed = urlparse(url)
    ext = os.path.splitext(parsed.path)[1].lower()
    if ext in SKIP_EXTENSIONS:
        return True
    for pattern in SKIP_URL_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    return False


def get_domain(url):
    return urlparse(url).netloc.lower()


def needs_cffi(url):
    domain = get_domain(url)
    return any(d in domain for d in CLOUDFLARE_DOMAINS)


def fetch_page(url):
    if needs_cffi(url):
        session = cffi_requests.Session(impersonate="chrome")
        return session.get(url, timeout=20, allow_redirects=True)
    return requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)


def fetch_pdf(url):
    if needs_cffi(url):
        session = cffi_requests.Session(impersonate="chrome")
        return session.get(url, timeout=60, allow_redirects=True)
    return requests.get(url, headers=HEADERS, timeout=60, allow_redirects=True)


def extract_text_content(soup):
    for tag in soup(["script", "style", "nav", "footer", "noscript", "iframe"]):
        tag.decompose()
    main = (
        soup.find("main")
        or soup.find("div", {"id": "content"})
        or soup.find("div", {"role": "main"})
        or soup.find("article")
        or soup.find("body")
    )
    if main:
        return main.get_text(separator="\n", strip=True)
    return soup.get_text(separator="\n", strip=True)


def crawl_site(base_url, domain_check_fn, max_pages=None, max_depth=None):
    visited = set()
    page_data = []
    pdf_links = set()
    publication_links = set()
    failed_urls = []

    queue = deque([(base_url, 0)])
    visited.add(base_url)
    count = 0

    while queue:
        if max_pages and count >= max_pages:
            print(f"\n  Reached page limit ({max_pages}), stopping crawl")
            break

        url, depth = queue.popleft()
        count += 1
        if count % 50 == 0:
            print(f"  --- Progress: {count} pages crawled, {len(queue)} in queue, {len(pdf_links)} PDFs found ---")

        time.sleep(0.15)

        try:
            resp = fetch_page(url)
            if resp.status_code != 200:
                if resp.status_code not in (301, 302, 404, 403):
                    failed_urls.append((url, resp.status_code))
                continue
            content_type = resp.headers.get("content-type", "")
            if "text/html" not in content_type:
                continue
        except Exception as e:
            failed_urls.append((url, str(e)))
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else url
        text = extract_text_content(soup)

        if text and len(text) > 50:
            page_data.append({
                "url": resp.url if hasattr(resp, 'url') else url,
                "title": title[:500],
                "content": text[:60000],
                "domain": get_domain(resp.url if hasattr(resp, 'url') else url),
            })

        if max_depth and depth >= max_depth:
            continue

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            if not href or href.startswith("#"):
                continue

            full_url = urljoin(url, href).split("#")[0]
            query_stripped = full_url.split("?")[0]

            if should_skip_url(full_url):
                continue
            if query_stripped.lower().endswith(".pdf"):
                pdf_links.add(full_url.split("?")[0])
                continue
            if "publications.qld.gov.au" in full_url and "/resource/" in full_url:
                publication_links.add(full_url.split("?")[0])
                continue
            if full_url in visited or query_stripped in visited:
                continue
            if domain_check_fn(full_url):
                visited.add(full_url)
                visited.add(query_stripped)
                queue.append((full_url, depth + 1))

    print(f"\nCrawl complete: {len(page_data)} pages, {len(pdf_links)} PDF links + {len(publication_links)} publication links")
    return page_data, pdf_links | publication_links, failed_urls


def enumerate_publications():
    datasets = []
    pdf_resources = []
    start = 0

    resp = requests.get(CKAN_API, params={"rows": 0}, headers=HEADERS, timeout=30)
    total = resp.json()["result"]["count"]
    print(f"  Publications portal: {total} datasets")

    while start < total:
        resp = requests.get(CKAN_API, params={"rows": 100, "start": start}, headers=HEADERS, timeout=30)
        results = resp.json()["result"]["results"]
        if not results:
            break

        for pkg in results:
            title = (pkg.get("title") or "").strip()
            notes = (pkg.get("notes") or "").strip()
            org = (pkg.get("organization") or {}).get("title", "")
            url = f"https://www.publications.qld.gov.au/dataset/{pkg['name']}"

            content_parts = []
            if title:
                content_parts.append(f"Title: {title}")
            if org:
                content_parts.append(f"Organisation: {org}")
            if notes:
                content_parts.append(f"\n{notes}")

            for r in pkg.get("resources", []):
                rname = (r.get("name") or "").strip()
                rdesc = (r.get("description") or "").strip()
                rfmt = (r.get("format") or "").upper()
                if rname:
                    content_parts.append(f"\nResource: {rname} ({rfmt})")
                if rdesc:
                    content_parts.append(rdesc)

            content = "\n".join(content_parts)
            if content and len(content) > 50:
                datasets.append({
                    "url": url,
                    "title": title[:500],
                    "content": content[:60000],
                    "domain": "www.publications.qld.gov.au",
                })

            for r in pkg.get("resources", []):
                fmt = (r.get("format") or "").upper()
                if fmt in ("PDF", ".PDF"):
                    rurl = r.get("url", "")
                    if rurl:
                        pdf_resources.append(rurl)

        start += 100

    print(f"  Enumeration done: {len(datasets)} datasets, {len(pdf_resources)} PDF resources")
    return datasets, pdf_resources


def download_pdfs(pdf_sources, download_dir):
    os.makedirs(download_dir, exist_ok=True)
    downloaded = []
    seen_filenames = set()

    for pdf_url in sorted(pdf_sources):
        try:
            resp = fetch_pdf(pdf_url)
            if resp.status_code != 200:
                continue

            content_type = resp.headers.get("content-type", "")
            if "pdf" not in content_type and "octet" not in content_type:
                if "html" in content_type:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    dl_link = soup.find("a", href=re.compile(r"\.(pdf|PDF)"))
                    if not dl_link:
                        dl_link = soup.find("a", string=re.compile(r"(download|Download|PDF)"))
                    if dl_link:
                        actual_url = urljoin(resp.url if hasattr(resp, 'url') else pdf_url, dl_link["href"])
                        resp = fetch_pdf(actual_url)
                        if resp.status_code != 200:
                            continue
                    else:
                        continue

            if len(resp.content) < 500:
                continue

            final_url = resp.url if hasattr(resp, 'url') else pdf_url
            filename = urlparse(final_url).path.split("/")[-1]
            if not filename or not filename.lower().endswith(".pdf"):
                filename = hashlib.md5(pdf_url.encode()).hexdigest() + ".pdf"
            filename = re.sub(r'[^\w\-.]', '_', filename)

            if filename in seen_filenames:
                base, ext = os.path.splitext(filename)
                counter = 1
                while f"{base}_{counter}{ext}" in seen_filenames:
                    counter += 1
                filename = f"{base}_{counter}{ext}"

            seen_filenames.add(filename)
            filepath = os.path.join(download_dir, filename)

            with open(filepath, "wb") as f:
                f.write(resp.content)
            downloaded.append({
                "url": pdf_url,
                "filename": filename,
                "filepath": filepath,
                "size_bytes": len(resp.content),
            })
        except Exception:
            pass

    print(f"Downloaded {len(downloaded)} PDFs")
    return downloaded


def get_snowflake_conn():
    if os.path.exists("/snowflake/session/token"):
        return snowflake.connector.connect(
            host=os.environ["SNOWFLAKE_HOST"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            token=open("/snowflake/session/token").read(),
            authenticator="oauth",
            database=DATABASE,
            schema=SCHEMA,
            warehouse=WAREHOUSE,
        )
    conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "AU_DEMO50")
    conn = snowflake.connector.connect(connection_name=conn_name)
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {DATABASE}")
    cur.execute(f"USE SCHEMA {SCHEMA}")
    cur.execute(f"USE WAREHOUSE {WAREHOUSE}")
    cur.close()
    return conn


def get_existing_hashes(cur, table):
    try:
        cur.execute(f"SELECT url, MD5(content) as hash FROM {table}")
        return {row[0]: row[1] for row in cur.fetchall()}
    except Exception:
        return {}


def get_existing_pdfs(cur, table):
    try:
        cur.execute(f"SELECT filename FROM {table}")
        return {row[0] for row in cur.fetchall()}
    except Exception:
        return set()


def sync_data(cur, page_data, downloaded_pdfs, web_table, pdf_table, stage):
    existing_hashes = get_existing_hashes(cur, web_table)
    existing_pdfs = get_existing_pdfs(cur, pdf_table)

    new_pages = []
    updated_pages = []
    for page in page_data:
        page_hash = hashlib.md5(page["content"].encode()).hexdigest()
        if page["url"] not in existing_hashes:
            new_pages.append(page)
        elif existing_hashes[page["url"]] != page_hash:
            updated_pages.append(page)

    if updated_pages:
        for page in updated_pages:
            cur.execute(
                f"UPDATE {web_table} SET title=%s, content=%s, domain=%s, scraped_at=CURRENT_TIMESTAMP() WHERE url=%s",
                (page["title"], page["content"], page["domain"], page["url"])
            )
        print(f"  Updated {len(updated_pages)} changed pages in {web_table}")

    if new_pages:
        cur.execute("BEGIN")
        for page in new_pages:
            cur.execute(
                f"INSERT INTO {web_table} (url, title, content, domain) VALUES (%s, %s, %s, %s)",
                (page["url"], page["title"], page["content"], page["domain"])
            )
        cur.execute("COMMIT")
        print(f"  Inserted {len(new_pages)} new pages in {web_table}")

    new_pdfs = [p for p in downloaded_pdfs if p["filename"] not in existing_pdfs]
    if new_pdfs:
        for pdf in new_pdfs:
            cur.execute(
                f"PUT 'file://{pdf['filepath']}' @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
        cur.execute(f"ALTER STAGE {stage} REFRESH")

        cur.execute("BEGIN")
        for pdf in new_pdfs:
            cur.execute(
                f"INSERT INTO {pdf_table} (filename, source_url, size_bytes, stage_path) VALUES (%s, %s, %s, %s)",
                (pdf["filename"], pdf["url"], pdf["size_bytes"], f"@{stage}/{pdf['filename']}")
            )
        cur.execute("COMMIT")
        print(f"  Uploaded {len(new_pdfs)} new PDFs to {stage}")

    return len(new_pages) + len(updated_pages) + len(new_pdfs)


def rebuild_pipeline(cur):
    parse_jobs = [
        ("CDSB PDFs", fqn("PDF_PARSED"), stage_ref("CDSB_DOCUMENTS")),
        ("QLD Gov PDFs", fqn("QLD_GOV_PDF_PARSED"), stage_ref("QLD_GOV_DOCUMENTS")),
        ("TMR PDFs", fqn("TMR_PDF_PARSED"), stage_ref("TMR_DOCUMENTS")),
        ("EDU PDFs", fqn("EDU_PDF_PARSED"), stage_ref("EDU_DOCUMENTS")),
        ("HEALTH PDFs", fqn("HEALTH_PDF_PARSED"), stage_ref("HEALTH_DOCUMENTS")),
        ("DPI PDFs", fqn("DPI_PDF_PARSED"), stage_ref("DPI_DOCUMENTS")),
        ("Police PDFs", fqn("POLICE_PDF_PARSED"), stage_ref("POLICE_DOCUMENTS")),
        ("Publications PDFs", fqn("PUBLICATIONS_PDF_PARSED"), stage_ref("PUBLICATIONS_DOCUMENTS")),
        ("QPS Reports", fqn("QPS_REPORTS_PARSED"), stage_ref("QPS_REPORTS")),
    ]

    for label, table, stage_at in parse_jobs:
        print(f"  Parsing {label}...")
        cur.execute(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT
                d.RELATIVE_PATH as filename,
                SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(
                    TO_FILE('{stage_at}', d.RELATIVE_PATH),
                    {{'mode': 'LAYOUT'}}
                ):content::VARCHAR as parsed_content,
                d.SIZE as file_size,
                CURRENT_TIMESTAMP() as parsed_at
            FROM DIRECTORY({stage_at}) d
            WHERE LOWER(d.RELATIVE_PATH) LIKE '%.pdf'
        """)

    unified_table = fqn("CDSB_DOCUMENTS_UNIFIED")
    chunks_table = fqn("CDSB_CHUNKS")

    source_configs = [
        ("WEB_PAGES", "PDF_PARSED", "PDF_METADATA", "cdsb.qld.gov.au"),
        ("QLD_GOV_WEB_PAGES", "QLD_GOV_PDF_PARSED", "QLD_GOV_PDF_METADATA", "qld.gov.au"),
        ("TMR_WEB_PAGES", "TMR_PDF_PARSED", "TMR_PDF_METADATA", "tmr.qld.gov.au"),
        ("EDU_WEB_PAGES", "EDU_PDF_PARSED", "EDU_PDF_METADATA", "education.qld.gov.au"),
        ("HEALTH_WEB_PAGES", "HEALTH_PDF_PARSED", "HEALTH_PDF_METADATA", "health.qld.gov.au"),
        ("DPI_WEB_PAGES", "DPI_PDF_PARSED", "DPI_PDF_METADATA", "dpi.qld.gov.au"),
        ("POLICE_WEB_PAGES", "POLICE_PDF_PARSED", "POLICE_PDF_METADATA", "police.qld.gov.au"),
        ("PUBLICATIONS_WEB_PAGES", "PUBLICATIONS_PDF_PARSED", "PUBLICATIONS_PDF_METADATA", "publications.qld.gov.au"),
    ]

    union_parts = []
    for web_t, pdf_t, meta_t, domain in source_configs:
        union_parts.append(f"""
            SELECT 'web' as source_type, url as source_url, title, content, domain
            FROM {fqn(web_t)}
            WHERE content IS NOT NULL AND LEN(content) > 50
        """)
        union_parts.append(f"""
            SELECT 'pdf' as source_type, COALESCE(m.source_url, p.filename) as source_url,
                   REPLACE(p.filename, '.pdf', '') as title, p.parsed_content as content, '{domain}' as domain
            FROM {fqn(pdf_t)} p
            LEFT JOIN {fqn(meta_t)} m ON m.filename = p.filename
            WHERE p.parsed_content IS NOT NULL AND LEN(p.parsed_content) > 50
        """)

    union_parts.append(f"""
        SELECT 'pdf' as source_type, p.filename as source_url,
               REPLACE(p.filename, '.pdf', '') as title, p.parsed_content as content, 'police.qld.gov.au' as domain
        FROM {fqn("QPS_REPORTS_PARSED")} p
        WHERE p.parsed_content IS NOT NULL AND LEN(p.parsed_content) > 50
    """)

    print("  Building unified documents...")
    cur.execute(f"CREATE OR REPLACE TABLE {unified_table} AS\n" + "\nUNION ALL\n".join(union_parts))

    print("  Creating chunks...")
    cur.execute(f"""
        CREATE OR REPLACE TABLE {chunks_table} AS
        WITH RECURSIVE numbered_docs AS (
            SELECT ROW_NUMBER() OVER (ORDER BY source_type, source_url) as doc_id,
                   source_type, source_url, title, content, domain
            FROM {unified_table}
        ),
        chunks AS (
            SELECT doc_id, source_type, source_url, title, domain, 1 as chunk_num,
                   SUBSTR(content, 1, 1500) as chunk_content, LEN(content) as total_len
            FROM numbered_docs
            UNION ALL
            SELECT c.doc_id, c.source_type, c.source_url, c.title, c.domain, c.chunk_num + 1,
                   SUBSTR(d.content, 1 + (c.chunk_num * 1000), 1500), c.total_len
            FROM chunks c JOIN numbered_docs d ON c.doc_id = d.doc_id
            WHERE 1 + (c.chunk_num * 1000) <= c.total_len
        )
        SELECT doc_id || '-' || chunk_num as chunk_id, chunk_content, source_type, title, source_url, domain
        FROM chunks WHERE LEN(chunk_content) > 50
        ORDER BY doc_id, chunk_num
    """)

    print("  Pipeline rebuild complete! (Search service will auto-refresh)")


SITES = [
    {"name": "CDSB", "url": "https://www.cdsb.qld.gov.au",
     "domains": {"www.cdsb.qld.gov.au", "cdsb.qld.gov.au"},
     "web_table": fqn("WEB_PAGES"), "pdf_table": fqn("PDF_METADATA"), "stage": fqn("CDSB_DOCUMENTS")},
    {"name": "QLD", "url": "https://www.qld.gov.au",
     "check": lambda url: get_domain(url).endswith(".qld.gov.au") or get_domain(url) == "qld.gov.au",
     "max_pages": 3000, "max_depth": 5,
     "web_table": fqn("QLD_GOV_WEB_PAGES"), "pdf_table": fqn("QLD_GOV_PDF_METADATA"), "stage": fqn("QLD_GOV_DOCUMENTS")},
    {"name": "TMR", "url": "https://www.tmr.qld.gov.au",
     "domains": TMR_DOMAINS, "max_pages": 5000, "max_depth": 5,
     "web_table": fqn("TMR_WEB_PAGES"), "pdf_table": fqn("TMR_PDF_METADATA"), "stage": fqn("TMR_DOCUMENTS")},
    {"name": "EDU", "url": "https://education.qld.gov.au/",
     "domains": EDU_DOMAINS, "max_pages": 5000, "max_depth": 5,
     "web_table": fqn("EDU_WEB_PAGES"), "pdf_table": fqn("EDU_PDF_METADATA"), "stage": fqn("EDU_DOCUMENTS")},
    {"name": "HEALTH", "url": "https://www.health.qld.gov.au/",
     "domains": HEALTH_DOMAINS, "max_pages": 5000, "max_depth": 5,
     "web_table": fqn("HEALTH_WEB_PAGES"), "pdf_table": fqn("HEALTH_PDF_METADATA"), "stage": fqn("HEALTH_DOCUMENTS")},
    {"name": "DPI", "url": "https://www.dpi.qld.gov.au/",
     "domains": DPI_DOMAINS, "max_pages": 5000, "max_depth": 5,
     "web_table": fqn("DPI_WEB_PAGES"), "pdf_table": fqn("DPI_PDF_METADATA"), "stage": fqn("DPI_DOCUMENTS")},
    {"name": "POLICE", "url": "https://www.police.qld.gov.au/",
     "domains": POLICE_DOMAINS, "max_pages": 5000, "max_depth": 5,
     "web_table": fqn("POLICE_WEB_PAGES"), "pdf_table": fqn("POLICE_PDF_METADATA"), "stage": fqn("POLICE_DOCUMENTS")},
]


if __name__ == "__main__":
    print("=" * 60)
    print("QLD Gov Multi-Site Hourly Scraper (8 sources + publications)")
    print("=" * 60)

    total_changes = 0
    all_results = {}
    n = len(SITES)

    with tempfile.TemporaryDirectory() as tmpdir:
        for i, site in enumerate(SITES):
            name = site["name"]
            check_fn = site.get("check", lambda url, d=site.get("domains", set()): get_domain(url) in d)
            mp = site.get("max_pages")
            md = site.get("max_depth")

            print(f"\n[{i*2+1}/{n*2+2}] Crawling {site['url']}...")
            pages, pdfs, _ = crawl_site(site["url"], check_fn, max_pages=mp, max_depth=md)

            dl_dir = os.path.join(tmpdir, name.lower())
            print(f"[{i*2+2}/{n*2+2}] Downloading {name} PDFs ({len(pdfs)})...")
            downloaded = download_pdfs(pdfs, dl_dir)

            all_results[name] = {"pages": pages, "downloaded": downloaded}

        print(f"\n[{n*2+1}/{n*2+2}] Enumerating publications via CKAN API...")
        pub_datasets, pub_pdf_urls = enumerate_publications()
        pub_dl_dir = os.path.join(tmpdir, "publications")
        pub_downloaded = download_pdfs(set(pub_pdf_urls), pub_dl_dir)
        all_results["PUBLICATIONS"] = {"pages": pub_datasets, "downloaded": pub_downloaded}

        conn = get_snowflake_conn()
        cur = conn.cursor()
        try:
            for site in SITES:
                name = site["name"]
                r = all_results[name]
                print(f"\nSyncing {name} data...")
                total_changes += sync_data(
                    cur, r["pages"], r["downloaded"],
                    site["web_table"], site["pdf_table"], site["stage"],
                )

            pub_r = all_results["PUBLICATIONS"]
            print("\nSyncing PUBLICATIONS data...")
            total_changes += sync_data(
                cur, pub_r["pages"], pub_r["downloaded"],
                fqn("PUBLICATIONS_WEB_PAGES"), fqn("PUBLICATIONS_PDF_METADATA"), fqn("PUBLICATIONS_DOCUMENTS"),
            )

            if total_changes > 0:
                print(f"\nRebuilding pipeline ({total_changes} changes)...")
                rebuild_pipeline(cur)
            else:
                print("\nNo changes detected, skipping pipeline rebuild")
        finally:
            cur.close()
            conn.close()

    for site in SITES:
        r = all_results[site["name"]]
        print(f"  {site['name']}: {len(r['pages'])} pages, {len(r['downloaded'])} PDFs")
    pub_r = all_results["PUBLICATIONS"]
    print(f"  PUBLICATIONS: {len(pub_r['pages'])} datasets, {len(pub_r['downloaded'])} PDFs")
    print(f"Total changes: {total_changes}")
