import os
import re
import hashlib
import time
import tempfile
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from urllib.parse import urljoin, urlparse
from collections import deque
from fnmatch import fnmatch
import requests
from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}

SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".mp3", ".mp4", ".avi", ".mov", ".wmv",
    ".zip", ".tar", ".gz", ".rar",
    ".xml", ".json", ".rss",
}

SKIP_URL_PATTERNS = [
    r"facebook\.com", r"linkedin\.com", r"youtube\.com",
    r"twitter\.com", r"instagram\.com", r"mailto:", r"tel:",
    r"javascript:", r"/login", r"/signup", r"/search\?",
    r"sso\.qld\.gov\.au", r"api\.", r"cdn\.", r"static\.",
    r"smartjobs\.qld\.gov\.au", r"legislation\.qld\.gov\.au",
    r"mode=results", r"result_page=",
    r"apps\.des\.qld\.gov\.au", r"oss\.qld\.gov\.au",
]

DOWNLOADABLE_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".csv", ".xlsx", ".xls", ".tiff", ".tif",
}


def should_skip_url(url):
    parsed = urlparse(url)
    ext = os.path.splitext(parsed.path)[1].lower()
    if ext in SKIP_EXTENSIONS:
        return True
    for pattern in SKIP_URL_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    return False


def is_downloadable(url, allowed_types):
    parsed = urlparse(url)
    ext = os.path.splitext(parsed.path)[1].lower()
    if ext in DOWNLOADABLE_EXTENSIONS:
        for ft in allowed_types:
            ft = ft.lower()
            if ft == "pdf" and ext == ".pdf":
                return True
            if ft in ("doc", "docx", "word") and ext in (".doc", ".docx"):
                return True
            if ft == "csv" and ext == ".csv":
                return True
            if ft in ("xlsx", "xls", "excel") and ext in (".xlsx", ".xls"):
                return True
            if ft in ("tiff", "tif") and ext in (".tiff", ".tif"):
                return True
    return False


def get_domain(url):
    return urlparse(url).netloc.lower()


def domain_matches(url, allowed_domains):
    domain = get_domain(url)
    for pattern in allowed_domains:
        if pattern.startswith("*."):
            suffix = pattern[1:]
            if domain.endswith(suffix) or domain == pattern[2:]:
                return True
        else:
            if domain == pattern:
                return True
    return False


def fetch_page(url, use_cffi=False):
    if use_cffi:
        session = cffi_requests.Session(impersonate="chrome")
        return session.get(url, timeout=20, allow_redirects=True)
    return requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)


def fetch_file(url, use_cffi=False):
    if use_cffi:
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


def get_sitemap_urls(sitemap_url, allowed_domains):
    try:
        resp = requests.get(sitemap_url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            urls = re.findall(r'<loc>(https?://[^<]+)</loc>', resp.text)
            filtered = [u for u in urls if domain_matches(u, allowed_domains)]
            return set(filtered)
    except Exception:
        pass
    return set()


class CrawlProgress:
    def __init__(self):
        self.pages_crawled = 0
        self.files_found = 0
        self.files_uploaded = 0
        self.status = "IDLE"
        self.log_lines = []
        self._lock = Lock()

    def log(self, msg):
        with self._lock:
            self.log_lines.append(f"[{time.strftime('%H:%M:%S')}] {msg}")
            print(msg)

    def update(self, **kwargs):
        with self._lock:
            for k, v in kwargs.items():
                setattr(self, k, v)

    def get_log_text(self):
        with self._lock:
            return "\n".join(self.log_lines[-200:])


def _fetch_and_parse(url, depth, use_cffi, allowed_domains, file_types, max_depth):
    try:
        resp = fetch_page(url, use_cffi=use_cffi)
        if resp.status_code != 200:
            return None, [], []
        content_type = resp.headers.get("content-type", "")
        if "text/html" not in content_type:
            return None, [], []
    except Exception:
        return None, [], []

    soup = BeautifulSoup(resp.text, "html.parser")
    title = soup.title.string.strip() if soup.title and soup.title.string else url
    text = extract_text_content(soup)

    page = None
    if text and len(text) > 50:
        page = {
            "url": resp.url if hasattr(resp, "url") else url,
            "title": title[:500],
            "content": text[:60000],
            "domain": get_domain(resp.url if hasattr(resp, "url") else url),
        }

    new_urls = []
    new_files = []
    if depth < max_depth:
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            if not href or href.startswith("#"):
                continue
            full_url = urljoin(url, href).split("#")[0]
            query_stripped = full_url.split("?")[0]
            if should_skip_url(full_url):
                continue
            if is_downloadable(full_url, file_types):
                new_files.append(query_stripped)
                continue
            if domain_matches(full_url, allowed_domains):
                new_urls.append((full_url, query_stripped, depth + 1))

    return page, new_urls, new_files


def crawl_bfs(source_config, progress, use_cffi=False):
    base_url = source_config["source_url"]
    allowed_domains = source_config["allowed_domains"]
    max_pages = source_config.get("max_pages") or 5000
    max_depth = source_config.get("max_depth") or 5
    file_types = source_config.get("file_types") or ["pdf"]
    sitemap_url = source_config.get("sitemap_url")
    crawl_workers = source_config.get("crawl_workers") or 10

    visited = set()
    page_data = []
    file_links = set()
    queue_bfs = deque()
    visited_lock = Lock()
    page_data_lock = Lock()
    file_links_lock = Lock()

    if sitemap_url:
        progress.log(f"  Fetching sitemap: {sitemap_url}")
        sitemap_urls = get_sitemap_urls(sitemap_url, allowed_domains)
        for url in sorted(sitemap_urls):
            if not should_skip_url(url) and not is_downloadable(url, file_types):
                visited.add(url)
                queue_bfs.append((url, 0))
            elif is_downloadable(url, file_types):
                file_links.add(url.split("?")[0])
        progress.log(f"  Sitemap seeded {len(queue_bfs)} pages, {len(file_links)} files")

    visited.add(base_url)
    if not any(u == base_url for u, _ in queue_bfs):
        queue_bfs.appendleft((base_url, 0))

    count = 0
    with ThreadPoolExecutor(max_workers=crawl_workers) as executor:
        futures = {}

        def submit_batch():
            submitted = 0
            while queue_bfs and len(futures) < crawl_workers * 2:
                url, depth = queue_bfs.popleft()
                fut = executor.submit(
                    _fetch_and_parse, url, depth, use_cffi,
                    allowed_domains, file_types, max_depth,
                )
                futures[fut] = (url, depth)
                submitted += 1
            return submitted

        submit_batch()

        while futures:
            done = next(as_completed(futures))
            url, depth = futures.pop(done)
            count += 1

            if count % 100 == 0:
                progress.log(
                    f"  Progress: {count} crawled, {len(queue_bfs)} queued, "
                    f"{len(futures)} in-flight, {len(file_links)} files"
                )
                progress.update(pages_crawled=count, files_found=len(file_links))

            try:
                page, new_urls, new_files = done.result()
            except Exception:
                if count < max_pages:
                    submit_batch()
                continue

            if page:
                page_data.append(page)

            for f in new_files:
                file_links.add(f)

            if count < max_pages:
                for full_url, query_stripped, new_depth in new_urls:
                    if full_url not in visited and query_stripped not in visited:
                        visited.add(full_url)
                        visited.add(query_stripped)
                        queue_bfs.append((full_url, new_depth))

                submit_batch()
            elif not futures:
                break

            if count >= max_pages and not futures:
                progress.log(f"  Reached page limit ({max_pages})")
                break

    progress.update(pages_crawled=count, files_found=len(file_links))
    progress.log(f"  BFS done: {len(page_data)} pages, {len(file_links)} file links")
    return page_data, file_links


def crawl_ckan(source_config, progress):
    ckan_url = source_config["ckan_api_url"]
    file_types = source_config.get("file_types") or ["pdf"]

    datasets = []
    file_urls = []
    start = 0

    resp = requests.get(ckan_url, params={"rows": 0}, headers=HEADERS, timeout=30)
    total = resp.json()["result"]["count"]
    progress.log(f"  CKAN portal: {total} datasets")

    while start < total:
        resp = requests.get(ckan_url, params={"rows": 100, "start": start}, headers=HEADERS, timeout=30)
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
                rurl = r.get("url", "")
                if not rurl:
                    continue
                for ft in file_types:
                    ft_upper = ft.upper()
                    if ft_upper in ("PDF", ".PDF") and fmt in ("PDF", ".PDF"):
                        file_urls.append(rurl)
                    elif ft_upper in ("CSV", ".CSV") and fmt in ("CSV", ".CSV"):
                        file_urls.append(rurl)
                    elif ft_upper in ("XLSX", "XLS", "EXCEL") and fmt in ("XLSX", "XLS"):
                        file_urls.append(rurl)
                    elif ft_upper in ("DOC", "DOCX", "WORD") and fmt in ("DOC", "DOCX"):
                        file_urls.append(rurl)

        start += 100
        if start % 500 == 0:
            progress.log(f"  CKAN: {start}/{total} datasets processed")
            progress.update(pages_crawled=start)

    progress.update(pages_crawled=len(datasets), files_found=len(file_urls))
    progress.log(f"  CKAN done: {len(datasets)} datasets, {len(file_urls)} file resources")
    return datasets, set(file_urls)


def crawl_targeted(source_config, progress):
    direct_urls = source_config.get("direct_urls") or []
    progress.log(f"  Targeted: {len(direct_urls)} direct URLs")
    progress.update(files_found=len(direct_urls))
    return [], set(direct_urls)


def _get_existing_stage_files(stage_at, conn_pool):
    try:
        uc, ucur = conn_pool.get()
        try:
            ucur.execute(f"SELECT RELATIVE_PATH FROM DIRECTORY({stage_at})")
            return {row[0] for row in ucur.fetchall()}
        finally:
            conn_pool.put((uc, ucur))
    except Exception:
        return set()


def download_and_upload_files(file_links, stage_at, conn_pool, progress, use_cffi=False, workers=10):
    existing_files = _get_existing_stage_files(stage_at, conn_pool)
    if existing_files:
        progress.log(f"  Found {len(existing_files)} existing files on stage — will skip duplicates")

    seen_filenames = set(existing_files)
    seen_lock = Lock()
    counter = [0, 0]
    skipped = [0]
    counter_lock = Lock()
    total = len(file_links)

    def process_file(file_url):
        try:
            resp = fetch_file(file_url, use_cffi=use_cffi)
            if resp.status_code != 200:
                return
            if len(resp.content) < 500:
                return

            content_type = resp.headers.get("content-type", "")
            if "html" in content_type and "pdf" not in content_type and "octet" not in content_type:
                soup = BeautifulSoup(resp.text, "html.parser")
                dl_link = soup.find("a", href=re.compile(r"\.(pdf|PDF|docx?|xlsx?|csv|tiff?)", re.I))
                if dl_link:
                    actual_url = urljoin(resp.url if hasattr(resp, "url") else file_url, dl_link["href"])
                    resp = fetch_file(actual_url, use_cffi=use_cffi)
                    if resp.status_code != 200:
                        return
                else:
                    return

            final_url = resp.url if hasattr(resp, "url") else file_url
            filename = urlparse(final_url).path.split("/")[-1]
            if not filename or "." not in filename:
                ext = ".pdf"
                if "word" in content_type or "docx" in content_type:
                    ext = ".docx"
                elif "csv" in content_type:
                    ext = ".csv"
                elif "excel" in content_type or "spreadsheet" in content_type:
                    ext = ".xlsx"
                elif "tiff" in content_type:
                    ext = ".tiff"
                filename = hashlib.md5(file_url.encode()).hexdigest() + ext
            filename = re.sub(r'[^\w\-.]', '_', filename)

            with seen_lock:
                if filename in seen_filenames:
                    if filename in existing_files:
                        with counter_lock:
                            skipped[0] += 1
                        return
                    base, ext = os.path.splitext(filename)
                    c = 1
                    while f"{base}_{c}{ext}" in seen_filenames:
                        c += 1
                    filename = f"{base}_{c}{ext}"
                seen_filenames.add(filename)

            suffix = os.path.splitext(filename)[1] or ".pdf"
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp.write(resp.content)
                tmp_path = tmp.name

            try:
                uc, ucur = conn_pool.get()
                try:
                    ucur.execute(
                        f"PUT 'file://{tmp_path}' {stage_at}/{filename} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                    )
                finally:
                    conn_pool.put((uc, ucur))

                with counter_lock:
                    counter[0] += 1
                    counter[1] += len(resp.content)
                    c = counter[0]
                if c % 25 == 0 or c == total:
                    progress.log(f"  Uploaded {c}/{total} files ({counter[1]/1024/1024:.1f} MB)")
                    progress.update(files_uploaded=c)
            finally:
                os.unlink(tmp_path)

        except Exception:
            pass

    progress.log(f"  Streaming {total} files to Snowflake...")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_file, url) for url in sorted(file_links)]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                pass

    progress.update(files_uploaded=counter[0])
    progress.log(f"  Upload done: {counter[0]} new files ({counter[1]/1024/1024:.1f} MB), {skipped[0]} skipped (already on stage)")
    return counter[0]


def create_connection_pool(get_conn_fn, init_fn, n):
    pool = queue.Queue()
    for _ in range(n):
        conn = get_conn_fn()
        cur = conn.cursor()
        init_fn(cur)
        pool.put((conn, cur))
    return pool


def close_pool(pool):
    while not pool.empty():
        conn, cur = pool.get()
        cur.close()
        conn.close()


def upload_web_pages(page_data, web_table, get_conn_fn, init_fn, close_after=True):
    conn = get_conn_fn()
    cur = conn.cursor()
    init_fn(cur)

    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {web_table} (
                url VARCHAR, title VARCHAR, content VARCHAR, domain VARCHAR,
                scraped_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        cur.execute(f"TRUNCATE TABLE {web_table}")

        if page_data:
            batch_size = 500
            for i in range(0, len(page_data), batch_size):
                batch = page_data[i:i + batch_size]
                cur.execute("BEGIN")
                for p in batch:
                    cur.execute(
                        f"INSERT INTO {web_table} (url, title, content, domain) "
                        f"VALUES (:1, :2, :3, :4)",
                        (p["url"], p["title"], p["content"], p["domain"]),
                    )
                cur.execute("COMMIT")
        return len(page_data)
    finally:
        cur.close()
        if close_after:
            conn.close()


def setup_stage(stage_fqn, get_conn_fn, init_fn, close_after=True):
    conn = get_conn_fn()
    cur = conn.cursor()
    init_fn(cur)
    try:
        cur.execute(
            f"CREATE STAGE IF NOT EXISTS {stage_fqn} "
            f"DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
        )
    finally:
        cur.close()
        if close_after:
            conn.close()


def refresh_stage_and_metadata(stage_fqn, stage_at, parsed_table, get_conn_fn, init_fn, close_after=True):
    conn = get_conn_fn()
    cur = conn.cursor()
    init_fn(cur)
    try:
        cur.execute(f"ALTER STAGE {stage_fqn} REFRESH")
    finally:
        cur.close()
        if close_after:
            conn.close()


def run_extraction(source_config, get_conn_fn, init_fn, database, schema, progress=None, single_connection=False):
    if progress is None:
        progress = CrawlProgress()

    source_name = source_config["source_name"]
    crawl_method = source_config["crawl_method"]
    use_cffi = source_config.get("cloudflare_bypass", False)
    stage_name = source_config["stage_name"]
    web_table = f"{database}.{schema}.{source_config['web_table_name']}"
    stage_fqn = f"{database}.{schema}.{stage_name}"
    stage_at = f"@{stage_fqn}"
    dl_workers = source_config.get("download_workers") or 10
    close_after = not single_connection

    progress.update(status="RUNNING")
    progress.log(f"Starting extraction: {source_name} (method={crawl_method})")

    page_data = []
    file_links = set()

    if crawl_method == "BFS":
        page_data, file_links = crawl_bfs(source_config, progress, use_cffi=use_cffi)
    elif crawl_method == "CKAN":
        page_data, file_links = crawl_ckan(source_config, progress)
    elif crawl_method == "TARGETED":
        page_data, file_links = crawl_targeted(source_config, progress)
    else:
        progress.log(f"  Unknown crawl method: {crawl_method}")
        progress.update(status="FAILED")
        return

    if page_data:
        progress.log(f"  Uploading {len(page_data)} web pages to {web_table}...")
        upload_web_pages(page_data, web_table, get_conn_fn, init_fn, close_after=close_after)

    if file_links:
        progress.log(f"  Setting up stage {stage_fqn}...")
        setup_stage(stage_fqn, get_conn_fn, init_fn, close_after=close_after)

        progress.log(f"  Downloading + uploading {len(file_links)} files...")
        if single_connection:
            shared_conn = get_conn_fn()
            _pool_lock = Lock()

            class SharedPool:
                def get(self):
                    _pool_lock.acquire()
                    return shared_conn, shared_conn.cursor()
                def put(self, item):
                    _, cur = item
                    cur.close()
                    _pool_lock.release()

            pool = SharedPool()
            download_and_upload_files(
                file_links, stage_at, pool, progress,
                use_cffi=use_cffi, workers=dl_workers,
            )
        else:
            pool = create_connection_pool(get_conn_fn, init_fn, min(dl_workers, 8))
            try:
                download_and_upload_files(
                    file_links, stage_at, pool, progress,
                    use_cffi=use_cffi, workers=dl_workers,
                )
            finally:
                close_pool(pool)

        progress.log(f"  Refreshing stage directory...")
        refresh_stage_and_metadata(stage_fqn, stage_at, None, get_conn_fn, init_fn, close_after=close_after)

    progress.update(status="COMPLETED")
    progress.log(f"Extraction complete: {source_name}")
    return {
        "pages": len(page_data),
        "files": len(file_links),
        "uploaded": progress.files_uploaded,
    }
