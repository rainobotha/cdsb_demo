# CDSB Queensland Government Data Extraction Platform

A production-ready, table-driven data extraction platform built on Snowflake. It crawls Queensland Government websites, downloads documents (PDF, Word, CSV, Excel, TIFF), parses them using Cortex AI, chunks the content, and exposes it via Cortex Search and a Cortex Agent — all managed through a Streamlit in Snowflake control plane.

---

## Table of Contents

1. [What This Does](#what-this-does)
2. [Architecture](#architecture)
3. [Snowflake Objects Reference](#snowflake-objects-reference)
4. [Project File Structure](#project-file-structure)
5. [How It Works End-to-End](#how-it-works-end-to-end)
6. [Setup Guide](#setup-guide)
7. [Streamlit Platform (Control Plane)](#streamlit-platform-control-plane)
8. [Adding a New Source](#adding-a-new-source)
9. [Running Extraction](#running-extraction)
10. [Incremental Pipeline](#incremental-pipeline)
11. [SPCS Scheduled Scraper](#spcs-scheduled-scraper)
12. [Cortex Search Service](#cortex-search-service)
13. [Cortex Agent](#cortex-agent)
14. [Configuration Reference](#configuration-reference)
15. [Key Technical Patterns](#key-technical-patterns)
16. [Troubleshooting](#troubleshooting)

---

## What This Does

The platform:

1. **Crawls** government websites using configurable BFS (breadth-first search), CKAN API, or targeted URL methods
2. **Downloads** documents (PDF, Word, CSV, Excel, TIFF) and **streams them directly to Snowflake stages** — nothing stored on local disk
3. **Parses** documents using `SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT` to extract text content
4. **Unifies** all web page text + parsed document text into a single table
5. **Chunks** content with 1500-character windows and 500-character overlap for optimal retrieval
6. **Indexes** chunks via Cortex Search Service (`snowflake-arctic-embed-l-v2.0` embeddings)
7. **Serves** an AI assistant (Cortex Agent) that can search the knowledge base and answer questions

All extraction targets are **table-driven** — you add, modify, or disable sources through the `EXTRACTION_SOURCES` config table (or the Streamlit UI). The pipeline is **incremental** — only new or changed documents are re-chunked, so Cortex Search only re-indexes the delta.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                    STREAMLIT IN SNOWFLAKE (Control Plane)             │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  │
│  │ Manage   │ │   Run    │ │ Monitor  │ │ Search & │ │ Pipeline │  │
│  │ Sources  │ │Extraction│ │          │ │   Chat   │ │          │  │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘  │
└───────────────────────────┬──────────────────────────────────────────┘
                            │ reads/writes
                            ▼
┌───────────────────────────────────────────────────────────────────────┐
│                     EXTRACTION_SOURCES (Config Table)                  │
│  source_name | source_url | crawl_method | file_types | max_pages ... │
│  ─────────── | ────────── | ──────────── | ────────── | ───────── ... │
│  CDSB        | https://.. | BFS          | [pdf]      | 5000      ... │
│  TMR         | https://.. | BFS          | [pdf]      | 5000      ... │
│  Publications| https://.. | CKAN         | [pdf]      | NULL      ... │
│  ...9 sources configured...                                           │
└───────────────────────────┬───────────────────────────────────────────┘
                            │ drives
                            ▼
┌───────────────────────────────────────────────────────────────────────┐
│                      UNIFIED SCRAPER ENGINE                           │
│                                                                       │
│   ┌─────────┐     ┌──────────┐     ┌──────────┐                     │
│   │   BFS   │     │   CKAN   │     │ TARGETED │                     │
│   │ Crawler │     │   API    │     │   URLs   │                     │
│   └────┬────┘     └────┬─────┘     └────┬─────┘                     │
│        │               │                │                             │
│        ▼               ▼                ▼                             │
│   ┌─────────────────────────────────────────┐                        │
│   │ Download + Stream-to-Snowflake          │                        │
│   │ (tempfile → PUT → unlink, no local disk)│                        │
│   │ Supports: PDF, Word, CSV, Excel, TIFF   │                        │
│   └─────────────────────────────────────────┘                        │
│        │                    │                                         │
│        ▼                    ▼                                         │
│   Web Pages Tables    Snowflake Stages                               │
│   (HTML text)         (raw document files)                           │
└───────────────────────────┬───────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────────────────┐
│                    INCREMENTAL PIPELINE                                │
│                                                                       │
│   Stage Files ──► AI_PARSE_DOCUMENT ──► Parsed Tables                │
│                   (PDF, Word, TIFF)      (extracted text)             │
│                                                                       │
│   Web Pages Tables ─┐                                                │
│                      ├──► MERGE into CDSB_DOCUMENTS_UNIFIED          │
│   Parsed Tables ─────┘    (MD5 hash comparison, upsert only changes) │
│                                                                       │
│   Changed docs only ──► DELETE old chunks + INSERT new chunks        │
│                          into CDSB_CHUNKS                             │
│                          (1500 char window, 500 char overlap)        │
└───────────────────────────┬───────────────────────────────────────────┘
                            │ auto-refresh (incremental)
                            ▼
┌───────────────────────────────────────────────────────────────────────┐
│              CORTEX SEARCH SERVICE: QLD_GOVERNMENT                    │
│              Model: snowflake-arctic-embed-l-v2.0                    │
│              Refresh mode: INCREMENTAL                               │
│              833,016 chunks indexed                                   │
│                                                                       │
│                            │                                          │
│                            ▼                                          │
│              CORTEX AGENT: CDSB_ASSISTANT                            │
│              Model: claude-sonnet-4-6                                  │
│              Tools: Cortex Search + Web Search                       │
└───────────────────────────────────────────────────────────────────────┘
```

---

## Snowflake Objects Reference

All objects live in `CDSB_DEMO.RAW`.

### Core Tables

| Object | Type | Purpose |
|--------|------|---------|
| `EXTRACTION_SOURCES` | Table | Config table — each row defines one extraction source (URL, method, file types, depth, etc.) |
| `EXTRACTION_RUNS` | Table | Run history — logs every extraction job with status, counts, timestamps |
| `CDSB_DOCUMENTS_UNIFIED` | Table | All content merged — web page text + parsed document text from all sources |
| `CDSB_CHUNKS` | Table | Chunked content — 1500-char windows with 500-char overlap, feeds Cortex Search |

### Per-Source Tables (9 sources)

| Source | Web Pages Table | Parsed Table | Stage |
|--------|----------------|--------------|-------|
| CDSB | `WEB_PAGES` | `PDF_PARSED` | `CDSB_DOCUMENTS` |
| QLD Gov | `QLD_GOV_WEB_PAGES` | `QLD_GOV_PDF_PARSED` | `QLD_GOV_DOCUMENTS` |
| TMR | `TMR_WEB_PAGES` | `TMR_PDF_PARSED` | `TMR_DOCUMENTS` |
| Education | `EDU_WEB_PAGES` | `EDU_PDF_PARSED` | `EDU_DOCUMENTS` |
| Health | `HEALTH_WEB_PAGES` | `HEALTH_PDF_PARSED` | `HEALTH_DOCUMENTS` |
| DPI | `DPI_WEB_PAGES` | `DPI_PDF_PARSED` | `DPI_DOCUMENTS` |
| Police | `POLICE_WEB_PAGES` | `POLICE_PDF_PARSED` | `POLICE_DOCUMENTS` |
| Publications | `PUBLICATIONS_WEB_PAGES` | `PUBLICATIONS_PDF_PARSED` | `PUBLICATIONS_DOCUMENTS` |
| QPS Reports | *(shares POLICE_WEB_PAGES)* | `QPS_REPORTS_PARSED` | `QPS_REPORTS` |

### AI / Search Objects

| Object | Type | Details |
|--------|------|---------|
| `QLD_GOVERNMENT` | Cortex Search Service | Embedding: `snowflake-arctic-embed-l-v2.0`, refresh: INCREMENTAL, source: `CDSB_CHUNKS` |
| `CDSB_ASSISTANT` | Cortex Agent | Model: `claude-sonnet-4-6`, tools: Cortex Search + Web Search |

### Infrastructure

| Object | Type | Details |
|--------|------|---------|
| `CDSB_EGRESS_RULE` | Network Rule | Allows outbound HTTP to government websites |
| `ALLOW_ALL_ACCESS_INTEGRATION` | External Access Integration | Enables network access for SPCS containers and Streamlit |
| `CDSB_IMAGES` | Image Repository | Stores Docker images for SPCS scraper container |
| `CPU_X64_XS` | Compute Pool | Runs the Streamlit app and SPCS scraper jobs |
| `CDSB_HOURLY_SCRAPER` | Task | Scheduled hourly SPCS job (currently suspended) |
| `CDSB_PLATFORM` | Streamlit App | The Streamlit in Snowflake control plane |

---

## Project File Structure

```
cdsb_demo/
│
├── config.py                      # Shared configuration for standalone scrapers
│                                    (env var parameterised: connection, database, schema, warehouse)
│
├── cdsb_platform/                 # PRODUCTION PLATFORM (Streamlit + Engine)
│   ├── snowflake.yml              # SiS deployment manifest (container runtime, compute pool)
│   ├── pyproject.toml             # Python dependencies
│   ├── streamlit_app.py           # Main entry point — 5-page sidebar navigation
│   ├── engine.py                  # Unified scraper engine (BFS, CKAN, TARGETED methods)
│   ├── pipeline.py                # Incremental pipeline (parse, MERGE, chunk)
│   ├── run_extraction.py          # CLI runner for local/manual extraction
│   └── app_pages/
│       ├── sources.py             # Page: Add/edit/delete extraction sources
│       ├── extract.py             # Page: Run extraction, full pipeline button
│       ├── monitor.py             # Page: Overview metrics, run history, data inventory
│       ├── search.py              # Page: Cortex Search + Chat with LLM
│       └── pipeline.py            # Page: Individual pipeline stages (parse, unify, chunk)
│
├── cdsb_spcs/                     # SPCS CONTAINER (scheduled hourly scraper)
│   ├── scraper.py                 # All-in-one: crawl 8 sites + CKAN, sync, rebuild pipeline
│   └── Dockerfile                 # Container image definition
│
├── police_scraper.py              # Standalone: Police website BFS crawler + PDF streamer
├── police_parse.py                # Standalone: AI_PARSE_DOCUMENT for police PDFs
├── tmr_scraper.py                 # Standalone: TMR crawler (curl_cffi for Cloudflare)
├── qld_gov_scraper.py             # Standalone: QLD Gov broad BFS crawler
├── qld_multi_scraper.py           # Standalone: EDU + HEALTH + DPI multi-site crawler
├── publications_scraper.py        # Standalone: CKAN API scraper for publications portal
├── publications_parse.py          # Standalone: AI_PARSE_DOCUMENT for publications PDFs
├── qps_reports_ingest.py          # Standalone: QPS annual reports targeted ingest
├── test_cdsb_search.py            # Test: Query Cortex Search Service
└── README.md                      # This file
```

---

## How It Works End-to-End

### Step 1: Define Sources (EXTRACTION_SOURCES table)

Each source is a row in the `EXTRACTION_SOURCES` table:

```sql
SELECT source_name, source_url, crawl_method, file_types, max_pages, max_depth,
       cloudflare_bypass, stage_name, web_table_name, parsed_table_name
FROM CDSB_DEMO.RAW.EXTRACTION_SOURCES;
```

The table currently has 9 sources configured:

| Source | Method | Cloudflare | File Types | Max Pages |
|--------|--------|------------|------------|-----------|
| CDSB | BFS | No | PDF | 5,000 |
| QLD Gov | BFS | No | PDF | 3,000 |
| TMR | BFS | Yes | PDF | 5,000 |
| Education | BFS | Yes | PDF | 5,000 |
| Health | BFS | Yes | PDF | 5,000 |
| DPI | BFS | Yes | PDF | 5,000 |
| Police | BFS | No | PDF | 5,000 |
| Publications | CKAN | No | PDF | — |
| QPS Reports | TARGETED | No | PDF | — |

### Step 2: Crawl + Download (Engine)

The engine reads the config and dispatches the appropriate crawler:

- **BFS**: Breadth-first crawl starting from `source_url`. Respects `max_pages`, `max_depth`, `allowed_domains`. Extracts document links matching `file_types`. Can seed from `sitemap_url` if provided.
- **CKAN**: Calls the CKAN API (`package_search` endpoint) to enumerate all datasets and their resource files. Used for `publications.qld.gov.au`.
- **TARGETED**: Downloads a specific list of URLs from `direct_urls` array column. Used for QPS Reports.

For Cloudflare-protected sites (TMR, Health, DPI, Education), the engine uses `curl_cffi` with browser impersonation instead of `requests`.

**Web page content** (HTML text) is inserted into the source's web pages table.

**Document files** are downloaded and streamed directly to the source's Snowflake stage using the stream-to-Snowflake pattern (no local disk storage):

```
HTTP response → tempfile.NamedTemporaryFile → PUT to @STAGE → os.unlink(tempfile)
```

### Step 3: Parse Documents (AI_PARSE_DOCUMENT)

Documents in stages are parsed using Snowflake's built-in AI document parser:

```sql
SELECT
    d.RELATIVE_PATH as filename,
    SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(
        TO_FILE('@CDSB_DEMO.RAW.POLICE_DOCUMENTS', d.RELATIVE_PATH),
        {'mode': 'LAYOUT'}
    ):content::VARCHAR as parsed_content
FROM DIRECTORY(@CDSB_DEMO.RAW.POLICE_DOCUMENTS) d
WHERE LOWER(d.RELATIVE_PATH) LIKE '%.pdf'
   OR LOWER(d.RELATIVE_PATH) LIKE '%.docx'
   OR LOWER(d.RELATIVE_PATH) LIKE '%.doc'
   OR LOWER(d.RELATIVE_PATH) LIKE '%.tiff'
   OR LOWER(d.RELATIVE_PATH) LIKE '%.tif'
```

**Supported file types for AI_PARSE_DOCUMENT**: PDF, Word (.doc, .docx), TIFF images.

CSV and Excel files are catalogued with metadata (filename + size) since they are structured data that doesn't need text extraction.

### Step 4: Incremental Merge into Unified Documents

All web page text and parsed document text is merged into `CDSB_DOCUMENTS_UNIFIED` using a MERGE statement:

1. A staging table `_UNIFIED_STAGING` is built from all source tables
2. Each row gets an `MD5(content)` hash
3. `MERGE INTO CDSB_DOCUMENTS_UNIFIED` compares on `(source_url, source_type)`:
   - **New rows** → INSERT
   - **Changed content** (hash differs) → UPDATE, set `updated_at = CURRENT_TIMESTAMP()`
   - **Unchanged rows** → skip (no cost)
4. Rows that no longer exist in sources → DELETE

### Step 5: Incremental Chunking

Only documents that were inserted or updated (i.e., `updated_at` within the last 5 minutes) get re-chunked:

1. Identify changed documents via `_changed_sources` temp table
2. DELETE existing chunks for those documents from `CDSB_CHUNKS`
3. INSERT new chunks using the recursive CTE:
   - **Window size**: 1,500 characters
   - **Overlap**: 500 characters (step = 1,000)
   - Minimum chunk length: 50 characters

This means if you have 833,016 chunks and only 50 documents changed, only those ~500 chunks are rewritten — Cortex Search only re-indexes the delta.

### Step 6: Cortex Search Auto-Refresh

The Cortex Search Service `QLD_GOVERNMENT` is defined over `CDSB_CHUNKS`:

```sql
-- DO NOT RECREATE THIS — it auto-refreshes
-- Definition for reference only:
CREATE CORTEX SEARCH SERVICE QLD_GOVERNMENT
    ON chunk_content
    ATTRIBUTES title, source_type, source_url, domain
    WAREHOUSE = TRANSPORT_NSW_WH
    TARGET_LAG = '1 day'
    AS (
        SELECT CHUNK_CONTENT, TITLE, SOURCE_TYPE, SOURCE_URL, DOMAIN
        FROM CDSB_DEMO.RAW.CDSB_CHUNKS
    );
```

It uses **INCREMENTAL** refresh mode — it detects changes in the underlying `CDSB_CHUNKS` table and only re-indexes what changed. **Do not drop or recreate this service.**

### Step 7: Cortex Agent

The `CDSB_ASSISTANT` agent uses the search service as a tool:

- **Model**: `claude-sonnet-4-6`
- **Tools**: Cortex Search (`QLD_GOVERNMENT`) + Web Search
- **Instructions**: Covers 6 QLD government departments, cites sources, identifies departments

---

## Setup Guide

### Prerequisites

1. **Snowflake account** with:
   - ACCOUNTADMIN role (or equivalent)
   - A warehouse (default: `COMPUTE_WH`)
   - A compute pool for SiS container runtime (default: `CPU_X64_XS`)
   - External access integration for outbound HTTP (default: `ALLOW_ALL_ACCESS_INTEGRATION`)

2. **Snowflake CLI** v3.14.0+ (for `definition_version: 2` deployment):
   ```bash
   snow --version    # Must be >= 3.14.0
   ```

3. **Python 3.11+** with packages (for local extraction):
   ```bash
   pip install snowflake-connector-python requests beautifulsoup4 curl_cffi
   ```

### Step 1: Create Database and Schema

```sql
CREATE DATABASE IF NOT EXISTS CDSB_DEMO;
CREATE SCHEMA IF NOT EXISTS CDSB_DEMO.RAW;
USE DATABASE CDSB_DEMO;
USE SCHEMA RAW;
```

### Step 2: Create Network Access (if not already exists)

```sql
-- Network rule for outbound HTTP to government sites
CREATE NETWORK RULE IF NOT EXISTS CDSB_EGRESS_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'www.cdsb.qld.gov.au', 'www.qld.gov.au', 'www.tmr.qld.gov.au',
        'education.qld.gov.au', 'www.health.qld.gov.au', 'www.dpi.qld.gov.au',
        'www.police.qld.gov.au', 'www.publications.qld.gov.au'
    );

-- Or use an existing allow-all integration
-- SHOW EXTERNAL ACCESS INTEGRATIONS;
```

### Step 3: Download TMR Open Data

The demo notebooks require the TMR vehicle registration dataset (~42 MB CSV from QLD Open Data). This is too large for version control, so a setup script downloads it from the CKAN API and uploads to a Snowflake stage:

```bash
SNOWFLAKE_CONNECTION_NAME=AU_DEMO50 python setup_data.py
```

This downloads the latest CSV from `data.qld.gov.au`, converts from UTF-16 to UTF-8, compresses, and uploads to `@QLD_OPEN_DATA/tmr/`. The notebook's `COPY INTO` statement loads from this stage.

### Step 4: Create Config Tables

```sql
-- These are created automatically by the platform, but here for reference:
CREATE TABLE IF NOT EXISTS EXTRACTION_SOURCES (
    source_id NUMBER AUTOINCREMENT PRIMARY KEY,
    source_name VARCHAR NOT NULL,
    source_url VARCHAR NOT NULL,
    crawl_method VARCHAR NOT NULL DEFAULT 'BFS',    -- BFS, CKAN, or TARGETED
    allowed_domains ARRAY,                           -- e.g. ['www.example.qld.gov.au']
    cloudflare_bypass BOOLEAN DEFAULT FALSE,         -- use curl_cffi for Cloudflare sites
    file_types ARRAY DEFAULT ARRAY_CONSTRUCT('pdf'), -- pdf, docx, csv, xlsx, tiff
    max_pages NUMBER DEFAULT 5000,
    max_depth NUMBER DEFAULT 5,
    crawl_workers NUMBER DEFAULT 10,
    download_workers NUMBER DEFAULT 10,
    stage_name VARCHAR NOT NULL,                     -- Snowflake stage name
    web_table_name VARCHAR NOT NULL,                 -- table for HTML page content
    parsed_table_name VARCHAR NOT NULL,              -- table for parsed document text
    sitemap_url VARCHAR,                             -- optional: seed BFS from sitemap
    ckan_api_url VARCHAR,                            -- required for CKAN method
    direct_urls ARRAY,                               -- required for TARGETED method
    enabled BOOLEAN DEFAULT TRUE,
    last_run_at TIMESTAMP_NTZ,
    last_run_status VARCHAR,
    last_run_pages NUMBER,
    last_run_files NUMBER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS EXTRACTION_RUNS (
    run_id NUMBER AUTOINCREMENT PRIMARY KEY,
    source_id NUMBER,
    source_name VARCHAR,
    started_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    completed_at TIMESTAMP_NTZ,
    status VARCHAR DEFAULT 'RUNNING',
    pages_crawled NUMBER DEFAULT 0,
    files_downloaded NUMBER DEFAULT 0,
    files_parsed NUMBER DEFAULT 0,
    error_message VARCHAR,
    log_text VARCHAR
);
```

### Step 4: Deploy the Streamlit Platform

```bash
cd cdsb_demo/cdsb_platform
snow streamlit deploy --replace --connection <YOUR_CONNECTION>
```

The app will be available at:
`https://app.snowflake.com/<ORG>/<account>/#/streamlit-apps/<DATABASE>.<SCHEMA>.CDSB_PLATFORM`

Or find it in **Snowsight → Projects → Streamlit**.

### Deploying to a Different Account

The solution is fully configurable. To deploy into your own Snowflake account:

1. **Change the database/schema** — Edit `snowflake.yml` to set your database and schema, then update `CDSB_DATABASE`/`CDSB_SCHEMA` environment variables for CLI scripts.

2. **Streamlit app** — Automatically detects the current database/schema from the Snowflake session at runtime. No code changes needed.

3. **Dashboard** — Copy `cdsb-dashboard/.env.example` to `.env.local` and set your account, user, database, etc.

4. **Notebooks** — The build script (`session_demos/build_sf_notebooks.py`) reads `CDSB_DATABASE`, `CDSB_SCHEMA`, `CDSB_WAREHOUSE` env vars. Run it to regenerate notebooks for your account:
   ```bash
   CDSB_DATABASE=MY_DB CDSB_SCHEMA=MY_SCHEMA python build_sf_notebooks.py
   ```

5. **SPCS scraper** — Reads `CDSB_DATABASE` and `CDSB_SCHEMA` from environment variables.

6. **CLI commands** — Replace `--connection AU_DEMO50` with your own connection name throughout.

---

## Streamlit Platform (Control Plane)

The Streamlit app has 5 pages:

### Manage Sources
- View all configured extraction sources with status
- **Add** new sources: name, URL, crawl method, domains, file types, depth, stage names
- **Edit** existing sources: enable/disable, change limits
- **Delete** sources

### Run Extraction
- Select which sources to extract (multi-select)
- **Start Extraction**: queues jobs (run via CLI or SPCS)
- **Run Full Pipeline Only**: Parse → MERGE → Chunk (incremental) without re-crawling
- **Refresh All Stages**: refreshes DIRECTORY() metadata for all stages
- Dry-run mode to inspect config without running

### Monitor
- **Overview**: source count, unified doc count, chunk count, search service status
- **Run History**: last 50 extraction runs with status, timing, counts
- **Data Inventory**: per-source counts (web pages, parsed docs, stage files)
- **Chunks by Domain**: bar chart of chunk distribution

### Search & Chat
- **Search tab**: Query Cortex Search directly, view results with titles, domains, content previews
- **Chat tab**: RAG-powered chat — searches context via Cortex Search, generates answers with Claude

### Pipeline
- **Parse Documents**: run AI_PARSE_DOCUMENT per source
- **Unify & Chunk**: incremental MERGE + chunk only changed docs
- Shows status and counts at each step

---

## Adding a New Source

### Via Streamlit UI

1. Go to **Manage Sources → Add Source** tab
2. Fill in:
   - **Source Name**: descriptive name (e.g. "Transport NSW")
   - **Source URL**: starting crawl URL
   - **Crawl Method**: BFS (web crawl), CKAN (API), or TARGETED (direct URLs)
   - **Cloudflare Bypass**: enable for sites behind Cloudflare
   - **Allowed Domains**: comma-separated list (e.g. `www.transport.nsw.gov.au, transport.nsw.gov.au`)
   - **File Types**: select from PDF, DOCX, DOC, CSV, XLSX, XLS, TIFF
   - **Max Pages / Depth**: crawl limits
   - **Stage Name**: Snowflake stage for documents (e.g. `TRANSPORT_NSW_DOCUMENTS`)
   - **Web Pages Table**: for HTML content (e.g. `TRANSPORT_NSW_WEB_PAGES`)
   - **Parsed Table**: for parsed document text (e.g. `TRANSPORT_NSW_PDF_PARSED`)
3. Click **Add Source**

### Via SQL

```sql
INSERT INTO CDSB_DEMO.RAW.EXTRACTION_SOURCES
    (source_name, source_url, crawl_method, allowed_domains, cloudflare_bypass,
     file_types, max_pages, max_depth, stage_name, web_table_name, parsed_table_name)
SELECT
    'Transport NSW',
    'https://www.transport.nsw.gov.au',
    'BFS',
    ARRAY_CONSTRUCT('www.transport.nsw.gov.au', 'transport.nsw.gov.au'),
    FALSE,
    ARRAY_CONSTRUCT('pdf', 'docx'),
    3000,
    5,
    'TRANSPORT_NSW_DOCUMENTS',
    'TRANSPORT_NSW_WEB_PAGES',
    'TRANSPORT_NSW_PDF_PARSED';
```

The stage and tables will be created automatically when extraction runs.

---

## Running Extraction

### Option 1: Via Streamlit UI

Go to **Run Extraction** → select sources → click **Start Extraction**. Then use **Run Full Pipeline Only** to parse and chunk.

### Option 2: Via CLI (local machine)

```bash
cd cdsb_demo/cdsb_platform

# Run all enabled sources
SNOWFLAKE_CONNECTION_NAME=AU_DEMO50 python run_extraction.py

# Run specific sources only
SNOWFLAKE_CONNECTION_NAME=AU_DEMO50 python run_extraction.py --sources=Police,TMR

# Run extraction without pipeline rebuild
SNOWFLAKE_CONNECTION_NAME=AU_DEMO50 python run_extraction.py --no-pipeline
```

### Option 3: Via SPCS (scheduled)

The hourly task runs the containerised scraper:

```sql
-- Resume the task (currently suspended)
ALTER TASK CDSB_DEMO.RAW.CDSB_HOURLY_SCRAPER RESUME;

-- Or run manually
EXECUTE TASK CDSB_DEMO.RAW.CDSB_HOURLY_SCRAPER;
```

---

## Incremental Pipeline

The pipeline is designed to be cost-efficient. Here's exactly what happens:

### Parse Stage (per source)
```sql
-- This is CREATE OR REPLACE because parsing the whole stage is idempotent
-- and AI_PARSE_DOCUMENT is the expensive step — we want fresh results
CREATE OR REPLACE TABLE {parsed_table} AS
SELECT
    d.RELATIVE_PATH as filename,
    SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(
        TO_FILE('@{stage}', d.RELATIVE_PATH),
        {'mode': 'LAYOUT'}
    ):content::VARCHAR as parsed_content,
    d.SIZE as file_size,
    CURRENT_TIMESTAMP() as parsed_at
FROM DIRECTORY(@{stage}) d
WHERE LOWER(d.RELATIVE_PATH) LIKE '%.pdf'
   OR LOWER(d.RELATIVE_PATH) LIKE '%.docx'
   OR LOWER(d.RELATIVE_PATH) LIKE '%.tiff';
```

### Unify Stage (MERGE — incremental)
```sql
-- 1. Build staging table with MD5 content hashes
CREATE OR REPLACE TABLE _UNIFIED_STAGING AS
SELECT 'web', url, title, content, domain, MD5(content)
FROM {web_table}
UNION ALL
SELECT 'document', filename, ..., parsed_content, ..., MD5(parsed_content)
FROM {parsed_table};

-- 2. MERGE: only insert new rows, update changed rows
MERGE INTO CDSB_DOCUMENTS_UNIFIED t
USING _UNIFIED_STAGING s
ON t.source_url = s.source_url AND t.source_type = s.source_type
WHEN MATCHED AND t.content_hash != s.content_hash THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;

-- 3. Remove deleted sources
DELETE FROM CDSB_DOCUMENTS_UNIFIED t
WHERE NOT EXISTS (SELECT 1 FROM _UNIFIED_STAGING s WHERE ...);
```

### Chunk Stage (DELETE + INSERT — incremental)
```sql
-- 1. Find what changed in the last 5 minutes
CREATE TEMPORARY TABLE _changed_sources AS
SELECT source_url, source_type FROM CDSB_DOCUMENTS_UNIFIED
WHERE updated_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP());

-- 2. Delete old chunks for changed documents only
DELETE FROM CDSB_CHUNKS
WHERE EXISTS (SELECT 1 FROM _changed_sources c WHERE c.source_url = CDSB_CHUNKS.source_url ...);

-- 3. Insert new chunks for changed documents only
INSERT INTO CDSB_CHUNKS
WITH RECURSIVE src AS (
    SELECT ... FROM CDSB_DOCUMENTS_UNIFIED
    WHERE EXISTS (SELECT 1 FROM _changed_sources ...)
),
chunks AS (
    -- 1500-char windows, 500-char overlap (step 1000)
    SELECT ..., SUBSTR(content, 1, 1500) ...
    UNION ALL
    SELECT ..., SUBSTR(content, 1 + (chunk_num * 1000), 1500) ...
    WHERE 1 + (chunk_num * 1000) <= total_len
)
SELECT ... FROM chunks WHERE LEN(chunk_content) > 50;
```

**Why this matters**: If 833,016 chunks exist and only 50 documents changed, approximately 500 chunks are rewritten. Cortex Search (INCREMENTAL mode) only re-embeds and re-indexes those 500 chunks instead of all 833,016.

---

## SPCS Scheduled Scraper

The `cdsb_spcs/` directory contains a standalone containerised version that:

1. Crawls all 8 web sources + the Publications CKAN API
2. Compares content hashes for incremental sync (only uploads new/changed content)
3. Rebuilds the full pipeline (parse → unify → chunk) if any changes detected
4. Runs hourly via a Snowflake Task

```
cdsb_spcs/
├── scraper.py    # All-in-one scraper (hardcoded 8 sites + CKAN)
└── Dockerfile    # python:3.11-slim + dependencies
```

The SPCS scraper uses the older full-rebuild approach (`CREATE OR REPLACE TABLE` for chunks). For production use, prefer the `cdsb_platform` incremental pipeline.

---

## Cortex Search Service

**Name**: `CDSB_DEMO.RAW.QLD_GOVERNMENT`
**Status**: ACTIVE
**Embedding model**: `snowflake-arctic-embed-l-v2.0`
**Refresh mode**: INCREMENTAL (auto-detects changes in CDSB_CHUNKS)
**Target lag**: 1 day
**Current rows**: 833,016

**IMPORTANT: Do not drop or recreate this service.** It will automatically pick up changes from the `CDSB_CHUNKS` table.

### Querying the Search Service

```sql
SELECT SNOWFLAKE.CORTEX.SEARCH(
    'CDSB_DEMO.RAW.QLD_GOVERNMENT',
    'road safety regulations',
    {
        'columns': ['chunk_content', 'title', 'source_url', 'domain'],
        'limit': 10
    }
) as results;
```

---

## Cortex Agent

**Name**: `CDSB_DEMO.RAW.CDSB_ASSISTANT`
**Model**: `claude-sonnet-4-6`
**Tools**: Cortex Search (QLD_GOVERNMENT) + Web Search
**Budget**: 900 seconds, 400,000 tokens

The agent covers 6 QLD government departments:
1. **CDSB** — Construction/demolition safety, workplace safety, asbestos, high-risk work
2. **QLD Gov** — Government services, grants, concessions, housing, employment
3. **TMR** — Driver licensing, vehicle registration, road rules, transport infrastructure
4. **Education** — School enrolment, curriculum, teaching standards, student wellbeing
5. **Health** — Hospital services, public health, clinical guidelines, mental health
6. **DPI** — Agriculture, fisheries, biosecurity, animal welfare, pest management

---

## Configuration Reference

### Environment Variables (for standalone scripts)

| Variable | Default | Description |
|----------|---------|-------------|
| `SNOWFLAKE_CONNECTION_NAME` | `AU_DEMO50` | Snowflake CLI connection name |
| `CDSB_DATABASE` | `CDSB_DEMO` | Target database |
| `CDSB_SCHEMA` | `RAW` | Target schema |
| `CDSB_WAREHOUSE` | `COMPUTE_WH` | Compute warehouse |
| `CDSB_CRAWL_WORKERS` | `10` | Concurrent BFS crawl threads |
| `CDSB_DOWNLOAD_WORKERS` | `10` | Concurrent download threads |
| `CDSB_UPLOAD_WORKERS` | `8` | Concurrent Snowflake PUT connections |
| `CDSB_MAX_DEPTH` | `5` | Maximum BFS crawl depth |
| `CDSB_MAX_PAGES` | `5000` | Maximum pages to crawl per source |

### EXTRACTION_SOURCES Columns

| Column | Type | Description |
|--------|------|-------------|
| `source_name` | VARCHAR | Display name |
| `source_url` | VARCHAR | Starting URL for crawl |
| `crawl_method` | VARCHAR | `BFS`, `CKAN`, or `TARGETED` |
| `allowed_domains` | ARRAY | Domains to stay within during BFS (supports wildcards: `*.qld.gov.au`) |
| `cloudflare_bypass` | BOOLEAN | Use `curl_cffi` browser impersonation |
| `file_types` | ARRAY | File types to download: `pdf`, `docx`, `doc`, `csv`, `xlsx`, `xls`, `tiff` |
| `max_pages` | NUMBER | BFS page limit |
| `max_depth` | NUMBER | BFS link depth limit |
| `stage_name` | VARCHAR | Snowflake stage for downloaded files |
| `web_table_name` | VARCHAR | Table for crawled HTML page text |
| `parsed_table_name` | VARCHAR | Table for AI_PARSE_DOCUMENT output |
| `sitemap_url` | VARCHAR | Optional sitemap URL to seed BFS |
| `ckan_api_url` | VARCHAR | Required for CKAN method |
| `direct_urls` | ARRAY | Required for TARGETED method |
| `enabled` | BOOLEAN | Whether to include in extraction runs |

### Streamlit Deployment (snowflake.yml)

```yaml
definition_version: 2
entities:
  cdsb_platform:
    type: streamlit
    identifier:
      name: CDSB_PLATFORM
      database: CDSB_DEMO
      schema: RAW
    query_warehouse: COMPUTE_WH
    runtime_name: SYSTEM$ST_CONTAINER_RUNTIME_PY3_11
    compute_pool: CPU_X64_XS
    external_access_integrations:
      - ALLOW_ALL_ACCESS_INTEGRATION
    main_file: streamlit_app.py
    artifacts:
      - streamlit_app.py
      - pyproject.toml
      - engine.py
      - pipeline.py
      - app_pages/sources.py
      - app_pages/extract.py
      - app_pages/monitor.py
      - app_pages/search.py
      - app_pages/pipeline.py
```

---

## Key Technical Patterns

### Stream-to-Snowflake (Zero Local Disk)

All file downloads use a temporary file that is immediately PUT to Snowflake and deleted:

```python
with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
    tmp.write(response.content)
    tmp_path = tmp.name
try:
    cursor.execute(f"PUT 'file://{tmp_path}' @STAGE/{filename} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
finally:
    os.unlink(tmp_path)
```

### Cloudflare Bypass

Sites behind Cloudflare (TMR, Health, DPI, Education) return 403/challenge pages to normal requests. The engine uses `curl_cffi` with Chrome browser impersonation:

```python
from curl_cffi import requests as cffi_requests
session = cffi_requests.Session(impersonate="chrome")
response = session.get(url, timeout=20, allow_redirects=True)
```

### Connection Pool for Parallel Uploads

Multiple pre-authenticated Snowflake connections are pooled in a `queue.Queue` for concurrent PUT operations:

```python
pool = queue.Queue()
for _ in range(n):
    conn = get_connection()
    cur = conn.cursor()
    init_session(cur)
    pool.put((conn, cur))
```

Worker threads borrow connections, PUT files, and return connections to the pool.

### Recursive CTE Chunking

Content is chunked using a recursive CTE with overlapping windows:

- **Window**: 1,500 characters
- **Step**: 1,000 characters (= 500-character overlap)
- **Minimum**: 50 characters per chunk

This ensures no information is lost at chunk boundaries — every 500-character segment appears in at least two chunks.

---

## Troubleshooting

### "Querying non-interactive table is not supported in interactive warehouses"
Use `COMPUTE_WH` (standard warehouse), not an interactive warehouse.

### PUT command creates nested directories
This is expected. The `DIRECTORY()` function uses `RELATIVE_PATH` which handles subdirectories correctly.

### Cortex Search not updating after pipeline run
The service has a `TARGET_LAG = '1 day'`. It will refresh within that window. You can check status:
```sql
SHOW CORTEX SEARCH SERVICES IN SCHEMA CDSB_DEMO.RAW;
```

### Cloudflare sites returning empty content
Ensure `cloudflare_bypass = TRUE` is set for those sources. The `curl_cffi` library must be installed.

### Task suspended due to errors
Check the task history:
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'CDSB_HOURLY_SCRAPER',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
));
```

### Streamlit app not loading
Check that the compute pool is active:
```sql
SHOW COMPUTE POOLS;
-- If suspended, it will auto-resume when the app is accessed
```

### Redeploying the Streamlit app
```bash
cd cdsb_demo/cdsb_platform
snow streamlit deploy --replace --connection AU_DEMO50
```

---

## Current Data Volume (as of April 2026)

| Metric | Count |
|--------|-------|
| Configured sources | 9 |
| Unified documents | 37,834 |
| Total chunks | 833,016 |
| Top domain (Publications) | 274,286 chunks |
| Search service status | ACTIVE |

### Chunks by Domain (top 10)

| Domain | Chunks |
|--------|--------|
| publications.qld.gov.au | 274,286 |
| health.qld.gov.au | 193,600 |
| tmr.qld.gov.au | 119,093 |
| qld.gov.au | 73,033 |
| dpi.qld.gov.au | 46,375 |
| police.qld.gov.au | 37,838 |
| education.qld.gov.au | 30,161 |
| www.health.qld.gov.au | 26,762 |
| www.tmr.qld.gov.au | 8,141 |
| www.publications.qld.gov.au | 6,538 |
