# CDSB Cortex AI Session — 90-Minute Run Sheet

**Customer:** Department of Customer Services, Open Data and Small & Family Business (CDSB)  
**Duration:** 90 minutes  
**Account:** SFSEAPAC-AU_DEMO50 | Database: CDSB_DEMO | Schema: RAW

---

## Agenda Overview

| # | Segment | Time | Duration | Format |
|---|---------|------|----------|--------|
| 1 | Welcome & Cortex AI Overview | 0:00 | 5 min | Slides / whiteboard |
| 2 | Document Ingestion & AI Search | 0:05 | 20 min | Streamlit + Snowsight |
| 3 | Anomaly Detection on QLD Open Data | 0:25 | 20 min | Snowflake Notebook |
| 4 | Process Mining & Conformance | 0:45 | 15 min | Snowflake Notebook |
| 5 | Neo4j Graph Analytics Integration | 1:00 | 15 min | Snowflake Notebook |
| 6 | Cortex Code Live Prototyping | 1:15 | 10 min | Live in Cortex Code |
| 7 | Wrap-up & Next Steps | 1:25 | 5 min | Discussion |

---

## Segment 1: Welcome & Cortex AI Overview (5 min)

**Key messages:**
- Snowflake Cortex = AI built into the data platform — no data movement, no infra
- Three pillars: **Cortex AI Functions** (LLM in SQL), **Cortex Search** (RAG), **Cortex Agents** (orchestration)
- ML Functions: Forecasting, Anomaly Detection, Classification — all SQL-native
- Everything we demo today runs on YOUR data, in YOUR account, governed by YOUR policies

**Gov references:**
- Queensland Government is a Snowflake customer (TMR open data, Education, Health)
- Federal: Services Australia, ATO, Defence all using Snowflake
- NSW: Transport, Health, Digital.NSW

---

## Segment 2: Document Ingestion & AI Search (20 min)

**Demo: Existing CDSB Platform**
- Streamlit app: `CDSB_DEMO.RAW.CDSB_PLATFORM`
- Search service: `CDSB_DEMO.RAW.QLD_GOVERNMENT` (833K+ chunks, 9 QLD gov sources)
- Cortex Agent: `CDSB_DEMO.RAW.CDSB_ASSISTANT`

**Flow:**
1. **Show the Platform** (5 min)
   - Open Streamlit app → Sources page → 9 government sources configured
   - Show extraction monitor → scraping stats, document counts
   - "We crawled 9 QLD government websites and ingested thousands of PDFs"

2. **AI Document Processing** (5 min)
   - Show `AI_PARSE_DOCUMENT` — PDFs → structured text, no OCR pipeline needed
   - Show chunking strategy (1500 char windows, 500 overlap)
   - Key message: "Minutes, not months"

3. **Cortex Search** (5 min)
   - Live search in the app → "What is CDSB's strategic plan?"
   - Show result attribution (source URL, document title, relevance score)
   - Show SQL: `SELECT SEARCH_PREVIEW(...)` — search is just a SQL function

4. **Cortex Agent + Snowflake Intelligence** (5 min)
   - Switch to Snowsight → Snowflake Intelligence → CDSB_ASSISTANT
   - Ask: "Compare CDSB's equity plan with their strategic plan priorities"
   - Agent reasons over multiple documents, cites sources
   - Key message: "From raw PDFs to conversational AI in one afternoon"

**Talking points:**
- Time to value: web crawl → parse → chunk → search → agent in hours
- Auto-refresh: new documents appear automatically
- RBAC: agent respects Snowflake row access policies
- Multi-source: mix web pages, PDFs, Word docs — all unified

---

## Segment 3: Anomaly Detection on QLD Open Data (20 min)

**Demo: Snowflake Notebook** — `01_anomaly_detection.ipynb`  
**Data:** TMR New Business Registration Transactions (304K rows, real QLD open data from data.qld.gov.au)

**Flow:**
1. **Ingest Data** (3 min)
   - Load CSV from QLD Open Data Portal into Snowflake
   - Show data: monthly transactions by suburb, vehicle make/model
   - Aggregate to daily/weekly time series by region

2. **Build Anomaly Detection Model** (5 min)
   - `CREATE SNOWFLAKE.ML.ANOMALY_DETECTION` — one SQL statement
   - Train on historical registration patterns
   - "Snowflake handles feature engineering, model selection, hyperparameter tuning"

3. **Detect Anomalies** (5 min)
   - Call model to detect unusual spikes/drops in registration volumes
   - Visualise: time series with anomaly bands (upper/lower bounds)
   - "This suburb had a 300% spike in truck registrations — investigate"

4. **Surface via Chat** (5 min)
   - Use `AI_COMPLETE` to have an LLM interpret the anomalies in natural language
   - "Summarise the top 5 anomalies detected this quarter and suggest root causes"
   - Show how this could be automated with Tasks + Alerts

5. **Connect to Their World** (2 min)
   - "Imagine this on customer complaint volumes, permit processing times, service centre wait times"
   - Automated alerts when patterns deviate from baseline
   - No Python, no ML expertise — just SQL

**Key Snowflake features shown:**
- `SNOWFLAKE.ML.ANOMALY_DETECTION` (train + detect)
- `AI_COMPLETE` (LLM interpretation)
- Snowflake Tasks + Alerts (automation)
- Snowflake Notebooks (interactive analysis)

---

## Segment 4: Process Mining & Conformance (15 min)

**Demo: Snowflake Notebook** — `02_process_mining.ipynb`  
**Data:** Synthetic government service request lifecycle events (modelled on QLD customer service processes)

**Flow:**
1. **Generate Process Event Data** (3 min)
   - Create realistic event log: Request Submitted → Triaged → Assigned → In Progress → Resolved → Closed
   - Include timestamps, case IDs, handlers, channels (phone, web, in-person)
   - Some cases follow the "happy path", others deviate

2. **Discover Process Patterns** (5 min)
   - SQL-based process discovery: find most common paths through the system
   - Use `AI_COMPLETE` to analyse event sequences and describe discovered processes
   - "The LLM identifies that 73% of cases follow the standard path, but 12% skip triage"

3. **Conformance Analysis** (5 min)
   - Define baseline process (the "should be" flow)
   - Detect deviations: skipped steps, out-of-order activities, bottlenecks
   - Use anomaly detection on process duration — flag cases taking 10x longer
   - `AI_COMPLETE` to explain: "Cases from the web channel that skip triage have 3x longer resolution times"

4. **Neo4j Handoff Teaser** (2 min)
   - "This is where graph databases shine — let's see that next"
   - Show how we generate node/relationship tables from event data
   - Preview: case → handler → category graph structure

**Key messages:**
- No specialised process mining tool needed — SQL + LLM
- Pattern discovery scales to millions of events
- Conformance checking can be automated and alerted on
- Natural language explanations make results accessible to non-technical stakeholders

---

## Segment 5: Neo4j Graph Analytics Integration (15 min)

**Demo: Snowflake Notebook** — `03_neo4j_graph_analytics.ipynb`  
**Prerequisite:** Neo4j Graph Analytics installed from Snowflake Marketplace

**Flow:**
1. **Setup** (3 min)
   - Show Neo4j Graph Analytics in Marketplace → already installed as Native App
   - "Neo4j runs inside Snowflake via SPCS — your data never leaves"
   - Create node and relationship tables from process mining data

2. **Community Detection** (5 min)
   - Run Weakly Connected Components (WCC) to find disconnected case clusters
   - Run Louvain to detect tightly-connected communities of handlers/cases
   - "These 3 handlers always work together and resolve cases 2x faster"

3. **Fraud / Anomaly Patterns** (5 min)
   - Run PageRank on handler network — who are the most central nodes?
   - Use Node Similarity to find cases with unusually similar patterns
   - Combine with anomaly detection results: "Flagged anomalies cluster in this community"

4. **Write Back & Analyse** (2 min)
   - Results written back to Snowflake tables
   - Join graph features with original data for downstream analytics
   - "Feed graph features into ML models for better fraud detection"

**Key messages:**
- Neo4j runs as a Native App inside Snowflake — zero data movement
- Graph algorithms via SQL — no Cypher needed
- Process mining + graph = powerful conformance & fraud detection
- Results flow back into Snowflake for dashboarding, alerting, ML

---

## Segment 6: Cortex Code Live Prototyping (10 min)

**Demo: Live in Cortex Code (CLI or Snowsight)**

**Flow:**
1. **Introduction** (2 min)
   - "Cortex Code is Snowflake's AI coding assistant — like having a data engineer on call"
   - Available in Snowsight, VS Code, and CLI

2. **Live Build** (8 min) — Pick ONE based on audience energy:

   **Option A: Build a quick Streamlit dashboard**
   - "Create a Streamlit app that shows anomaly detection results with charts"
   - Cortex Code writes the app, deploys it, we test it — live in 5 minutes

   **Option B: Natural language to SQL pipeline**
   - "Write me a query that finds the top 10 suburbs with the most registration anomalies and visualise them on a map"
   - Show how Cortex Code understands the schema, writes the SQL, explains it

   **Option C: Debug and extend**
   - Take one of the notebooks we just ran
   - "Add a new cell that creates a Snowflake Alert to email me when anomalies are detected"
   - Cortex Code writes the alert, task, and notification integration

**Key messages:**
- Rapid prototyping: from idea to working code in minutes
- Context-aware: understands your schema, tables, functions
- Not just code generation — full IDE experience with execution
- Accelerates the "art of the possible" conversations

---

## Segment 7: Wrap-up & Next Steps (5 min)

**Summary slide / whiteboard:**

| Their Ask | What We Showed | Snowflake Feature |
|-----------|---------------|-------------------|
| Anomaly Detection | ML on real QLD data | `SNOWFLAKE.ML.ANOMALY_DETECTION` |
| Ingest & Chat with PDFs | 9 gov sources, 833K chunks | `AI_PARSE_DOCUMENT` + Cortex Search + Cortex Agent |
| Process Discovery | Event analysis with LLM | `AI_COMPLETE` + SQL analytics |
| Conformance / Non-conformance | Baseline comparison + alerts | Anomaly Detection + Tasks + Alerts |
| Unusual Patterns | Graph community detection | Neo4j Graph Analytics (Native App) |
| SQL to Neo4j | Graph projection from SQL tables | Neo4j SPCS integration |
| Document AI / Vector Search | RAG pipeline | Cortex Search Service |
| Time to Value | Hours, not months | Cortex Code + Notebooks |
| Cortex Code Prototyping | Live build | Cortex Code |
| Neo4j ↔ Snowflake Integration | Native App, zero ETL | Snowflake Marketplace |

**Next steps to propose:**
1. **POC:** Bring their real complaint/service data → build anomaly detection in a half-day workshop
2. **Document AI pilot:** Ingest their policy documents → build internal knowledge assistant
3. **Graph analytics exploration:** Model their service delivery network → find bottlenecks
4. **Cortex Code trial:** Give their team access → accelerate development

---

## Pre-Session Checklist

- [ ] Verify CDSB Platform Streamlit app is running
- [ ] Verify Cortex Agent CDSB_ASSISTANT responds
- [ ] Run notebook 01 (anomaly detection) end-to-end
- [ ] Run notebook 02 (process mining) end-to-end
- [ ] Install Neo4j Graph Analytics from Marketplace
- [ ] Run notebook 03 (Neo4j) end-to-end
- [ ] Test Cortex Code with a simple prompt
- [ ] Have backup screenshots/recordings in case of network issues
