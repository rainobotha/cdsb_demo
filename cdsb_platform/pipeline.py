def fqn(database, schema, name):
    return f"{database}.{schema}.{name}"


def stage_ref(database, schema, name):
    return f"@{database}.{schema}.{name}"


def parse_stage_documents(cur, parsed_table, stage_name, database, schema):
    stage_at = stage_ref(database, schema, stage_name)
    cur.execute(f"""
        CREATE OR REPLACE TABLE {fqn(database, schema, parsed_table)} AS
        SELECT
            d.RELATIVE_PATH as filename,
            LOWER(d.RELATIVE_PATH) as filename_lower,
            CASE
                WHEN LOWER(d.RELATIVE_PATH) LIKE '%.pdf'
                    OR LOWER(d.RELATIVE_PATH) LIKE '%.tiff'
                    OR LOWER(d.RELATIVE_PATH) LIKE '%.tif'
                    OR LOWER(d.RELATIVE_PATH) LIKE '%.doc'
                    OR LOWER(d.RELATIVE_PATH) LIKE '%.docx'
                THEN SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(
                    TO_FILE('{stage_at}', d.RELATIVE_PATH),
                    {{'mode': 'LAYOUT'}}
                ):content::VARCHAR
                ELSE NULL
            END as parsed_content,
            d.SIZE as file_size,
            CURRENT_TIMESTAMP() as parsed_at
        FROM DIRECTORY({stage_at}) d
        WHERE LOWER(d.RELATIVE_PATH) LIKE '%.pdf'
           OR LOWER(d.RELATIVE_PATH) LIKE '%.tiff'
           OR LOWER(d.RELATIVE_PATH) LIKE '%.tif'
           OR LOWER(d.RELATIVE_PATH) LIKE '%.doc'
           OR LOWER(d.RELATIVE_PATH) LIKE '%.docx'
    """)


def parse_csv_excel_from_stage(cur, parsed_table, stage_name, database, schema):
    stage_at = stage_ref(database, schema, stage_name)
    cur.execute(f"""
        INSERT INTO {fqn(database, schema, parsed_table)} (filename, filename_lower, parsed_content, file_size, parsed_at)
        SELECT
            d.RELATIVE_PATH,
            LOWER(d.RELATIVE_PATH),
            'CSV/Excel file: ' || d.RELATIVE_PATH || ' (size: ' || d.SIZE || ' bytes)',
            d.SIZE,
            CURRENT_TIMESTAMP()
        FROM DIRECTORY({stage_at}) d
        WHERE LOWER(d.RELATIVE_PATH) LIKE '%.csv'
           OR LOWER(d.RELATIVE_PATH) LIKE '%.xlsx'
           OR LOWER(d.RELATIVE_PATH) LIKE '%.xls'
    """)


def rebuild_unified_and_chunks(cur, sources, database, schema):
    unified_table = fqn(database, schema, "CDSB_DOCUMENTS_UNIFIED")
    chunks_table = fqn(database, schema, "CDSB_CHUNKS")
    staging_table = fqn(database, schema, "_UNIFIED_STAGING")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {unified_table} (
            source_type VARCHAR,
            source_url VARCHAR,
            title VARCHAR,
            content VARCHAR,
            domain VARCHAR,
            content_hash VARCHAR,
            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {chunks_table} (
            chunk_id VARCHAR,
            chunk_content VARCHAR,
            source_type VARCHAR,
            title VARCHAR,
            source_url VARCHAR,
            domain VARCHAR
        )
    """)

    union_parts = []
    for src in sources:
        web_t = fqn(database, schema, src["web_table_name"])
        parsed_t = fqn(database, schema, src["parsed_table_name"])
        domain = src.get("domain_label") or src["source_name"].lower()

        union_parts.append(f"""
            SELECT 'web' as source_type, url as source_url, title, content, domain,
                   MD5(content) as content_hash
            FROM {web_t}
            WHERE content IS NOT NULL AND LEN(content) > 50
        """)

        union_parts.append(f"""
            SELECT 'document' as source_type,
                   filename as source_url,
                   REPLACE(REPLACE(REPLACE(REPLACE(filename, '.pdf', ''), '.docx', ''), '.doc', ''), '.tiff', '') as title,
                   parsed_content as content,
                   '{domain}' as domain,
                   MD5(parsed_content) as content_hash
            FROM {parsed_t}
            WHERE parsed_content IS NOT NULL AND LEN(parsed_content) > 50
        """)

    if not union_parts:
        return

    cur.execute(
        f"CREATE OR REPLACE TABLE {staging_table} AS\n"
        + "\nUNION ALL\n".join(union_parts)
    )

    cur.execute(f"""
        MERGE INTO {unified_table} t
        USING {staging_table} s
        ON t.source_url = s.source_url AND t.source_type = s.source_type
        WHEN MATCHED AND t.content_hash != s.content_hash THEN UPDATE SET
            title = s.title,
            content = s.content,
            domain = s.domain,
            content_hash = s.content_hash,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (source_type, source_url, title, content, domain, content_hash, updated_at)
        VALUES
            (s.source_type, s.source_url, s.title, s.content, s.domain, s.content_hash, CURRENT_TIMESTAMP())
    """)
    merge_result = cur.fetchone()

    cur.execute(f"""
        DELETE FROM {unified_table} t
        WHERE NOT EXISTS (
            SELECT 1 FROM {staging_table} s
            WHERE s.source_url = t.source_url AND s.source_type = t.source_type
        )
    """)

    cur.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE _changed_sources AS
        SELECT source_url, source_type FROM {unified_table}
        WHERE updated_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
    """)

    cur.execute("SELECT COUNT(*) FROM _changed_sources")
    changed_count = cur.fetchone()[0]

    if changed_count > 0:
        cur.execute(f"""
            DELETE FROM {chunks_table}
            WHERE EXISTS (
                SELECT 1 FROM _changed_sources c
                WHERE c.source_url = {chunks_table}.source_url
                  AND c.source_type = {chunks_table}.source_type
            )
        """)

        cur.execute(f"""
            INSERT INTO {chunks_table} (chunk_id, chunk_content, source_type, title, source_url, domain)
            WITH RECURSIVE src AS (
                SELECT ROW_NUMBER() OVER (ORDER BY source_type, source_url) as doc_id,
                       source_type, source_url, title, content, domain
                FROM {unified_table}
                WHERE EXISTS (
                    SELECT 1 FROM _changed_sources c
                    WHERE c.source_url = {unified_table}.source_url
                      AND c.source_type = {unified_table}.source_type
                )
            ),
            chunks AS (
                SELECT doc_id, source_type, source_url, title, domain, 1 as chunk_num,
                       SUBSTR(content, 1, 1500) as chunk_content, LEN(content) as total_len
                FROM src
                UNION ALL
                SELECT c.doc_id, c.source_type, c.source_url, c.title, c.domain, c.chunk_num + 1,
                       SUBSTR(d.content, 1 + (c.chunk_num * 1000), 1500), c.total_len
                FROM chunks c JOIN src d ON c.doc_id = d.doc_id
                WHERE 1 + (c.chunk_num * 1000) <= c.total_len
            )
            SELECT doc_id || '-' || chunk_num, chunk_content, source_type, title, source_url, domain
            FROM chunks WHERE LEN(chunk_content) > 50
        """)

    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")

    return changed_count


def run_pipeline(cur, sources, database, schema, progress=None):
    if progress:
        progress.log("Pipeline: Parsing documents from stages...")

    for src in sources:
        label = src["source_name"]
        parsed_t = src["parsed_table_name"]
        stage_n = src["stage_name"]

        if progress:
            progress.log(f"  Parsing {label}...")

        try:
            parse_stage_documents(cur, parsed_t, stage_n, database, schema)
            parse_csv_excel_from_stage(cur, parsed_t, stage_n, database, schema)
        except Exception as e:
            if progress:
                progress.log(f"  Warning: parse failed for {label}: {e}")

    if progress:
        progress.log("Pipeline: Incremental merge into unified docs + chunks...")

    changed = rebuild_unified_and_chunks(cur, sources, database, schema)

    if progress:
        progress.log(f"Pipeline complete! {changed} documents changed. Search service will pick up changes incrementally.")
