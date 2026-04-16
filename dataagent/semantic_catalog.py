"""
Semantic Catalog — The persistent "Brain" of the BQ Assistant.

Replaces the simple text-blob `dataset_analysis` table with a structured
knowledge graph stored in BigQuery workspace tables. Each discovery —
column semantics, join patterns, business terms, successful queries,
value meanings — is recorded and indexed so that the agent gets smarter
with every interaction.

Tables managed (auto-created in the user's workspace dataset):
  semantic_catalog   — Column-level semantic metadata
  join_patterns      — Discovered join relationships across tables/datasets
  business_glossary  — Business term → SQL expression mappings
  query_history      — Successful (NL, SQL) pairs for similarity retrieval
  value_mappings     — Encoded value meanings (e.g. status=4 → "Churned")
  profiling_status   — Background profiling job state (running/completed/failed)
"""

import re
import time
import json
from datetime import datetime, timezone
from google.cloud import bigquery


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

_SEMANTIC_CATALOG_SCHEMA = [
    bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("data_type", "STRING"),
    bigquery.SchemaField("semantic_type", "STRING"),  # e.g. "identifier", "metric", "dimension", "timestamp"
    bigquery.SchemaField("business_name", "STRING"),  # human-friendly name
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("sample_values", "STRING"),  # JSON array of sample values
    bigquery.SchemaField("null_pct", "FLOAT"),
    bigquery.SchemaField("distinct_count", "INTEGER"),
    bigquery.SchemaField("is_partition_key", "BOOLEAN"),
    bigquery.SchemaField("is_clustering_key", "BOOLEAN"),
    bigquery.SchemaField("discovered_by", "STRING"),  # "auto_profile" | "human_feedback" | "query_learning"
    bigquery.SchemaField("confidence", "FLOAT"),  # 0.0–1.0
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
]

_JOIN_PATTERNS_SCHEMA = [
    bigquery.SchemaField("dataset_a", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("table_a", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("column_a", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("dataset_b", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("table_b", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("column_b", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("join_type", "STRING"),  # "exact_name" | "foreign_key" | "human_defined"
    bigquery.SchemaField("confidence", "FLOAT"),
    bigquery.SchemaField("discovered_by", "STRING"),
    bigquery.SchemaField("usage_count", "INTEGER"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

_BUSINESS_GLOSSARY_SCHEMA = [
    bigquery.SchemaField("term", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sql_expression", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("dataset_context", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("created_by", "STRING"),
    bigquery.SchemaField("confidence", "FLOAT"),
    bigquery.SchemaField("usage_count", "INTEGER"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
]

_QUERY_HISTORY_SCHEMA = [
    bigquery.SchemaField("natural_language", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sql", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("dataset", "STRING"),
    bigquery.SchemaField("tables_used", "STRING"),  # JSON array
    bigquery.SchemaField("result_row_count", "INTEGER"),
    bigquery.SchemaField("was_successful", "BOOLEAN"),
    bigquery.SchemaField("user_feedback", "STRING"),  # "correct" | "incorrect" | "partial" | null
    bigquery.SchemaField("feedback_notes", "STRING"),
    bigquery.SchemaField("execution_time_ms", "INTEGER"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

_VALUE_MAPPINGS_SCHEMA = [
    bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("raw_value", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("business_meaning", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("discovered_by", "STRING"),
    bigquery.SchemaField("confidence", "FLOAT"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

_PROFILING_STATUS_SCHEMA = [
    bigquery.SchemaField("dataset", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),  # running | completed | failed
    bigquery.SchemaField("phase", "STRING"),  # structural | semantic
    bigquery.SchemaField("started_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("completed_at", "TIMESTAMP"),
    bigquery.SchemaField("details", "STRING"),
    bigquery.SchemaField("job_name", "STRING"),
]

_TABLE_DEFINITIONS = {
    "semantic_catalog": _SEMANTIC_CATALOG_SCHEMA,
    "join_patterns": _JOIN_PATTERNS_SCHEMA,
    "business_glossary": _BUSINESS_GLOSSARY_SCHEMA,
    "query_history": _QUERY_HISTORY_SCHEMA,
    "value_mappings": _VALUE_MAPPINGS_SCHEMA,
    "profiling_status": _PROFILING_STATUS_SCHEMA,
}


# ---------------------------------------------------------------------------
# Table bootstrapping
# ---------------------------------------------------------------------------

def _ensure_table(client: bigquery.Client, workspace: str, table_name: str) -> str:
    """Create the catalog table if it doesn't exist. Returns the full table ID."""
    project_id = client.project
    table_id = f"{project_id}.{workspace}.{table_name}"
    schema = _TABLE_DEFINITIONS[table_name]
    try:
        client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
    return table_id


def ensure_all_tables(client: bigquery.Client, workspace: str) -> dict[str, str]:
    """Ensure all semantic catalog tables exist. Returns {name: full_table_id}."""
    return {name: _ensure_table(client, workspace, name) for name in _TABLE_DEFINITIONS}


def count_catalog_entries(client: bigquery.Client, workspace: str, dataset: str) -> int:
    """Quick check: how many catalog entries exist for this dataset?
    
    Used by load_semantic_context to decide whether to auto-profile.
    Returns 0 if the table doesn't exist yet.
    """
    try:
        table_id = _ensure_table(client, workspace, "semantic_catalog")
        rows = list(client.query(
            f"SELECT COUNT(*) as cnt FROM `{table_id}` WHERE dataset = '{dataset}'"
        ).result())
        return rows[0].cnt if rows else 0
    except Exception:
        return 0


def set_profiling_status(
    client: bigquery.Client,
    workspace: str,
    dataset: str,
    status: str,
    phase: str = "",
    details: str = "",
    job_name: str = "",
) -> None:
    """
    Set the profiling status for a dataset (upsert).

    status: "running", "completed", or "failed"
    phase: "structural" or "semantic" (while running)
    """
    table_id = _ensure_table(client, workspace, "profiling_status")
    now = _now_ts()

    # For a running → running update (phase change), preserve started_at
    # by reading the existing record BEFORE deleting it
    preserved_started_at = None
    if status == "running" and phase:
        try:
            existing = list(client.query(
                f"SELECT started_at FROM `{table_id}` WHERE dataset = '{_escape(dataset)}'"
            ).result())
            if existing and existing[0].started_at:
                preserved_started_at = existing[0].started_at.isoformat()
        except Exception:
            pass

    # Delete existing row for this dataset, then insert (upsert)
    try:
        client.query(
            f"DELETE FROM `{table_id}` WHERE dataset = '{_escape(dataset)}'"
        ).result()
    except Exception:
        pass

    row = {
        "dataset": dataset,
        "status": status,
        "phase": phase,
        "started_at": preserved_started_at or now,
        "updated_at": now,
        "completed_at": now if status in ("completed", "failed") else None,
        "details": details or "",
        "job_name": job_name or "",
    }

    client.insert_rows_json(table_id, [row])


def get_profiling_status(
    client: bigquery.Client,
    workspace: str,
    dataset: str,
) -> dict:
    """
    Get the profiling status for a dataset.

    Returns a dict with at least {"status": "running|completed|failed|unknown"}.
    Detects stale "running" statuses (> 30 min) and treats them as "failed".
    """
    _STALE_MINUTES = 30

    try:
        table_id = _ensure_table(client, workspace, "profiling_status")
        rows = list(client.query(
            f"SELECT * FROM `{table_id}` "
            f"WHERE dataset = '{_escape(dataset)}' "
            f"ORDER BY updated_at DESC LIMIT 1"
        ).result())
    except Exception:
        return {"status": "unknown"}

    if not rows:
        return {"status": "unknown"}

    r = rows[0]
    result = {
        "status": r.status,
        "phase": r.phase or "",
        "started_at": r.started_at.isoformat() if r.started_at else "",
        "updated_at": r.updated_at.isoformat() if r.updated_at else "",
        "completed_at": r.completed_at.isoformat() if r.completed_at else "",
        "details": r.details or "",
        "job_name": r.job_name or "",
    }

    # Stale detection: if "running" for > 30 min, treat as failed
    if result["status"] == "running" and r.updated_at:
        elapsed = (datetime.now(timezone.utc) - r.updated_at.replace(tzinfo=timezone.utc)).total_seconds()
        if elapsed > _STALE_MINUTES * 60:
            result["status"] = "failed"
            result["details"] = f"Stale: no update for {elapsed/60:.0f} min (timeout={_STALE_MINUTES}m)"

    return result


def _escape(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return s.replace("'", "\\'")


def _now_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Deep profiling
# ---------------------------------------------------------------------------

def profile_table(
    client: bigquery.Client,
    dataset: str,
    table_name: str,
    workspace: str,
    max_sample_values: int = 5,
) -> dict:
    """
    Profile a table using INFORMATION_SCHEMA and data sampling queries.

    Returns a dict with column-level metadata ready for catalog insertion,
    plus table-level statistics.
    """
    # Resolve project
    if "." in dataset:
        project_id, dataset_id = dataset.split(".", 1)
    else:
        project_id = client.project
        dataset_id = dataset

    full_dataset = f"{project_id}.{dataset_id}"

    # 1. INFORMATION_SCHEMA for column metadata
    info_sql = f"""
    SELECT
        column_name,
        data_type,
        is_nullable,
        column_default,
        is_partitioning_column,
        clustering_ordinal_position
    FROM `{full_dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position
    """

    try:
        info_rows = list(client.query(info_sql).result())
    except Exception as e:
        # Fallback: use client.get_table for basic schema
        info_rows = []
        try:
            t = client.get_table(f"{full_dataset}.{table_name}")
            for f in t.schema:
                info_rows.append({
                    "column_name": f.name,
                    "data_type": f.field_type,
                    "is_nullable": f.mode != "REQUIRED",
                    "column_default": None,
                    "is_partitioning_column": "NO",
                    "clustering_ordinal_position": None,
                })
        except Exception:
            return {"error": f"Could not profile table {table_name}: {e}"}

    # 2. Table-level stats
    table_ref = f"`{full_dataset}.{table_name}`"
    try:
        t = client.get_table(f"{full_dataset}.{table_name}")
        total_rows = t.num_rows
        total_bytes = t.num_bytes
    except Exception:
        total_rows = 0
        total_bytes = 0

    # 3. Per-column profiling (batched into a single query for efficiency)
    columns_meta = []
    col_names = []

    for r in info_rows:
        if isinstance(r, dict):
            col_name = r["column_name"]
            data_type = r["data_type"]
            is_partition = r.get("is_partitioning_column", "NO") == "YES"
            is_clustering = r.get("clustering_ordinal_position") is not None
        else:
            col_name = r.column_name
            data_type = r.data_type
            is_partition = getattr(r, "is_partitioning_column", "NO") == "YES"
            clustering_pos = getattr(r, "clustering_ordinal_position", None)
            is_clustering = clustering_pos is not None

        col_names.append(col_name)
        columns_meta.append({
            "column_name": col_name,
            "data_type": data_type,
            "is_partition_key": is_partition,
            "is_clustering_key": is_clustering,
        })

    # Size thresholds for profiling strategy
    # Keep these low — full table scans (APPROX_COUNT_DISTINCT on every column)
    # can easily take 5+ minutes on tables with millions of rows, causing HTTP timeouts.
    _LARGE_TABLE_BYTES = 2 * 1024**3    # 2 GB
    _LARGE_TABLE_ROWS = 5_000_000       # 5M rows
    is_large_table = total_bytes > _LARGE_TABLE_BYTES or total_rows > _LARGE_TABLE_ROWS

    # Build a single profiling query for all columns
    # Skip for very large tables to avoid timeouts
    if col_names and total_rows > 0 and not is_large_table:
        profile_parts = []
        for col in col_names:
            safe_col = f"`{col}`"
            profile_parts.append(
                f"COUNTIF({safe_col} IS NULL) AS null_{col}, "
                f"APPROX_COUNT_DISTINCT({safe_col}) AS distinct_{col}"
            )

        profile_sql = f"SELECT {', '.join(profile_parts)} FROM {table_ref}"
        try:
            profile_result = list(client.query(profile_sql).result())
            if profile_result:
                row = profile_result[0]
                for cm in columns_meta:
                    cn = cm["column_name"]
                    null_count = getattr(row, f"null_{cn}", 0) or 0
                    cm["null_pct"] = round(null_count / max(total_rows, 1) * 100, 2)
                    cm["distinct_count"] = getattr(row, f"distinct_{cn}", 0) or 0
        except Exception as e:
            print(f"Warning: column profiling query failed: {e}")

        # Sample values per column (limited queries)
        for cm in columns_meta:
            cn = cm["column_name"]
            safe_col = f"`{cn}`"
            try:
                sample_sql = f"SELECT {safe_col} FROM {table_ref} WHERE {safe_col} IS NOT NULL LIMIT {max_sample_values}"
                sample_rows = list(client.query(sample_sql).result())
                cm["sample_values"] = json.dumps([str(getattr(r, cn, "")) for r in sample_rows])
            except Exception:
                cm["sample_values"] = "[]"
    elif is_large_table:
        print(f"INFO: Skipping data profiling for large table {table_name} "
              f"({total_rows:,} rows, {total_bytes/1e9:.1f} GB) — schema-only mode")
        for cm in columns_meta:
            cm["null_pct"] = 0
            cm["distinct_count"] = 0
            cm["sample_values"] = "[]"

    # 4. Infer semantic types
    for cm in columns_meta:
        cm["semantic_type"] = _infer_semantic_type(cm["column_name"], cm["data_type"], cm.get("distinct_count", 0), total_rows)
        cm["confidence"] = 0.6 if cm["semantic_type"] != "unknown" else 0.3
        # Lower confidence for large tables where we didn't profile data
        if is_large_table:
            cm["confidence"] = max(cm["confidence"] - 0.2, 0.2)

    return {
        "dataset": dataset,
        "table_name": table_name,
        "total_rows": total_rows,
        "total_bytes": total_bytes,
        "schema_only": is_large_table,
        "columns": columns_meta,
    }


def _infer_semantic_type(col_name: str, data_type: str, distinct_count: int, total_rows: int) -> str:
    """
    Heuristic inference of semantic type from column metadata.
    """
    name_lower = col_name.lower()
    dtype_upper = (data_type or "").upper()

    # Identifiers
    if name_lower.endswith("_id") or name_lower == "id" or name_lower.endswith("_key"):
        return "identifier"

    # Timestamps / dates
    if "TIMESTAMP" in dtype_upper or "DATE" in dtype_upper or "TIME" in dtype_upper:
        return "timestamp"
    if any(kw in name_lower for kw in ["date", "time", "created", "updated", "modified", "_at", "_on"]):
        return "timestamp"

    # Metrics (numeric with high cardinality)
    if dtype_upper in ("FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC", "INT64", "INTEGER"):
        if any(kw in name_lower for kw in ["amount", "total", "count", "sum", "avg", "price", "cost", "revenue", "qty", "quantity", "score", "rate"]):
            return "metric"
        if total_rows > 0 and distinct_count > total_rows * 0.5:
            return "metric"
        return "measure"

    # Dimensions (string with low-medium cardinality)
    if dtype_upper == "STRING":
        if any(kw in name_lower for kw in ["name", "label", "title", "desc", "category", "type", "status", "region", "country", "city", "state"]):
            return "dimension"
        if total_rows > 0 and distinct_count < total_rows * 0.1:
            return "dimension"
        if any(kw in name_lower for kw in ["url", "uri", "path", "link", "email", "address"]):
            return "attribute"
        return "attribute"

    # Boolean
    if dtype_upper in ("BOOLEAN", "BOOL"):
        return "flag"

    return "unknown"


def save_profile_to_catalog(
    client: bigquery.Client,
    workspace: str,
    profile: dict,
) -> int:
    """
    Persist a profile result into the semantic_catalog table.
    Returns the number of rows inserted.
    """
    table_id = _ensure_table(client, workspace, "semantic_catalog")
    now = _now_ts()

    dataset = profile["dataset"]
    tbl = profile["table_name"]

    # Upsert: delete existing entries for this table, then insert fresh
    try:
        delete_sql = (
            f"DELETE FROM `{table_id}` "
            f"WHERE dataset = '{dataset}' AND table_name = '{tbl}' "
            f"AND discovered_by = 'auto_profile'"
        )
        client.query(delete_sql).result()
    except Exception:
        pass

    rows = []
    for cm in profile.get("columns", []):
        rows.append({
            "dataset": dataset,
            "table_name": tbl,
            "column_name": cm["column_name"],
            "data_type": cm.get("data_type"),
            "semantic_type": cm.get("semantic_type"),
            "business_name": None,
            "description": None,
            "sample_values": cm.get("sample_values", "[]"),
            "null_pct": cm.get("null_pct", 0),
            "distinct_count": cm.get("distinct_count", 0),
            "is_partition_key": cm.get("is_partition_key", False),
            "is_clustering_key": cm.get("is_clustering_key", False),
            "discovered_by": "auto_profile",
            "confidence": cm.get("confidence", 0.5),
            "created_at": now,
            "updated_at": now,
        })

    if rows:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            print(f"Warning: catalog insert errors: {errors[:3]}")
    return len(rows)


# ---------------------------------------------------------------------------
# Semantic context retrieval (prompt-ready)
# ---------------------------------------------------------------------------

def get_semantic_context(client: bigquery.Client, workspace: str, dataset: str, tables: list[str] | None = None) -> str:
    """
    Build a prompt-ready summary of all known semantics for the given dataset.

    Returns a structured text block that the LLM can use as context for
    query planning.
    """
    ids = ensure_all_tables(client, workspace)
    catalog_id = ids["semantic_catalog"]
    join_id = ids["join_patterns"]
    glossary_id = ids["business_glossary"]

    sections = []

    # 1. Column catalog
    where = f"WHERE dataset = '{dataset}'"
    if tables:
        tables_str = ", ".join([f"'{t}'" for t in tables])
        where += f" AND table_name IN ({tables_str})"

    try:
        cat_sql = f"SELECT * FROM `{catalog_id}` {where} ORDER BY table_name, column_name"
        cat_rows = list(client.query(cat_sql).result())

        if cat_rows:
            current_table = None
            table_section = []
            for r in cat_rows:
                if r.table_name != current_table:
                    if table_section:
                        sections.append("\n".join(table_section))
                    current_table = r.table_name
                    table_section = [f"\n### Table: {current_table}"]

                extras = []
                if r.semantic_type:
                    extras.append(f"type={r.semantic_type}")
                if r.business_name:
                    extras.append(f"aka '{r.business_name}'")
                if r.null_pct and r.null_pct > 50:
                    extras.append(f"⚠️ {r.null_pct:.0f}% NULL")
                if r.is_partition_key:
                    extras.append("🔑 partition key")
                if r.sample_values and r.sample_values != "[]":
                    try:
                        samples = json.loads(r.sample_values)[:3]
                        extras.append(f"e.g. {samples}")
                    except Exception:
                        pass

                desc = r.description or ""
                extra_str = f" [{', '.join(extras)}]" if extras else ""
                table_section.append(f"  - `{r.column_name}` ({r.data_type}){extra_str} {desc}")

            if table_section:
                sections.append("\n".join(table_section))
    except Exception:
        pass

    # 2. Join patterns
    try:
        join_sql = (
            f"SELECT * FROM `{join_id}` "
            f"WHERE dataset_a = '{dataset}' OR dataset_b = '{dataset}' "
            f"ORDER BY confidence DESC LIMIT 20"
        )
        join_rows = list(client.query(join_sql).result())
        if join_rows:
            join_section = ["\n### Known Join Patterns"]
            for r in join_rows:
                join_section.append(
                    f"  - `{r.table_a}.{r.column_a}` ↔ `{r.table_b}.{r.column_b}` "
                    f"({r.join_type}, confidence={r.confidence:.0%}, used {r.usage_count}x)"
                )
            sections.append("\n".join(join_section))
    except Exception:
        pass

    # 3. Business glossary
    try:
        gloss_sql = (
            f"SELECT * FROM `{glossary_id}` "
            f"WHERE dataset_context = '{dataset}' OR dataset_context IS NULL "
            f"ORDER BY usage_count DESC LIMIT 20"
        )
        gloss_rows = list(client.query(gloss_sql).result())
        if gloss_rows:
            gloss_section = ["\n### Business Glossary"]
            for r in gloss_rows:
                gloss_section.append(f"  - **{r.term}** = `{r.sql_expression}` — {r.description or ''}")
            sections.append("\n".join(gloss_section))
    except Exception:
        pass

    # 4. Value mappings
    try:
        vm_where = f"WHERE dataset = '{dataset}'"
        if tables:
            tables_str = ", ".join([f"'{t}'" for t in tables])
            vm_where += f" AND table_name IN ({tables_str})"
        vm_sql = f"SELECT * FROM `{ids['value_mappings']}` {vm_where} ORDER BY table_name, column_name"
        vm_rows = list(client.query(vm_sql).result())
        if vm_rows:
            vm_section = ["\n### Value Mappings"]
            for r in vm_rows:
                vm_section.append(f"  - `{r.table_name}.{r.column_name}` = '{r.raw_value}' → \"{r.business_meaning}\"")
            sections.append("\n".join(vm_section))
    except Exception:
        pass

    # 5. Ambiguity detection — flag columns the system doesn't understand well
    try:
        if cat_rows:
            ambiguities = []
            for r in cat_rows:
                if r.semantic_type == "unknown" or (r.confidence is not None and r.confidence < 0.5):
                    conf_str = f"{r.confidence:.0%}" if r.confidence is not None else "?"
                    ambiguities.append(
                        f"  - `{r.table_name}.{r.column_name}` ({r.data_type}) — "
                        f"type={r.semantic_type or '?'}, confidence={conf_str}"
                    )
                elif r.null_pct is not None and r.null_pct > 80:
                    ambiguities.append(
                        f"  - `{r.table_name}.{r.column_name}` — ⚠️ {r.null_pct:.0f}% NULL (mostly empty, may not be useful for filtering)"
                    )
            if ambiguities:
                sections.append(
                    "\n### ⚠️ Ambiguous Columns (ask the user if relevant to their question)\n"
                    "The following columns could not be reliably classified. "
                    "If the user's question involves any of these, ASK them to clarify "
                    "before writing SQL. Record their answer using `submit_feedback`.\n"
                    + "\n".join(ambiguities)
                )
    except Exception:
        pass

    if not sections:
        return f"No semantic knowledge available yet for dataset '{dataset}'. Consider running `profile_dataset` first."

    header = f"## Semantic Knowledge for `{dataset}`\n"
    return header + "\n".join(sections)


# ---------------------------------------------------------------------------
# Query history
# ---------------------------------------------------------------------------

def record_successful_query(
    client: bigquery.Client,
    workspace: str,
    natural_language: str,
    sql: str,
    dataset: str,
    tables_used: list[str] | None = None,
    result_row_count: int = 0,
    execution_time_ms: int = 0,
) -> None:
    """Record a successful query for future similarity retrieval."""
    table_id = _ensure_table(client, workspace, "query_history")
    row = {
        "natural_language": natural_language,
        "sql": sql,
        "dataset": dataset,
        "tables_used": json.dumps(tables_used or []),
        "result_row_count": result_row_count,
        "was_successful": True,
        "user_feedback": None,
        "feedback_notes": None,
        "execution_time_ms": execution_time_ms,
        "created_at": _now_ts(),
    }
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        print(f"Warning: query history insert error: {errors[:2]}")


def find_similar_queries(
    client: bigquery.Client,
    workspace: str,
    nl_query: str,
    dataset: str,
    limit: int = 5,
) -> list[dict]:
    """
    Find similar past queries using keyword overlap.

    Returns a list of dicts with {natural_language, sql, result_row_count, user_feedback}.
    """
    table_id = _ensure_table(client, workspace, "query_history")

    # Extract keywords for matching
    keywords = _extract_keywords(nl_query)
    if not keywords:
        return []

    # Build a scoring query using keyword matching
    # Each keyword match in the natural_language field adds to the score
    conditions = []
    for kw in keywords[:10]:  # limit to top 10 keywords
        safe_kw = kw.replace("'", "\\'")
        conditions.append(f"IF(LOWER(natural_language) LIKE '%{safe_kw}%', 1, 0)")

    score_expr = " + ".join(conditions)
    sql = (
        f"SELECT * FROM ("
        f"SELECT natural_language, sql, result_row_count, user_feedback, "
        f"({score_expr}) AS relevance_score "
        f"FROM `{table_id}` "
        f"WHERE dataset = '{dataset}' AND was_successful = TRUE"
        f") sub "
        f"WHERE relevance_score > 0 "
        f"ORDER BY relevance_score DESC "
        f"LIMIT {limit}"
    )

    try:
        rows = list(client.query(sql).result())
        return [
            {
                "natural_language": r.natural_language,
                "sql": r.sql,
                "result_row_count": r.result_row_count,
                "user_feedback": r.user_feedback,
                "relevance_score": r.relevance_score,
            }
            for r in rows
        ]
    except Exception as e:
        print(f"Warning: similar query search failed: {e}")
        return []


_STOP_WORDS = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "need", "must", "ought",
    "i", "me", "my", "we", "our", "you", "your", "he", "him", "she",
    "her", "it", "its", "they", "them", "their", "what", "which", "who",
    "this", "that", "these", "those", "am", "been", "from", "for",
    "with", "about", "between", "through", "during", "before", "after",
    "above", "below", "to", "of", "in", "on", "at", "by", "up", "out",
    "off", "over", "under", "not", "no", "nor", "and", "but", "or",
    "so", "if", "then", "than", "too", "very", "just", "only", "also",
    "how", "all", "each", "every", "both", "few", "more", "most",
    "other", "some", "such", "into", "show", "get", "find", "give",
    "tell", "list", "display", "want", "like", "many", "much",
}


def _extract_keywords(text: str) -> list[str]:
    """Extract meaningful keywords from a natural language query."""
    words = re.findall(r'[a-zA-Z_]+', text.lower())
    return [w for w in words if w not in _STOP_WORDS and len(w) > 2]


# ---------------------------------------------------------------------------
# Feedback processing
# ---------------------------------------------------------------------------

def update_catalog_from_feedback(
    client: bigquery.Client,
    workspace: str,
    dataset: str,
    feedback_type: str,
    feedback_data: dict,
) -> str:
    """
    Process human feedback and update the appropriate catalog table.

    feedback_type: "column_rename" | "value_mapping" | "glossary" | "join_pattern" | "query_correction"

    feedback_data varies by type:
      column_rename:   {table, column, business_name, description}
      value_mapping:   {table, column, raw_value, business_meaning}
      glossary:        {term, sql_expression, description}
      join_pattern:    {table_a, column_a, table_b, column_b}
      query_correction: {original_nl, original_sql, corrected_sql, notes}
    """
    ids = ensure_all_tables(client, workspace)
    now = _now_ts()
    messages = []

    if feedback_type == "column_rename":
        table_id = ids["semantic_catalog"]
        tbl = feedback_data.get("table", "")
        col = feedback_data.get("column", "")
        biz_name = feedback_data.get("business_name", "")
        desc = feedback_data.get("description", "")

        update_parts = []
        if biz_name:
            update_parts.append(f"business_name = '{_escape(biz_name)}'")
        if desc:
            update_parts.append(f"description = '{_escape(desc)}'")
        update_parts.append(f"discovered_by = 'human_feedback'")
        update_parts.append(f"confidence = 1.0")
        update_parts.append(f"updated_at = TIMESTAMP('{now}')")

        if update_parts:
            sql = (
                f"UPDATE `{table_id}` SET {', '.join(update_parts)} "
                f"WHERE dataset = '{dataset}' AND table_name = '{tbl}' AND column_name = '{col}'"
            )
            try:
                client.query(sql).result()
                messages.append(f"Updated column metadata for {tbl}.{col}")
            except Exception as e:
                messages.append(f"Error updating column: {e}")

    elif feedback_type == "value_mapping":
        table_id = ids["value_mappings"]
        row = {
            "dataset": dataset,
            "table_name": feedback_data.get("table", ""),
            "column_name": feedback_data.get("column", ""),
            "raw_value": str(feedback_data.get("raw_value", "")),
            "business_meaning": feedback_data.get("business_meaning", ""),
            "discovered_by": "human_feedback",
            "confidence": 1.0,
            "created_at": now,
        }
        errors = client.insert_rows_json(table_id, [row])
        if not errors:
            messages.append(f"Recorded value mapping: {row['raw_value']} → {row['business_meaning']}")
        else:
            messages.append(f"Error recording value mapping: {errors}")

    elif feedback_type == "glossary":
        table_id = ids["business_glossary"]
        term = feedback_data.get("term", "")
        # Upsert: delete then insert
        try:
            client.query(
                f"DELETE FROM `{table_id}` WHERE term = '{_escape(term)}' "
                f"AND (dataset_context = '{dataset}' OR dataset_context IS NULL)"
            ).result()
        except Exception:
            pass
        row = {
            "term": term,
            "sql_expression": feedback_data.get("sql_expression", ""),
            "dataset_context": dataset,
            "description": feedback_data.get("description", ""),
            "created_by": "human_feedback",
            "confidence": 1.0,
            "usage_count": 0,
            "created_at": now,
            "updated_at": now,
        }
        errors = client.insert_rows_json(table_id, [row])
        if not errors:
            messages.append(f"Added glossary term: '{term}'")
        else:
            messages.append(f"Error adding glossary term: {errors}")

    elif feedback_type == "join_pattern":
        table_id = ids["join_patterns"]
        row = {
            "dataset_a": dataset,
            "table_a": feedback_data.get("table_a", ""),
            "column_a": feedback_data.get("column_a", ""),
            "dataset_b": feedback_data.get("dataset_b", dataset),
            "table_b": feedback_data.get("table_b", ""),
            "column_b": feedback_data.get("column_b", ""),
            "join_type": "human_defined",
            "confidence": 1.0,
            "discovered_by": "human_feedback",
            "usage_count": 0,
            "created_at": now,
        }
        errors = client.insert_rows_json(table_id, [row])
        if not errors:
            messages.append(f"Recorded join: {row['table_a']}.{row['column_a']} ↔ {row['table_b']}.{row['column_b']}")
        else:
            messages.append(f"Error recording join: {errors}")

    elif feedback_type == "query_correction":
        # Record the correction in query_history
        table_id = ids["query_history"]
        row = {
            "natural_language": feedback_data.get("original_nl", ""),
            "sql": feedback_data.get("corrected_sql", ""),
            "dataset": dataset,
            "tables_used": "[]",
            "result_row_count": 0,
            "was_successful": True,
            "user_feedback": "corrected",
            "feedback_notes": feedback_data.get("notes", ""),
            "execution_time_ms": 0,
            "created_at": now,
        }
        errors = client.insert_rows_json(table_id, [row])
        if not errors:
            messages.append("Recorded query correction for future learning")
        else:
            messages.append(f"Error recording correction: {errors}")
    else:
        messages.append(f"Unknown feedback type: {feedback_type}")

    return "; ".join(messages)


# ---------------------------------------------------------------------------
# Join discovery
# ---------------------------------------------------------------------------

def discover_joins(
    client: bigquery.Client,
    workspace: str,
    dataset: str,
    tables: list[str] | None = None,
) -> list[dict]:
    """
    Auto-discover potential join relationships between tables
    based on column name matching and type compatibility.

    Returns a list of discovered join pattern dicts.
    """
    catalog_id = _ensure_table(client, workspace, "semantic_catalog")
    join_id = _ensure_table(client, workspace, "join_patterns")

    # Get all columns for this dataset
    where = f"WHERE dataset = '{dataset}'"
    if tables:
        tables_str = ", ".join([f"'{t}'" for t in tables])
        where += f" AND table_name IN ({tables_str})"

    try:
        sql = f"SELECT table_name, column_name, data_type, semantic_type FROM `{catalog_id}` {where}"
        rows = list(client.query(sql).result())
    except Exception:
        return []

    # Group by (column_name, data_type)
    col_to_tables: dict[tuple[str, str], list[str]] = {}
    for r in rows:
        key = (r.column_name.lower(), r.data_type)
        if key not in col_to_tables:
            col_to_tables[key] = []
        col_to_tables[key].append(r.table_name)

    # Find columns that appear in multiple tables → likely join keys
    discoveries = []
    now = _now_ts()
    for (col_name, data_type), table_list in col_to_tables.items():
        if len(table_list) < 2:
            continue
        # Skip generic columns that are unlikely join keys
        if col_name in ("created_at", "updated_at", "modified_at", "description", "name"):
            continue

        for i, tbl_a in enumerate(table_list):
            for tbl_b in table_list[i + 1:]:
                discoveries.append({
                    "dataset_a": dataset,
                    "table_a": tbl_a,
                    "column_a": col_name,
                    "dataset_b": dataset,
                    "table_b": tbl_b,
                    "column_b": col_name,
                    "join_type": "exact_name",
                    "confidence": 0.7,
                    "discovered_by": "auto_profile",
                    "usage_count": 0,
                    "created_at": now,
                })

    # Save discoveries (skip if already known)
    if discoveries:
        for d in discoveries:
            try:
                check_sql = (
                    f"SELECT COUNT(*) as cnt FROM `{join_id}` "
                    f"WHERE table_a = '{d['table_a']}' AND column_a = '{d['column_a']}' "
                    f"AND table_b = '{d['table_b']}' AND column_b = '{d['column_b']}'"
                )
                check = list(client.query(check_sql).result())
                if check and check[0].cnt == 0:
                    client.insert_rows_json(join_id, [d])
            except Exception:
                pass

    return discoveries


def get_join_suggestions(
    client: bigquery.Client,
    workspace: str,
    tables: list[str],
    dataset: str,
) -> str:
    """
    Get known join patterns for a set of tables.
    Returns a prompt-ready text block.
    """
    join_id = _ensure_table(client, workspace, "join_patterns")

    if len(tables) < 2:
        return "Need at least 2 tables to suggest joins."

    tables_str = ", ".join([f"'{t}'" for t in tables])
    sql = (
        f"SELECT * FROM `{join_id}` "
        f"WHERE (table_a IN ({tables_str}) AND table_b IN ({tables_str})) "
        f"OR (table_a IN ({tables_str}) AND dataset_b = '{dataset}') "
        f"ORDER BY confidence DESC, usage_count DESC "
        f"LIMIT 10"
    )

    try:
        rows = list(client.query(sql).result())
        if not rows:
            return f"No known join patterns for tables: {', '.join(tables)}. Consider running `profile_dataset` to discover them."

        lines = ["## Suggested Joins"]
        for r in rows:
            conf_label = "HIGH" if r.confidence >= 0.8 else ("MEDIUM" if r.confidence >= 0.5 else "LOW")
            lines.append(
                f"  - `{r.table_a}`.`{r.column_a}` = `{r.table_b}`.`{r.column_b}` "
                f"[{conf_label} confidence, {r.join_type}, used {r.usage_count}x]"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching join suggestions: {e}"


# ---------------------------------------------------------------------------
# Migration: existing dataset_analysis blobs → semantic catalog
# ---------------------------------------------------------------------------

def migrate_legacy_analysis(
    client: bigquery.Client,
    workspace: str,
    dataset: str,
) -> str:
    """
    Read existing `dataset_analysis` table entries and preserve them as
    description annotations in the semantic catalog. Best-effort: the
    unstructured text is stored as a description on a synthetic
    '_legacy_analysis' column entry so it's available in get_semantic_context.
    """
    project_id = client.project
    legacy_table = f"{project_id}.{workspace}.dataset_analysis"
    catalog_id = _ensure_table(client, workspace, "semantic_catalog")
    now = _now_ts()

    try:
        rows = list(client.query(
            f"SELECT dataset_name, analysis FROM `{legacy_table}` "
            f"WHERE dataset_name = '{dataset}'"
        ).result())
    except Exception:
        return "No legacy analysis data found to migrate."

    if not rows:
        return "No legacy analysis data found to migrate."

    migrated = 0
    for r in rows:
        entry = {
            "dataset": r.dataset_name,
            "table_name": "_legacy_notes",
            "column_name": f"_note_{migrated}",
            "data_type": "TEXT",
            "semantic_type": "legacy_note",
            "business_name": None,
            "description": r.analysis[:2000],  # Truncate very long blobs
            "sample_values": "[]",
            "null_pct": 0,
            "distinct_count": 0,
            "is_partition_key": False,
            "is_clustering_key": False,
            "discovered_by": "legacy_migration",
            "confidence": 0.5,
            "created_at": now,
            "updated_at": now,
        }
        try:
            client.insert_rows_json(catalog_id, [entry])
            migrated += 1
        except Exception:
            pass

    return f"Migrated {migrated} legacy analysis entries for '{dataset}'."


# ---------------------------------------------------------------------------
# Glossary suggestion
# ---------------------------------------------------------------------------

# Common phrases that should NOT be suggested as business terms
_NOISE_PHRASES = {
    "the top", "show me", "how many", "what is", "what are", "give me",
    "list of", "list all", "total number", "can you", "i want", "i need",
    "please show", "tell me", "most popular", "top most", "in the",
    "for the", "by the", "from the", "to the", "of the", "and the",
    "group by", "order by", "sort by",
}


def suggest_glossary_entries(
    client: bigquery.Client,
    workspace: str,
    dataset: str,
    nl_question: str,
    sql: str,
) -> list[str]:
    """Identify candidate business terms from a successful NL→SQL mapping.

    Extracts 2-3 word phrases from the NL question that:
    - Are not already in the business glossary
    - Are not common stop phrases
    - Appear to be domain-specific (contain at least one meaningful word)

    The LLM agent is expected to evaluate these candidates and decide
    which ones to present to the user as glossary suggestions.

    Returns a list of candidate term strings (max 3).
    """
    # Get existing glossary terms
    existing_terms = set()
    try:
        glossary_id = _ensure_table(client, workspace, "business_glossary")
        rows = list(client.query(
            f"SELECT term FROM `{glossary_id}` "
            f"WHERE dataset_context = '{dataset}' OR dataset_context IS NULL"
        ).result())
        existing_terms = {r.term.lower() for r in rows}
    except Exception:
        pass

    # Extract candidate phrases (2 and 3 word ngrams)
    words = re.findall(r'[a-zA-Z_]+', nl_question.lower())
    candidates = []

    for length in [3, 2]:
        for i in range(len(words) - length + 1):
            phrase = " ".join(words[i:i + length])
            if phrase in _NOISE_PHRASES:
                continue
            if phrase in existing_terms:
                continue
            # Must contain at least one word not in the stop list
            has_meaningful = any(
                w not in _STOP_WORDS and len(w) > 2 for w in words[i:i + length]
            )
            if has_meaningful:
                candidates.append(phrase)

    # Deduplicate and cap at 3
    seen = set()
    unique = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)
    return unique[:3]

