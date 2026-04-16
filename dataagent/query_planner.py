"""
Query Planner — Self-correction loop for the BQ Assistant.

Provides diagnostic functions that help the agent understand WHY a query
failed or returned empty results, and what to try next. These are
called as tools by the agent rather than running as a separate
autonomous agent, keeping latency and cost reasonable.
"""

import re
import json
from google.cloud import bigquery


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def dry_run_query(client: bigquery.Client, sql: str) -> dict:
    """
    Validate SQL and estimate cost using BigQuery's dry_run flag.

    Returns {
        "valid": bool,
        "error": str | None,
        "estimated_bytes": int,
        "estimated_cost_usd": float,   # approximate at $5/TB
    }
    """
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    try:
        job = client.query(sql, job_config=job_config)
        estimated_bytes = job.total_bytes_processed or 0
        estimated_tb = estimated_bytes / (1024 ** 4)
        estimated_cost = estimated_tb * 5.0  # $5/TB on-demand pricing
        return {
            "valid": True,
            "error": None,
            "estimated_bytes": estimated_bytes,
            "estimated_cost_usd": round(estimated_cost, 4),
        }
    except Exception as e:
        return {
            "valid": False,
            "error": str(e),
            "estimated_bytes": 0,
            "estimated_cost_usd": 0,
        }


# ---------------------------------------------------------------------------
# Empty result diagnosis
# ---------------------------------------------------------------------------

def diagnose_empty_result(
    client: bigquery.Client,
    sql: str,
    dataset: str,
) -> str:
    """
    When a query returns 0 rows, analyze the WHERE/HAVING clauses
    to determine which filter is too restrictive.

    Returns a diagnostic text with suggestions.
    """
    diagnostics = []

    # 1. Extract WHERE conditions
    where_match = re.search(r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|HAVING|$)', sql, re.IGNORECASE | re.DOTALL)
    if not where_match:
        diagnostics.append("No WHERE clause found — the table itself may be empty.")
        # Check if table has data
        from_match = re.search(r'FROM\s+`?([^\s`]+)`?', sql, re.IGNORECASE)
        if from_match:
            table_ref = from_match.group(1)
            try:
                count_sql = f"SELECT COUNT(*) as cnt FROM `{table_ref}`"
                result = list(client.query(count_sql).result())
                if result:
                    cnt = result[0].cnt
                    diagnostics.append(f"Table `{table_ref}` has {cnt} total rows.")
                    if cnt == 0:
                        diagnostics.append("The table is empty — no data to query.")
            except Exception as e:
                diagnostics.append(f"Could not count rows: {e}")
        return "\n".join(diagnostics)

    where_clause = where_match.group(1).strip()
    diagnostics.append(f"WHERE clause: `{where_clause}`")

    # 2. Try to parse individual conditions (simple AND-separated)
    conditions = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)

    from_match = re.search(r'FROM\s+`?([^\s`]+)`?', sql, re.IGNORECASE)
    if not from_match:
        diagnostics.append("Could not determine source table.")
        return "\n".join(diagnostics)

    table_ref = from_match.group(1)

    # 3. Test each condition independently
    diagnostics.append(f"\nFilter analysis (testing each condition against `{table_ref}`):")
    for i, cond in enumerate(conditions):
        cond = cond.strip().rstrip(')')
        # Remove leading parens
        while cond.startswith('('):
            cond = cond[1:]
        if not cond:
            continue
        try:
            test_sql = f"SELECT COUNT(*) as cnt FROM `{table_ref}` WHERE {cond}"
            result = list(client.query(test_sql).result())
            if result:
                cnt = result[0].cnt
                if cnt == 0:
                    diagnostics.append(f"  ❌ Filter #{i+1} `{cond}` → 0 matching rows (THIS is likely the problem)")
                    # Try to suggest alternatives
                    _suggest_filter_fix(client, table_ref, cond, diagnostics)
                else:
                    diagnostics.append(f"  ✅ Filter #{i+1} `{cond}` → {cnt} matching rows")
        except Exception as e:
            diagnostics.append(f"  ⚠️ Filter #{i+1} `{cond}` → error: {e}")

    # 4. Overall recommendation
    diagnostics.append("\n**Suggestion:** Remove or relax the failing filter(s) and retry.")

    return "\n".join(diagnostics)


def _suggest_filter_fix(client: bigquery.Client, table_ref: str, condition: str, diagnostics: list):
    """Try to suggest a fix for a failing filter condition."""
    # Extract column and value from simple conditions
    eq_match = re.match(r'`?(\w+)`?\s*=\s*[\'"]?([^\'"]+)[\'"]?', condition)
    like_match = re.match(r'`?(\w+)`?\s+LIKE\s+[\'"]%?([^\'"%]+)%?[\'"]', condition, re.IGNORECASE)

    col = None
    value = None

    if eq_match:
        col = eq_match.group(1)
        value = eq_match.group(2)
    elif like_match:
        col = like_match.group(1)
        value = like_match.group(2)

    if col:
        try:
            # Show actual values in the column
            distinct_sql = f"SELECT DISTINCT `{col}` FROM `{table_ref}` WHERE `{col}` IS NOT NULL LIMIT 10"
            distinct_rows = list(client.query(distinct_sql).result())
            actual_values = [str(getattr(r, col, "")) for r in distinct_rows]
            if actual_values:
                diagnostics.append(f"    → Actual values in `{col}`: {actual_values}")
                if value:
                    # Check for case mismatch
                    lower_values = [v.lower() for v in actual_values]
                    if value.lower() in lower_values and value not in actual_values:
                        idx = lower_values.index(value.lower())
                        diagnostics.append(f"    → ⚡ Case mismatch! Try: `{col}` = '{actual_values[idx]}'")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Error diagnosis
# ---------------------------------------------------------------------------

_ERROR_PATTERNS = [
    (r"Not found: Table (\S+)", "table_not_found",
     "The table `{0}` does not exist. Check the dataset and table name."),
    (r"Not found: Dataset (\S+)", "dataset_not_found",
     "The dataset `{0}` does not exist."),
    (r"Unrecognized name: (\S+)", "column_not_found",
     "Column `{0}` not found. Run `profile_dataset` to see actual column names."),
    (r"Name (\S+) not found inside (\S+)", "column_not_found_in_table",
     "Column `{0}` not found in `{1}`. Check the schema."),
    (r"Syntax error", "syntax_error",
     "SQL syntax error. Check for missing commas, mismatched parentheses, or unsupported functions."),
    (r"Cannot query over table .+ without a filter over column\(s\) '([^']+)'", "partition_filter_required",
     "This table requires a partition filter on column `{0}`. Add a WHERE clause for it."),
    (r"Resources exceeded", "resources_exceeded",
     "Query is too large. Add more specific filters, use LIMIT, or partition the query."),
    (r"Access Denied", "access_denied",
     "You don't have permission to access this resource."),
    (r"Ambiguous column name (\S+)", "ambiguous_column",
     "Column `{0}` exists in multiple tables — qualify it with a table alias."),
    (r"No matching signature for function (\S+)", "function_error",
     "Function `{0}` was called with wrong argument types. Check the BigQuery function reference."),
]


def diagnose_error(sql: str, error_msg: str) -> str:
    """
    Parse a BigQuery error message and return a human-friendly diagnosis
    with actionable suggestions.
    """
    diagnostics = [f"**Error:** {error_msg}\n"]

    # Try to match known patterns
    matched = False
    for pattern, error_type, template in _ERROR_PATTERNS:
        m = re.search(pattern, error_msg, re.IGNORECASE)
        if m:
            matched = True
            suggestion = template.format(*m.groups())
            diagnostics.append(f"**Diagnosis ({error_type}):** {suggestion}")
            break

    if not matched:
        diagnostics.append("**Diagnosis:** Could not auto-classify this error. Please review the error message above.")

    # Add the problematic SQL for reference
    diagnostics.append(f"\n**SQL that failed:**\n```sql\n{sql}\n```")

    return "\n".join(diagnostics)


# ---------------------------------------------------------------------------
# Column probing
# ---------------------------------------------------------------------------

def probe_column_values(
    client: bigquery.Client,
    table_ref: str,
    column_name: str,
    limit: int = 20,
) -> str:
    """
    Run an exploratory query to inspect actual values in a column.
    Uses APPROX_TOP_COUNT for efficiency on large tables.

    Returns a formatted text summary.
    """
    safe_col = f"`{column_name}`"

    lines = [f"## Column Profile: `{column_name}` in `{table_ref}`\n"]

    # Count distinct + NULL ratio
    try:
        stats_sql = (
            f"SELECT "
            f"COUNT(*) as total, "
            f"COUNTIF({safe_col} IS NULL) as nulls, "
            f"APPROX_COUNT_DISTINCT({safe_col}) as distinct_vals "
            f"FROM `{table_ref}`"
        )
        result = list(client.query(stats_sql).result())
        if result:
            r = result[0]
            null_pct = round(r.nulls / max(r.total, 1) * 100, 1)
            lines.append(f"- Total rows: {r.total:,}")
            lines.append(f"- NULL values: {r.nulls:,} ({null_pct}%)")
            lines.append(f"- Approximate distinct values: {r.distinct_vals:,}")
    except Exception as e:
        lines.append(f"- Stats query failed: {e}")

    # Top values
    try:
        top_sql = (
            f"SELECT value, count FROM "
            f"UNNEST((SELECT APPROX_TOP_COUNT({safe_col}, {limit}) FROM `{table_ref}`)) "
        )
        top_rows = list(client.query(top_sql).result())
        if top_rows:
            lines.append(f"\n**Top {len(top_rows)} values:**")
            for r in top_rows:
                lines.append(f"  - `{r.value}` ({r.count:,} occurrences)")
    except Exception as e:
        # Fallback to simple DISTINCT
        try:
            fallback_sql = f"SELECT {safe_col}, COUNT(*) as cnt FROM `{table_ref}` WHERE {safe_col} IS NOT NULL GROUP BY 1 ORDER BY cnt DESC LIMIT {limit}"
            fb_rows = list(client.query(fallback_sql).result())
            if fb_rows:
                lines.append(f"\n**Top {len(fb_rows)} values:**")
                for r in fb_rows:
                    val = getattr(r, column_name, "?")
                    lines.append(f"  - `{val}` ({r.cnt:,} occurrences)")
        except Exception:
            lines.append(f"- Could not retrieve top values: {e}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Query confidence scoring
# ---------------------------------------------------------------------------

def estimate_query_confidence(
    semantic_context: str,
    nl_query: str,
    similar_queries: list[dict],
    tables_known: bool,
    columns_known: bool,
) -> dict:
    """
    Estimate a confidence score (0.0–1.0) for how likely a generated
    query is to succeed, based on available semantic knowledge.

    Returns {
        "score": float,
        "level": "HIGH" | "MEDIUM" | "LOW",
        "factors": [str],  # what boosted/lowered confidence
    }
    """
    score = 0.5  # Base score
    factors = []

    # Boost: semantic context is available
    if semantic_context and "No semantic knowledge" not in semantic_context:
        score += 0.15
        factors.append("+15%: Semantic catalog available for this dataset")
    else:
        score -= 0.1
        factors.append("-10%: No semantic catalog — consider running profile_dataset first")

    # Boost: similar past queries exist
    if similar_queries:
        best_score = max(q.get("relevance_score", 0) for q in similar_queries)
        if best_score >= 3:
            score += 0.2
            factors.append(f"+20%: Found highly similar past query (score={best_score})")
        elif best_score >= 1:
            score += 0.1
            factors.append(f"+10%: Found somewhat similar past query (score={best_score})")

    # Boost: tables are known
    if tables_known:
        score += 0.1
        factors.append("+10%: All referenced tables are known in catalog")
    else:
        score -= 0.15
        factors.append("-15%: Some tables are not in the semantic catalog")

    # Boost: columns are known
    if columns_known:
        score += 0.1
        factors.append("+10%: All referenced columns are known in catalog")
    else:
        score -= 0.1
        factors.append("-10%: Some columns are not in the semantic catalog")

    # Clamp to [0, 1]
    score = max(0.0, min(1.0, score))

    if score >= 0.8:
        level = "HIGH"
    elif score >= 0.5:
        level = "MEDIUM"
    else:
        level = "LOW"

    return {
        "score": round(score, 2),
        "level": level,
        "factors": factors,
    }
