"""
LLM-based dataset description and intelligent join inference.

Uses Gemini to generate:
- Natural language table descriptions
- Intelligent join discovery (Level 2 — beyond name-matching heuristics)
- Relationship maps explaining how tables relate
- Dataset summaries
"""

import os
import json
from google import genai

_MODEL = os.environ.get("LLM_DESCRIBER_MODEL", "gemini-3-flash-preview")
_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "antoine-exp")
_LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION", "global")


def _get_client() -> genai.Client:
    """Return a Gemini client configured for Vertex AI."""
    return genai.Client(
        vertexai=True,
        project=_PROJECT,
        location=_LOCATION,
    )


def _call_llm(prompt: str, max_tokens: int = 4096) -> str:
    """Call Gemini and return the text response."""
    client = _get_client()
    response = client.models.generate_content(
        model=_MODEL,
        contents=prompt,
        config=genai.types.GenerateContentConfig(
            max_output_tokens=max_tokens,
            temperature=0.3,  # Low temp for factual descriptions
        ),
    )
    return response.text or ""


def _format_profile_for_prompt(profile: dict, join_info: list[dict] | None = None) -> str:
    """Format a table profile into a compact text block for the LLM prompt."""
    lines = []
    table = profile.get("table_name", "unknown")
    rows = profile.get("total_rows", 0)
    size_gb = profile.get("total_bytes", 0) / 1e9
    schema_only = profile.get("schema_only", False)

    lines.append(f"Table: {profile.get('dataset', '')}.{table} ({rows:,} rows, {size_gb:.1f} GB)")
    if schema_only:
        lines.append("  [Schema-only profile — no data statistics available]")

    for col in profile.get("columns", []):
        parts = [f"{col['column_name']} ({col.get('data_type', '?')})"]
        if col.get("semantic_type") and col["semantic_type"] != "unknown":
            parts.append(f"type={col['semantic_type']}")
        if col.get("distinct_count"):
            parts.append(f"{col['distinct_count']:,} distinct")
        if col.get("null_pct", 0) > 10:
            parts.append(f"{col['null_pct']:.0f}% NULL")
        if col.get("is_partition_key"):
            parts.append("PARTITION KEY")
        if col.get("is_clustering_key"):
            parts.append("CLUSTERING KEY")
        sample = col.get("sample_values", "[]")
        try:
            samples = json.loads(sample) if isinstance(sample, str) else sample
            if samples:
                parts.append(f"e.g. {samples[:3]}")
        except (json.JSONDecodeError, TypeError):
            pass
        lines.append(f"  - {', '.join(parts)}")

    if join_info:
        relevant = [j for j in join_info if j.get("table_a") == table or j.get("table_b") == table]
        if relevant:
            lines.append("  Known joins:")
            for j in relevant:
                lines.append(f"    - {j['table_a']}.{j['column_a']} ↔ {j['table_b']}.{j['column_b']} "
                             f"(by {j.get('discovered_by', '?')}, confidence={j.get('confidence', '?')})")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_table_descriptions(
    profiles: list[dict],
    join_info: list[dict] | None = None,
    batch_size: int = 5,
) -> list[dict]:
    """
    Generate natural language descriptions for tables using LLM.

    Args:
        profiles: List of profile dicts from profile_table().
        join_info: List of join pattern dicts from discover_joins().
        batch_size: How many small tables to batch into one LLM call.

    Returns:
        List of {"table": name, "description": str, "concepts": list, "use_cases": list}
    """
    results = []

    # Batch small tables, process large ones individually
    batch = []
    for p in profiles:
        batch.append(p)
        if len(batch) >= batch_size:
            results.extend(_describe_batch(batch, join_info))
            batch = []
    if batch:
        results.extend(_describe_batch(batch, join_info))

    return results


def _describe_batch(profiles: list[dict], join_info: list[dict] | None) -> list[dict]:
    """Describe a batch of tables in a single LLM call."""
    formatted = "\n\n".join(_format_profile_for_prompt(p, join_info) for p in profiles)

    prompt = (
        "You are a data analyst describing BigQuery tables for a semantic knowledge graph.\n\n"
        "For EACH table below, provide:\n"
        "1. **Description**: A 1-2 sentence description of what the table contains and its purpose.\n"
        "2. **Concepts**: 3-5 key business concepts represented (as a comma-separated list).\n"
        "3. **Use Cases**: 2-3 suggested analytics use cases (as a comma-separated list).\n\n"
        "Format your response as one block per table:\n"
        "TABLE: <table_name>\n"
        "DESCRIPTION: <description>\n"
        "CONCEPTS: <concept1>, <concept2>, ...\n"
        "USE_CASES: <use_case1>, <use_case2>, ...\n\n"
        f"Tables to describe:\n\n{formatted}"
    )

    try:
        response = _call_llm(prompt)
        return _parse_descriptions(response, profiles)
    except Exception as e:
        print(f"Warning: LLM table description failed: {e}")
        return [{"table": p.get("table_name", "?"), "description": "No description generated.",
                 "concepts": [], "use_cases": []} for p in profiles]


def _parse_descriptions(response: str, profiles: list[dict]) -> list[dict]:
    """Parse the structured LLM response into description dicts."""
    results = []
    blocks = response.split("TABLE:")

    table_names = {p.get("table_name", ""): p for p in profiles}

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        lines = block.split("\n")
        table_name = lines[0].strip()
        desc = ""
        concepts = []
        use_cases = []

        for line in lines[1:]:
            line = line.strip()
            if line.startswith("DESCRIPTION:"):
                desc = line[len("DESCRIPTION:"):].strip()
            elif line.startswith("CONCEPTS:"):
                concepts = [c.strip() for c in line[len("CONCEPTS:"):].split(",") if c.strip()]
            elif line.startswith("USE_CASES:"):
                use_cases = [u.strip() for u in line[len("USE_CASES:"):].split(",") if u.strip()]

        # Match to known table name (fuzzy — the LLM might include dataset prefix)
        matched_name = table_name
        for name in table_names:
            if name in table_name or table_name in name:
                matched_name = name
                break

        results.append({
            "table": matched_name,
            "description": desc or "No description generated.",
            "concepts": concepts,
            "use_cases": use_cases,
        })

    # Ensure all tables have entries, even if parsing missed some
    seen = {r["table"] for r in results}
    for p in profiles:
        name = p.get("table_name", "")
        if name and name not in seen:
            results.append({"table": name, "description": "No description generated.",
                           "concepts": [], "use_cases": []})

    return results


def infer_joins(
    profiles: list[dict],
    heuristic_joins: list[dict],
) -> list[dict]:
    """
    Level 2: Use LLM to discover join relationships that name-matching missed.

    The LLM reasons about column naming patterns, semantic types, and table context
    to find relationships like owner_user_id ↔ users.id.

    Args:
        profiles: List of profile dicts.
        heuristic_joins: Level 1 joins from discover_joins() (same-name matching).

    Returns:
        List of new join dicts to be persisted with discovered_by="llm_inference".
    """
    # Build compact column listing
    table_cols = []
    for p in profiles:
        table = p.get("table_name", "?")
        cols = []
        for c in p.get("columns", []):
            sem = f", {c['semantic_type']}" if c.get("semantic_type", "unknown") != "unknown" else ""
            cols.append(f"{c['column_name']} ({c.get('data_type', '?')}{sem})")
        table_cols.append(f"Table: {table}\n  Columns: {', '.join(cols)}")

    tables_text = "\n\n".join(table_cols)

    # Format existing heuristic joins
    if heuristic_joins:
        heuristic_text = "\n".join(
            f"  - {j['table_a']}.{j['column_a']} ↔ {j['table_b']}.{j['column_b']} (name match, confidence={j.get('confidence', 0.7)})"
            for j in heuristic_joins
        )
    else:
        heuristic_text = "  (none detected)"

    prompt = (
        "You are a data engineer analyzing BigQuery tables to discover join relationships.\n"
        "BigQuery has NO foreign key constraints, so you must infer joins from column patterns.\n\n"
        "I've already detected some joins by matching columns with the same name (listed below).\n"
        "Please:\n"
        "1. CONFIRM or REJECT each heuristic join (is it a real relationship?)\n"
        "2. DISCOVER additional joins that name-matching missed, based on:\n"
        "   - Column naming patterns (e.g., owner_user_id likely references users.id)\n"
        "   - Semantic meaning (an 'answers' table likely references a 'questions' table)\n"
        "   - Data types (two INT64 identifier columns in related tables)\n\n"
        "For each join, provide EXACTLY this format (one per line):\n"
        "JOIN: <table_a>.<column_a> ↔ <table_b>.<column_b> | confidence=<0.0-1.0> | <explanation>\n"
        "REJECT: <table_a>.<column_a> ↔ <table_b>.<column_b> | <reason>\n\n"
        f"Tables:\n{tables_text}\n\n"
        f"Heuristic joins to review:\n{heuristic_text}"
    )

    try:
        response = _call_llm(prompt, max_tokens=2048)
        return _parse_joins(response, heuristic_joins)
    except Exception as e:
        print(f"Warning: LLM join inference failed: {e}")
        return []


def _parse_joins(response: str, heuristic_joins: list[dict]) -> list[dict]:
    """Parse LLM join inference response into join dicts."""
    new_joins = []
    existing_keys = {
        (j["table_a"], j["column_a"], j["table_b"], j["column_b"])
        for j in heuristic_joins
    }
    # Also add reverse keys
    existing_keys |= {
        (j["table_b"], j["column_b"], j["table_a"], j["column_a"])
        for j in heuristic_joins
    }

    for line in response.split("\n"):
        line = line.strip()
        if not line.startswith("JOIN:"):
            continue

        try:
            rest = line[len("JOIN:"):].strip()
            parts = rest.split("|")
            if len(parts) < 2:
                continue

            join_spec = parts[0].strip()
            confidence_part = parts[1].strip()
            explanation = parts[2].strip() if len(parts) > 2 else ""

            # Parse "table_a.column_a ↔ table_b.column_b"
            sides = join_spec.split("↔")
            if len(sides) != 2:
                continue
            left = sides[0].strip().split(".")
            right = sides[1].strip().split(".")
            if len(left) != 2 or len(right) != 2:
                continue

            table_a, col_a = left
            table_b, col_b = right

            # Parse confidence
            conf = 0.8
            if "=" in confidence_part:
                try:
                    conf = float(confidence_part.split("=")[1].strip())
                except ValueError:
                    conf = 0.8

            # Skip if already known
            key = (table_a.strip(), col_a.strip(), table_b.strip(), col_b.strip())
            if key in existing_keys:
                continue

            new_joins.append({
                "table_a": table_a.strip(),
                "column_a": col_a.strip(),
                "table_b": table_b.strip(),
                "column_b": col_b.strip(),
                "join_type": "llm_inferred",
                "confidence": conf,
                "discovered_by": "llm_inference",
                "explanation": explanation,
            })
            existing_keys.add(key)

        except Exception:
            continue

    return new_joins


def generate_relationship_map(
    table_descriptions: list[dict],
    all_joins: list[dict],
) -> str:
    """
    Generate a natural language relationship map explaining how tables relate.

    Args:
        table_descriptions: From generate_table_descriptions().
        all_joins: All joins (heuristic + LLM-inferred + human feedback).

    Returns:
        A paragraph explaining the data model and relationships.
    """
    # Format table summaries
    tables_text = "\n".join(
        f"- {d['table']}: {d['description']}"
        for d in table_descriptions
    )

    # Format joins
    if all_joins:
        joins_text = "\n".join(
            f"- {j.get('table_a', '?')}.{j.get('column_a', '?')} ↔ "
            f"{j.get('table_b', '?')}.{j.get('column_b', '?')} "
            f"(by {j.get('discovered_by', '?')}"
            f"{', ' + j.get('explanation', '') if j.get('explanation') else ''})"
            for j in all_joins
        )
    else:
        joins_text = "(no joins discovered)"

    prompt = (
        "You are a data analyst writing documentation for a BigQuery knowledge graph.\n\n"
        "Given these tables and their join relationships, write a clear, concise explanation of:\n"
        "1. How the tables relate to each other (the data model)\n"
        "2. The recommended join paths for common analyses\n"
        "3. Any important caveats (e.g., tables that don't join directly)\n\n"
        "Write 2-4 paragraphs in plain language that a business analyst would understand.\n"
        "Use specific column names when describing joins.\n\n"
        f"Tables:\n{tables_text}\n\n"
        f"Join relationships:\n{joins_text}"
    )

    try:
        return _call_llm(prompt, max_tokens=2048)
    except Exception as e:
        print(f"Warning: LLM relationship map generation failed: {e}")
        return "Relationship map generation failed. Join patterns are available in the semantic catalog."


def generate_dataset_summary(
    table_descriptions: list[dict],
    relationship_map: str,
) -> str:
    """
    Generate a one-paragraph dataset overview.

    Args:
        table_descriptions: From generate_table_descriptions().
        relationship_map: From generate_relationship_map().

    Returns:
        A brief dataset summary paragraph.
    """
    tables_text = "\n".join(
        f"- {d['table']}: {d['description']}"
        for d in table_descriptions
    )

    prompt = (
        "Write a single concise paragraph (3-5 sentences) summarizing this BigQuery dataset.\n"
        "Include: what domain it covers, key entities, and typical analytics use cases.\n\n"
        f"Tables:\n{tables_text}\n\n"
        f"Relationships:\n{relationship_map[:500]}"
    )

    try:
        return _call_llm(prompt, max_tokens=512)
    except Exception as e:
        print(f"Warning: LLM dataset summary failed: {e}")
        return "Dataset summary generation failed."


def regenerate_table_description(
    profile: dict,
    join_info: list[dict] | None = None,
) -> dict:
    """Re-generate description for a single table (after feedback)."""
    results = generate_table_descriptions([profile], join_info, batch_size=1)
    return results[0] if results else {
        "table": profile.get("table_name", "?"),
        "description": "Re-generation failed.",
        "concepts": [],
        "use_cases": [],
    }


def regenerate_relationship_map(
    table_descriptions: list[dict],
    all_joins: list[dict],
) -> str:
    """Re-generate the relationship map (after join feedback)."""
    return generate_relationship_map(table_descriptions, all_joins)
