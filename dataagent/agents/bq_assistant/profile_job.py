#!/usr/bin/env python3
"""
Cloud Run Job entrypoint for background dataset profiling.

Runs two phases:
  Phase 1 — Structural profiling (BQ SQL, no LLM)
  Phase 2 — Semantic analysis (LLM-based descriptions, join inference, relationship map)

Usage:
  python profile_job.py --dataset bigquery-public-data.stackoverflow --workspace user_workspace_xxx

Environment variables (inherited from Cloud Run Job config):
  GOOGLE_CLOUD_PROJECT, DATA_PROJECT, GOOGLE_GENAI_USE_VERTEXAI, GOOGLE_CLOUD_LOCATION
"""

import argparse
import logging
import os
import sys
import traceback

# Add parent dirs to path so we can import shared modules
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from google.cloud import bigquery

import semantic_catalog
import llm_describer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("profile_job")


def _get_bq_client() -> bigquery.Client:
    """Get a BQ client using the DATA_PROJECT for cross-project reads."""
    data_project = (
        os.environ.get("DATA_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or "octo-aif-sandbox"
    )
    return bigquery.Client(project=data_project)


def _update_status(client, workspace, dataset, status, phase="", details="", job_name=""):
    """Update profiling status, logging on failure but not crashing."""
    try:
        semantic_catalog.set_profiling_status(
            client, workspace, dataset, status, phase=phase, details=details, job_name=job_name,
        )
        logger.info(f"Status: {status} (phase={phase})")
    except Exception as e:
        logger.error(f"Failed to update profiling status: {e}")


def run_phase1_structural(client, dataset_name, workspace, dataset_id, project_id):
    """
    Phase 1: Structural profiling — BQ SQL, no LLM.
    
    - Profile each table (with size-based schema-only thresholds)
    - Save to semantic catalog
    - Discover heuristic joins (Level 1: same-name columns)
    """
    _update_status(client, workspace, dataset_name, "running", phase="structural")

    # Ensure catalog tables exist
    semantic_catalog.ensure_all_tables(client, workspace)

    # List all tables
    dataset_ref = client.dataset(dataset_id, project=project_id)
    tables = list(client.list_tables(dataset_ref))
    logger.info(f"Phase 1: Found {len(tables)} tables in {dataset_name}")

    profiles = []
    total_columns = 0

    for table in tables:
        try:
            logger.info(f"  Profiling {table.table_id}...")
            profile = semantic_catalog.profile_table(
                client, dataset_name, table.table_id, workspace,
            )
            if "error" in profile:
                logger.warning(f"  Skipped {table.table_id}: {profile['error']}")
                continue

            num_saved = semantic_catalog.save_profile_to_catalog(client, workspace, profile)
            total_columns += num_saved
            profiles.append(profile)

            mode = "schema-only" if profile.get("schema_only") else "full"
            logger.info(f"  ✓ {table.table_id}: {profile['total_rows']:,} rows, "
                        f"{len(profile['columns'])} cols ({mode})")
        except Exception as e:
            logger.error(f"  ✗ {table.table_id}: {e}")
            continue

    # Discover heuristic joins (Level 1)
    table_names = [t.table_id for t in tables]
    heuristic_joins = []
    try:
        heuristic_joins = semantic_catalog.discover_joins(
            client, workspace, dataset_name, table_names,
        )
        logger.info(f"Phase 1: Discovered {len(heuristic_joins)} heuristic joins")
    except Exception as e:
        logger.error(f"Phase 1: Join discovery failed: {e}")

    logger.info(f"Phase 1 complete: {len(profiles)} tables, {total_columns} columns")
    return profiles, heuristic_joins


def run_phase2_semantic(client, dataset_name, workspace, profiles, heuristic_joins):
    """
    Phase 2: Semantic analysis — LLM-based.
    
    Phase 2a: Generate table descriptions
    Phase 2b: Intelligent join inference + relationship map
    Phase 2c: Dataset summary
    """
    _update_status(client, workspace, dataset_name, "running", phase="semantic")

    # --- Phase 2a: Table descriptions ---
    logger.info("Phase 2a: Generating table descriptions...")
    table_descriptions = []
    try:
        table_descriptions = llm_describer.generate_table_descriptions(profiles, heuristic_joins)
        for desc in table_descriptions:
            analysis_text = (
                f"[TABLE_DESCRIPTION] {desc['table']}\n"
                f"{desc['description']}\n"
                f"Concepts: {', '.join(desc.get('concepts', []))}\n"
                f"Use cases: {', '.join(desc.get('use_cases', []))}"
            )
            _save_analysis(client, workspace, dataset_name, analysis_text)
        logger.info(f"Phase 2a: Generated {len(table_descriptions)} table descriptions")
    except Exception as e:
        logger.error(f"Phase 2a failed: {e}\n{traceback.format_exc()}")

    # --- Phase 2b: Intelligent join inference ---
    logger.info("Phase 2b: Running LLM join inference...")
    all_joins = list(heuristic_joins)
    try:
        llm_joins = llm_describer.infer_joins(profiles, heuristic_joins)
        logger.info(f"Phase 2b: LLM discovered {len(llm_joins)} new joins")

        # Persist new LLM-inferred joins
        if llm_joins:
            join_table_id = semantic_catalog._ensure_table(client, workspace, "join_patterns")
            now = semantic_catalog._now_ts()
            for j in llm_joins:
                row = {
                    "dataset_a": dataset_name,
                    "table_a": j["table_a"],
                    "column_a": j["column_a"],
                    "dataset_b": dataset_name,
                    "table_b": j["table_b"],
                    "column_b": j["column_b"],
                    "join_type": j.get("join_type", "llm_inferred"),
                    "confidence": j.get("confidence", 0.8),
                    "discovered_by": "llm_inference",
                    "usage_count": 0,
                    "created_at": now,
                }
                try:
                    client.insert_rows_json(join_table_id, [row])
                except Exception as e:
                    logger.warning(f"  Failed to save join {j['table_a']}.{j['column_a']} ↔ "
                                   f"{j['table_b']}.{j['column_b']}: {e}")
            all_joins.extend(llm_joins)

    except Exception as e:
        logger.error(f"Phase 2b join inference failed: {e}\n{traceback.format_exc()}")

    # --- Phase 2b continued: Relationship map ---
    logger.info("Phase 2b: Generating relationship map...")
    relationship_map = ""
    try:
        relationship_map = llm_describer.generate_relationship_map(table_descriptions, all_joins)
        _save_analysis(client, workspace, dataset_name, f"[RELATIONSHIPS]\n{relationship_map}")
        logger.info("Phase 2b: Relationship map generated")
    except Exception as e:
        logger.error(f"Phase 2b relationship map failed: {e}\n{traceback.format_exc()}")

    # --- Phase 2c: Dataset summary ---
    logger.info("Phase 2c: Generating dataset summary...")
    try:
        summary = llm_describer.generate_dataset_summary(table_descriptions, relationship_map)
        _save_analysis(client, workspace, dataset_name, f"[SUMMARY]\n{summary}")
        logger.info("Phase 2c: Dataset summary generated")
    except Exception as e:
        logger.error(f"Phase 2c failed: {e}\n{traceback.format_exc()}")

    logger.info("Phase 2 complete")


def _save_analysis(client, workspace, dataset_name, analysis_text):
    """Save an analysis entry using the existing dataset_analysis table in bq_tools."""
    try:
        project_id = client.project
        table_id = f"{project_id}.{workspace}.dataset_analysis"

        # Ensure table exists
        schema = [
            bigquery.SchemaField("dataset_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("analysis", "STRING", mode="REQUIRED"),
        ]
        try:
            client.get_table(table_id)
        except Exception:
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)

        rows = [{"dataset_name": dataset_name, "analysis": analysis_text}]
        client.insert_rows_json(table_id, rows)
    except Exception as e:
        logger.error(f"Failed to save analysis: {e}")


def main():
    parser = argparse.ArgumentParser(description="Background dataset profiler")
    parser.add_argument("--dataset", required=True, help="Dataset name (e.g., bigquery-public-data.stackoverflow)")
    parser.add_argument("--workspace", required=True, help="Workspace dataset (e.g., user_workspace_xxx)")
    args = parser.parse_args()

    dataset_name = args.dataset
    workspace = args.workspace

    # Parse project and dataset ID
    if "." in dataset_name:
        project_id, dataset_id = dataset_name.split(".", 1)
    else:
        project_id = os.environ.get("DATA_PROJECT", os.environ.get("GOOGLE_CLOUD_PROJECT", ""))
        dataset_id = dataset_name

    logger.info(f"Starting background profiling: {dataset_name} → workspace={workspace}")

    client = _get_bq_client()
    job_name = os.environ.get("CLOUD_RUN_EXECUTION", "local")

    try:
        # Phase 1: Structural
        profiles, heuristic_joins = run_phase1_structural(
            client, dataset_name, workspace, dataset_id, project_id,
        )

        if not profiles:
            _update_status(client, workspace, dataset_name, "failed",
                           details="No tables could be profiled")
            logger.error("No tables profiled — marking as failed")
            sys.exit(1)

        # Phase 2: Semantic (LLM)
        run_phase2_semantic(client, dataset_name, workspace, profiles, heuristic_joins)

        # Done!
        _update_status(client, workspace, dataset_name, "completed",
                       details=f"Profiled {len(profiles)} tables", job_name=job_name)
        logger.info("✅ Background profiling completed successfully")

    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"Background profiling failed: {e}\n{tb}")
        _update_status(client, workspace, dataset_name, "failed",
                       details=f"Error: {e}\n{tb[:500]}", job_name=job_name)
        sys.exit(1)


if __name__ == "__main__":
    main()
