import os
import time
import json
from google.cloud import bigquery
from google.adk.tools.tool_context import ToolContext
from google.adk.events.ui_widget import UiWidget
import semantic_catalog
import query_planner

# Cache the per-user workspace name so we only detect credentials once
_cached_user_workspace: str | None = None
_cached_user_email: str | None = None

# Module-level variable set per-request by __main__.py with the authenticated user's email
_current_user_email: str | None = None

# Whether user OAuth consent flow is enabled (requires Workspace admin approval).
# Set USER_OAUTH_ENABLED=true to enable user-delegated credentials for data APIs.
# When false (default), all data API calls use the SA (Cloud Run) or ADC (local).
_USER_OAUTH_ENABLED = os.environ.get("USER_OAUTH_ENABLED", "false").lower() == "true"

# Project for data operations (BigQuery, GCS). Separate from GOOGLE_CLOUD_PROJECT
# which is used for Vertex AI / LLM calls.
_DATA_PROJECT = os.environ.get("DATA_PROJECT", "octo-aif-sandbox")

# Module-level a2ui payload — bypasses ADK session state which doesn't
# reliably propagate from sub-agent tool_context to parent session.
_latest_a2ui_payload = None

def get_latest_a2ui_payload():
    """Called by __main__.py to retrieve the latest a2ui payload."""
    global _latest_a2ui_payload
    payload = _latest_a2ui_payload
    _latest_a2ui_payload = None  # Clear after read
    return payload

def set_user_email(email: str | None):
    """Called by __main__.py to set the authenticated user's email per-request."""
    global _current_user_email, _cached_user_workspace, _cached_user_email
    if email and email != _cached_user_email:
        # New user — invalidate cache so workspace is re-derived
        _cached_user_workspace = None
        _cached_user_email = email
    _current_user_email = email


def _get_bq_client() -> bigquery.Client:
    """Return a BigQuery client using the appropriate credentials.
    
    When USER_OAUTH_ENABLED: uses the user's OAuth credentials if available.
    Otherwise (default): uses SA (Cloud Run) or ADC (local dev).
    """
    if _USER_OAUTH_ENABLED and _current_user_email:
        try:
            from user_credentials import get_user_bq_client
            client = get_user_bq_client(_current_user_email)
            if client:
                return client
        except Exception:
            pass
    return bigquery.Client(project=_DATA_PROJECT)


def _get_gcs_client():
    """Return a GCS client using the appropriate credentials."""
    from google.cloud import storage
    if _USER_OAUTH_ENABLED and _current_user_email:
        try:
            from user_credentials import get_user_gcs_client
            client = get_user_gcs_client(_current_user_email)
            if client:
                return client
        except Exception:
            pass
    return storage.Client(project=_DATA_PROJECT)


def _get_sheets_service():
    """Return a Sheets API service using the appropriate credentials."""
    from googleapiclient.discovery import build
    if _USER_OAUTH_ENABLED and _current_user_email:
        try:
            from user_credentials import get_user_sheets_service
            service = get_user_sheets_service(_current_user_email)
            if service:
                return service
        except Exception:
            pass
    return build('sheets', 'v4')


def _get_user_workspace() -> str:
    """
    Derives the per-user workspace dataset name from the authenticated user's email.
    Priority: 1) _current_user_email (from OAuth), 2) ADC/gcloud detection, 3) fallback.
    """
    global _cached_user_workspace
    if _cached_user_workspace:
        return _cached_user_workspace

    email = _current_user_email  # Set by OAuth flow

    # Fallback: try ADC / gcloud config if no OAuth email
    if not email or '@' not in email:
        try:
            import google.auth
            credentials, _ = google.auth.default()
            email = getattr(credentials, 'service_account_email', None)
            if not email or email == 'default':
                if hasattr(credentials, '_service_account_email'):
                    email = credentials._service_account_email
                elif hasattr(credentials, 'signer_email'):
                    email = credentials.signer_email
            
            if not email or email == 'default' or '@' not in str(email):
                import subprocess
                result = subprocess.run(
                    ['gcloud', 'config', 'get-value', 'account'],
                    capture_output=True, text=True, timeout=5
                )
                email = result.stdout.strip()
        except Exception as e:
            print(f"WARNING: Error detecting user email: {e}")

    if email and '@' in email:
        username = email.split('@')[0]
        username = username.replace('.', '_').replace('-', '_')
        _cached_user_workspace = f"user_workspace_{username}"
        print(f"INFO: Using per-user workspace: {_cached_user_workspace} (from {email})")
    else:
        _cached_user_workspace = "user_workspace"
        print("WARNING: Could not detect user email. Using default workspace.")
    
    return _cached_user_workspace

def _get_workspace_for_dataset(client: bigquery.Client, dataset_name: str, base_workspace: str = None) -> str:
    """
    Looks up the location of the provided dataset and returns a region-suffixed 
    workspace dataset (e.g., user_workspace_larmanjat_us_central1) if the location is not 'US'.
    It also ensures the localized workspace exists.
    """
    if base_workspace is None:
        base_workspace = _get_user_workspace()
    try:
        # Resolve destination dataset if full id given
        target = dataset_name
        if "." not in target:
             target = f"{client.project}.{target}"
             
        ds = client.get_dataset(target)
        loc = ds.location
        if not loc or loc.upper() == "US":
             return base_workspace
             
        suffix = loc.lower().replace("-", "_")
        regional_ws = f"{base_workspace}_{suffix}"
        
        # Ensure regional workspace exists
        try:
             client.get_dataset(f"{client.project}.{regional_ws}")
        except Exception:
             # Create it
             new_ds = bigquery.Dataset(f"{client.project}.{regional_ws}")
             new_ds.location = loc
             client.create_dataset(new_ds, exists_ok=True)
             
        return regional_ws
    except Exception as e:
        print(f"Warning: could not lookup location for dataset {dataset_name}: {e}. Defaulting to {base_workspace}.")
        return base_workspace

def scan_datasets(limit: int = 10) -> str:
    """
    Scans and lists BigQuery datasets the user has access to, limited to top projects.

    Args:
        limit: Max number of projects to scan. Defaults to 10.

    Returns:
        A formatted string listing projects and their datasets.
    """
    try:
        client = _get_bq_client()
        output = []
        current_project = client.project
        projects = [current_project]
        
        # Add other projects from iterator until limit is reached
        for p in client.list_projects(max_results=limit):
            if p.project_id != current_project:
                projects.append(p.project_id)
            if len(projects) >= limit:
                break
                
        if not projects:
            return "No projects found."
            
        for project_id in projects:
            output.append(f"Project: {project_id}")
            with open("/tmp/bq_scan.log", "a") as f:
                 f.write(f"Scanning project: {project_id}\n")
            try:
                datasets = list(client.list_datasets(project=project_id, max_results=limit))
                if datasets:
                    for d in datasets:
                        output.append(f"  - Dataset: {d.dataset_id}")
                else:
                    output.append("  - No datasets found.")
            except Exception as e:
                output.append(f"  - Error listing datasets: {e}")
                
        return "\n".join(output)
    except Exception as e:
        return f"Error scanning datasets: {e}"

def save_selected_datasets(selected_datasets: list[str], dataset_name: str = None) -> str:
    """
    Creates a dataset is the current GCP project to store the list of datasets the user wants to work on.

    Args:
        selected_datasets: A list of dataset names (e.g., ["project_id.dataset_id", ...]) or just dataset IDs.
        dataset_name: The name of the dataset to create to store the list. Defaults to 'user_workspace'.

    Returns:
        A success or failure message.
    """
    if dataset_name is None: dataset_name = _get_user_workspace()
    try:
        client = _get_bq_client()
        project_id = client.project
        
        # 1. Create the dataset if not exists
        dataset_id = f"{project_id}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.description = "Stores user selected workspaces"
        
        try:
            client.get_dataset(dataset_id)
            output_msg = f"Dataset '{dataset_name}' already exists.\n"
        except Exception:
            # Not found, create it
            client.create_dataset(dataset)
            output_msg = f"Created dataset '{dataset_name}'.\n"
            
        # 2. Create the table to store the list
        table_id = f"{dataset_id}.selected_datasets"
        schema = [
            bigquery.SchemaField("dataset_name", "STRING", mode="REQUIRED")
        ]
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            client.get_table(table_id)
            output_msg += "Table 'selected_datasets' already exists.\n"
        except Exception:
            client.create_table(table)
            output_msg += "Created table 'selected_datasets'.\n"
            
        # 3. Insert the selected datasets
        rows_to_insert = [{"dataset_name": d} for d in selected_datasets]
        errors = client.insert_rows_json(table_id, rows_to_insert)
        
        if not errors:
            output_msg += f"Successfully saved {len(selected_datasets)} selections."
        else:
            output_msg += f"Errors inserting rows: {errors}"
            
        return output_msg
        
    except Exception as e:
        return f"Error saving selected datasets: {e}"

def load_selected_datasets(dataset_name: str = None) -> list[str]:
    """
    Loads the list of previously selected datasets from the workspace storage.

    Args:
        dataset_name: The dataset name. Defaults to 'user_workspace'.

    Returns:
        A list of string dataset names, or an empty list if none found.
    """
    if dataset_name is None: dataset_name = _get_user_workspace()
    try:
        client = _get_bq_client()
        project_id = client.project
        table_id = f"{project_id}.{dataset_name}.selected_datasets"
        
        try:
            rows = client.list_rows(table_id)
            return [row["dataset_name"] for row in rows]
        except Exception:
            # Table or dataset doesn't exist yet
            return []
    except Exception:
        return []

def remove_selected_datasets(datasets_to_remove: list[str], dataset_name: str = None) -> str:
    """
    Removes the specified datasets from the workspace storage list.

    Args:
        datasets_to_remove: A list of dataset names to remove (e.g., ["project_id.dataset_id", ...]).
        dataset_name: The dataset name.

    Returns:
        A success or failure message.
    """
    if dataset_name is None: dataset_name = _get_user_workspace()
    try:
        client = _get_bq_client()
        project_id = client.project
        table_id = f"{project_id}.{dataset_name}.selected_datasets"
        
        if not datasets_to_remove:
            return "No datasets provided to remove."
            
        datasets_str = ", ".join([f"'{d}'" for d in datasets_to_remove])
        query = f"DELETE FROM `{table_id}` WHERE dataset_name IN ({datasets_str})"
        
        query_job = client.query(query)
        query_job.result()
        
        return f"Successfully removed {len(datasets_to_remove)} datasets from selection."
    except Exception as e:
        return f"Error removing selected datasets: {e}"

def analyze_dataset(dataset_name: str, tool_context: ToolContext = None) -> str:
    """
    Gathers statistics and metadata for a specific BigQuery dataset and its tables.
    
    Args:
        dataset_name: The dataset ID or full 'project.dataset'.
        tool_context: Internal execution context automatically provided by the ADK runtime.
        
    Returns:
        Structured text describing table rows, sizes, and schema columns.
    """
    try:
        from google.cloud import bigquery
        client = _get_bq_client()
        if "." in dataset_name:
             project_id, dataset_id = dataset_name.split(".", 1)
        else:
             project_id = client.project
             dataset_id = dataset_name
             
        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = list(client.list_tables(dataset_ref))
        
        output = [f"Analysis of Dataset: {project_id}.{dataset_id}\n"]
        
        flat_components = []
        
        dataset_title = f"{project_id}.{dataset_id}"
        
        flat_components.append({
            "id": "dataset_card",
            "component": {
                "Card": {
                    "title": {"literal": f"Dataset: {dataset_title}"},
                    "child": "main_col"
                }
            }
        })
        
        main_col_children = []
        has_tables = False
        
        for table in tables:
             has_tables = True
             t = client.get_table(table.reference)
             output.append(f"Table: {t.table_id}")
             output.append(f"  - Rows: {t.num_rows}")
             output.append(f"  - Size: {t.num_bytes} bytes")
             
             schema_output = []
             schema_strs = []
             for field in t.schema:
                  schema_output.append(f"    * {field.name} ({field.field_type}) - {field.description or 'No description'}")
                  schema_strs.append(f"{field.name} ({field.field_type})")
             output.append("  - Schema:")
             output.extend(schema_output)
             output.append("")
             
             # UI Components
             table_card_id = f"table_{t.table_id}_card"
             text_id = f"text_{t.table_id}"
             
             schema_str = ", ".join(schema_strs)
             if len(schema_str) > 150:
                 schema_str = schema_str[:147] + "..."
                 
             text_content = f"Rows: {t.num_rows} | Size: {t.num_bytes} bytes\nSchema: {schema_str}"
             
             flat_components.append({
                 "id": text_id,
                 "component": {
                     "Text": {
                         "text": {"literalString": text_content}
                     }
                 }
             })
             
             flat_components.append({
                 "id": table_card_id,
                 "component": {
                     "Card": {
                         "title": {"literalString": t.table_id},
                         "child": text_id
                     }
                 }
             })
             
             main_col_children.append(table_card_id)
        
        if not has_tables:
            text_id = "no_tables_text"
            flat_components.append({
                 "id": text_id,
                 "component": {
                     "Text": {
                         "text": {"literal": "No tables found in this dataset."}
                     }
                 }
            })
            main_col_children.append(text_id)

        flat_components.append({
            "id": "main_col",
            "component": {
                "Column": {
                    "children": {"literalArray": [{"literalString": cid} for cid in main_col_children]}
                }
            }
        })
        
        if tool_context:
            import uuid
            unique_surface_id = f"table_{uuid.uuid4().hex[:8]}"
            a2ui_payload = [
                {
                    "surfaceUpdate": {
                        "surfaceId": unique_surface_id,
                        "components": flat_components
                    }
                },
                {
                    "beginRendering": {
                        "surfaceId": "@default",
                        "root": "dataset_card"
                    }
                }
            ]
            tool_context.state["pending_bq_a2ui"] = a2ui_payload
            global _latest_a2ui_payload
            _latest_a2ui_payload = a2ui_payload
            return "Dataset analysis displayed in UI."
            
        return "\n".join(output)
    except Exception as e:
        return f"Error analyzing dataset '{dataset_name}': {e}"

def execute_query(sql: str, query_name: str = None, natural_language_question: str = None, dataset_name: str = None, tool_context: ToolContext = None) -> str:
    """
    Executes a SQL query on BigQuery and returns the results.
    
    After execution:
    - On SUCCESS with rows: auto-records the query in the Semantic Catalog
      for future similarity retrieval (learning from success).
    - On 0 ROWS: runs a diagnostic analysis on the WHERE clause filters
      to identify which filter is too restrictive, and suggests fixes.
    - On ERROR: parses the BigQuery error message and provides actionable
      diagnosis with specific suggestions.
    
    Args:
        sql: The SQL query string to execute.
        query_name: A short, descriptive title for this query (e.g. "Top 10 Fantasy Books", "Book Count per Genre"). If not provided, a name will be derived from the SQL.
        natural_language_question: The original user question that led to this query. Used for learning.
        dataset_name: The active dataset context. Used for learning and diagnostics.
        tool_context: Internal execution context automatically provided by the ADK runtime.
        
    Returns:
        Structured text describing the query results (rows), or diagnostic info on failure.
    """
    try:
        from google.cloud import bigquery
        import html
        import re
        client = _get_bq_client()
        
        print(f"DEBUG EXECUTE_QUERY: SQL = {sql[:200]}...")
        start_time = time.time()
        query_job = client.query(sql)
        results = query_job.result()
        elapsed_ms = int((time.time() - start_time) * 1000)
        
        if not hasattr(results, 'schema'):
             return "Query executed but returned no structured schema. No data to display."
        
        # Derive a display title for the table
        if query_name:
            display_title = query_name
        else:
            # Try to auto-derive a name from the SQL
            sql_upper = sql.strip().upper()
            # Extract table name from FROM clause
            from_match = re.search(r'FROM\s+[`]?(\S+?)[`]?\s', sql, re.IGNORECASE)
            table_name = from_match.group(1).split('.')[-1] if from_match else None
            if table_name:
                display_title = f"Results from {table_name}"
            else:
                display_title = "Query Results"
             
        # 1. Transform rows into array of strings for A2UI
        headers = [f.name for f in results.schema]
        rows_list = list(results)
        
        counter = 0
        rows_data = []
        for row in rows_list:
             counter += 1
             rows_data.append([str(row[h]) for h in headers])
             if counter >= 50:
                  break
        
        if tool_context:
            import uuid
            unique_surface_id = f"table_{uuid.uuid4().hex[:8]}"
            table_component_id = f"table_query_results_{unique_surface_id}"
            
            flat_components = [{
                "id": table_component_id,
                "component": {
                    "Table": {
                        "tableTitle": {"literalString": f"{display_title} ({results.total_rows} total rows)"},
                        "headers": {"literalArray": [{"literalString": str(h)} for h in headers]},
                        "rows": {"literalArray": [{"literalArray": [{"literalString": str(c)} for c in row]} for row in rows_data]}
                    }
                }
            }]
            
            a2ui_payload = [
                {
                    "beginRendering": {
                        "surfaceId": unique_surface_id,
                        "root": table_component_id
                    }
                },
                {
                    "surfaceUpdate": {
                        "surfaceId": unique_surface_id,
                        "components": flat_components
                    }
                }
            ]
            
            try:
                tool_context.state['pending_bq_a2ui'] = a2ui_payload
                # Also store in module-level var (reliable cross-agent propagation)
                global _latest_a2ui_payload
                _latest_a2ui_payload = a2ui_payload
                # Log the table title to trace which query's result is stored
                table_title = ''
                for pld in a2ui_payload:
                    su = pld.get('surfaceUpdate', {})
                    for comp in su.get('components', []):
                        t = comp.get('component', {}).get('Table', {}).get('tableTitle', {}).get('literalString', '')
                        if t: table_title = t
                print(f"DEBUG TOOL_CONTEXT: Set pending_bq_a2ui, table='{table_title}'")
            except Exception as e:
                print(f"DEBUG TOOL_CONTEXT ERROR: Could not set state: {e}. Is tool_context None? {tool_context is None}")
                pass
        
        output = [f"**{display_title} ({results.total_rows} total rows)**\n"]
        
        if results.total_rows > 50:
             output.append("\n*Note: Result set is large. Showing only the first 50 rows in UI. Use `export_query_to_sheets` to view full results.*")
             
        if results.total_rows == 0:
             output = ["Query executed successfully but returned **0 rows**."]
             # Auto-diagnose empty results
             if dataset_name:
                 try:
                     diagnosis = query_planner.diagnose_empty_result(client, sql, dataset_name)
                     output.append(f"\n### 🔍 Empty Result Diagnosis\n{diagnosis}")
                     output.append("\n**Suggestion:** Review the failing filter(s) above. You can use `probe_column` to inspect actual values before adjusting the query.")
                 except Exception as diag_e:
                     output.append(f"\n(Could not auto-diagnose: {diag_e})")
        else:
             # Auto-record successful query for future learning
             if natural_language_question and dataset_name:
                 try:
                     workspace = _get_user_workspace()
                     regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace)
                     # Extract tables used from SQL
                     table_matches = re.findall(r'FROM\s+`?([^\s`]+)`?', sql, re.IGNORECASE)
                     table_matches += re.findall(r'JOIN\s+`?([^\s`]+)`?', sql, re.IGNORECASE)
                     tables_used = list(set(t.split('.')[-1] for t in table_matches))
                     
                     semantic_catalog.record_successful_query(
                         client, regional_ws,
                         natural_language=natural_language_question,
                         sql=sql,
                         dataset=dataset_name,
                         tables_used=tables_used,
                         result_row_count=results.total_rows,
                         execution_time_ms=elapsed_ms,
                     )
                     print(f"DEBUG: Recorded successful query in history (elapsed={elapsed_ms}ms)")
                 except Exception as rec_e:
                     print(f"DEBUG: Failed to record query history: {rec_e}")
              
             # Glossary suggestions — identify candidate business terms
             if natural_language_question and dataset_name:
                 try:
                     suggestions = semantic_catalog.suggest_glossary_entries(
                         client, regional_ws, dataset_name, natural_language_question, sql
                     )
                     if suggestions:
                         output.append(
                             f"\n### 💡 Glossary Suggestions\n"
                             f"I noticed some terms that might be worth recording in the business glossary "
                             f"for better future queries: **{', '.join(suggestions)}**\n"
                             f"Would you like to define what any of these mean? "
                             f"(I'll remember them for next time using `submit_feedback` with feedback_type='glossary')"
                         )
                 except Exception as gloss_e:
                     print(f"DEBUG: Glossary suggestion failed: {gloss_e}")
             
        # Include actual data rows so the LLM can present them faithfully.
        # Without this, the LLM hallucinates data values from training knowledge.
        output.append(f"\n| {' | '.join(str(h) for h in headers)} |")
        output.append(f"| {' | '.join('---' for _ in headers)} |")
        for row in rows_data:
            output.append(f"| {' | '.join(str(c) for c in row)} |")
        
        output.append("\n(The results are also displayed as an interactive table in the UI panel.)")
        output.append("**IMPORTANT: When presenting these results, use the EXACT values from the table above. Do NOT invent or guess any data values.**")
        
        output_str = "\n".join(output)
        return output_str
    except Exception as e:
        error_msg = str(e)
        # Auto-diagnose the error
        try:
            diagnosis = query_planner.diagnose_error(sql, error_msg)
            return f"Error executing query.\n\n{diagnosis}"
        except Exception:
            return f"Error executing query: {error_msg}"

def save_dataset_analysis(dataset_name: str, analysis: str, workspace_dataset: str = None) -> str:
    """
    Stores an analysis summary for a given dataset in the workspace.
    
    Args:
        dataset_name: The dataset ID or full 'project.dataset' being analyzed.
        analysis: The text analysis formulated by the assistant.
        workspace_dataset: Destination workspace holding selection lists.
        
    Returns:
        Success or error string.
    """
    if workspace_dataset is None: workspace_dataset = _get_user_workspace()
    try:
        from google.cloud import bigquery
        client = _get_bq_client()
        project_id = client.project
        
        regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace_dataset)
        table_id = f"{project_id}.{regional_ws}.dataset_analysis"
        
        # 1. Ensure Table Exists
        schema = [
             bigquery.SchemaField("dataset_name", "STRING", mode="REQUIRED"),
             bigquery.SchemaField("analysis", "STRING", mode="REQUIRED")
        ]
        table = bigquery.Table(table_id, schema=schema)
        try:
             client.get_table(table_id)
        except Exception:
             client.create_table(table)
             
        # 2. Insert row (Upsert is cleaner but insert is faster. Let's append!)
        rows_to_insert = [{"dataset_name": dataset_name, "analysis": analysis}]
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
             return f"Error saving analysis: {errors}"
        return f"Successfully saved analysis for '{dataset_name}'."
    except Exception as e:
        return f"Error storing analysis: {e}"

def save_query(query_name: str, sql: str, description: str, dataset_name: str, workspace_dataset: str = None, worksheet_url: str = None, gcs_bucket: str = None, chart_type: str = None) -> str:
    """
    Saves a custom SQL query referenced by the user to the workspace.
    
    Args:
        query_name: Custom mnemonic/title for the query.
        sql: The valid SELECT SQL string.
        description: Brief description about what the query fetches.
        dataset_name: Active scope context item.
        workspace_dataset: The dataset where the view/metadata is actually stored.
        worksheet_url: Optional tracking URI for Google Sheets export.
        gcs_bucket: Optional tracking URI for GCS export.
    """
    if workspace_dataset is None: workspace_dataset = _get_user_workspace()
    try:
        from google.cloud import bigquery
        client = _get_bq_client()
        project_id = client.project
        
        regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace_dataset)
        table_id = f"{project_id}.{regional_ws}.saved_queries"
        
        import re
        # Clean query name for valid BQ view ID
        cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '_', query_name).lower()
        view_name = f"view_{cleaned_name}"
        view_id = f"{project_id}.{regional_ws}.{view_name}"
        
        # 1. Create or Replace View
        try:
             client.query(f"CREATE OR REPLACE VIEW `{view_id}` AS {sql}").result()
             view_saved = True
        except Exception as e:
             # Ignore view creation error (usually cross-region issues) and just save metadata
             print(f"Warning: could not create view {view_name}: {e}")
             view_saved = False
             
        # 2. Add column to schema if strictly necessary
        schema = [
             bigquery.SchemaField("dataset_name", "STRING", mode="REQUIRED"),
             bigquery.SchemaField("query_name", "STRING", mode="REQUIRED"),
             bigquery.SchemaField("sql", "STRING", mode="REQUIRED"),
             bigquery.SchemaField("description", "STRING", mode="REQUIRED")
        ]
        table = bigquery.Table(table_id, schema=schema)
        try:
             # Dynamically add needed optional columns
             existing_table = client.get_table(table_id)
             existing_cols = [f.name for f in existing_table.schema]
             
             new_fields = []
             if "view_name" not in existing_cols: new_fields.append(bigquery.SchemaField("view_name", "STRING"))
             if "worksheet_url" not in existing_cols: new_fields.append(bigquery.SchemaField("worksheet_url", "STRING"))
             if "gcs_bucket" not in existing_cols: new_fields.append(bigquery.SchemaField("gcs_bucket", "STRING"))
             if "chart_type" not in existing_cols: new_fields.append(bigquery.SchemaField("chart_type", "STRING"))
             
             if new_fields:
                  new_schema = existing_table.schema[:] + new_fields
                  existing_table.schema = new_schema
                  client.update_table(existing_table, ["schema"])
        except Exception:
             # Create table with all columns if it doesn't exist
             table.schema = table.schema + [
                  bigquery.SchemaField("view_name", "STRING"),
                  bigquery.SchemaField("worksheet_url", "STRING"),
                  bigquery.SchemaField("gcs_bucket", "STRING"),
                  bigquery.SchemaField("chart_type", "STRING")
             ]
             client.create_table(table)
             
        # Fetch previous metadata to preserve existing fields before deleting
        try:
             query_job = client.query(f"SELECT view_name, worksheet_url, gcs_bucket, chart_type FROM `{table_id}` WHERE dataset_name = '{dataset_name}' AND query_name = '{query_name}'")
             rows = list(query_job.result())
             if rows:
                  existing_view = rows[0].get("view_name")
                  existing_sheet = rows[0].get("worksheet_url")
                  existing_gcs = rows[0].get("gcs_bucket")
                  existing_chart = rows[0].get("chart_type")
                  
                  if not view_saved and existing_view:
                       view_saved = True
                       view_name = existing_view
                  if not worksheet_url and existing_sheet:
                       worksheet_url = existing_sheet
                  if not gcs_bucket and existing_gcs:
                       gcs_bucket = existing_gcs
                  if not chart_type and existing_chart:
                       chart_type = existing_chart
        except Exception:
             pass
             
        # 3. Clear previous metadata row to prevent duplicates (Upsert)
        try:
             delete_sql = f"DELETE FROM `{table_id}` WHERE dataset_name = '{dataset_name}' AND query_name = '{query_name}'"
             client.query(delete_sql).result()
        except Exception:
             pass
             
        rows_to_insert = [{
             "dataset_name": dataset_name,
             "query_name": query_name,
             "sql": sql,
             "description": description,
             "view_name": view_name if view_saved else None,
             "worksheet_url": worksheet_url,
             "gcs_bucket": gcs_bucket,
             "chart_type": chart_type
        }]
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
             return f"Error saving query metadata: {errors}"
             
        if view_saved:
             return f"Successfully saved query and created view '{view_name}'."
        else:
             return f"Successfully saved query, but could not create a view (possibly due to region mismatch)."
    except Exception as e:
        return f"Error saving query: {e}"

def load_saved_queries(dataset_name: str, workspace_dataset: str = None) -> str:
    """
    Loads custom queries that were previously saved for a given dataset scope.
    """
    if workspace_dataset is None: workspace_dataset = _get_user_workspace()
    try:
        from google.cloud import bigquery
        client = _get_bq_client()
        project_id = client.project
        
        regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace_dataset)
        table_id = f"{project_id}.{regional_ws}.saved_queries"
        
        # Check schema first to see if view_name column exists
        existing_table = client.get_table(table_id)
        columns = [f.name for f in existing_table.schema]
        select_cols = "query_name, sql, description"
        if "view_name" in columns:
             select_cols += ", view_name"
        if "worksheet_url" in columns:
             select_cols += ", worksheet_url"
        if "gcs_bucket" in columns:
             select_cols += ", gcs_bucket"
        if "chart_type" in columns:
             select_cols += ", chart_type"
             
        query = f"SELECT {select_cols} FROM `{table_id}` WHERE dataset_name = '{dataset_name}'"
        query_job = client.query(query)
        results = query_job.result()
        
        output = [f"Saved Queries for {dataset_name}:\n"]
        has_items = False
        for row in results:
             has_items = True
             view_info = f" (View: {row.view_name})" if "view_name" in columns and row.view_name else ""
             sheet_info = f" (Sheets: {row.worksheet_url})" if "worksheet_url" in columns and row.worksheet_url else ""
             gcs_info = f" (GCS: {row.gcs_bucket})" if "gcs_bucket" in columns and row.gcs_bucket else ""
             chart_info = f" (Chart: {row.chart_type})" if "chart_type" in columns and row.chart_type else ""
             
             output.append(f"* **{row.query_name}**{view_info}{sheet_info}{gcs_info}{chart_info}: {row.description}")
             output.append(f"  `{row.sql}`\n")
             
        if not has_items:
             return f"No saved queries found for dataset '{dataset_name}'."
        return "\n".join(output)
    except Exception as e:
        return f"No saved queries found (Table might not exist yet)."

def export_query_to_sheets(sql: str, spreadsheet_title: str, query_name: str = None, description: str = None, dataset_name: str = None, workspace_dataset: str = None) -> str:
    """
    Executes a query and creates a new Google Spreadsheet containing the results.
    Creates a spreadsheet and updates values using the v4 Sheets API set.
    """
    if workspace_dataset is None: workspace_dataset = _get_user_workspace()
    try:
        from googleapiclient.discovery import build
        from google.cloud import bigquery
        
        # 1. Run Query
        client_bq = _get_bq_client()
        query_job = client_bq.query(sql)
        results = query_job.result()
        
        # 2. Format Rows
        if not hasattr(results, 'schema'):
             return "Query returned no schema layout to export."
             
        headers = [f.name for f in results.schema]
        rows = [headers]
        for row in results:
             rows.append([str(row[f.name]) for f in results.schema])
             
        if len(rows) <= 1:
             return "Query returned no rows to export."
             
        # 3. Create Spreadsheet
        service = _get_sheets_service()
        spreadsheet_body = {
             'properties': {
                  'title': spreadsheet_title
             }
        }
        res = service.spreadsheets().create(body=spreadsheet_body).execute()
        spreadsheet_id = res.get('spreadsheetId')
        spreadsheet_url = res.get('spreadsheetUrl')
        
        # 4. Append Values
        value_range_body = {
             'values': rows
        }
        service.spreadsheets().values().update(
             spreadsheetId=spreadsheet_id,
             range="Sheet1!A1",
             valueInputOption="USER_ENTERED",
             body=value_range_body
        ).execute()
        
        msg = f"Successfully created Spreadsheet '{spreadsheet_title}'. URL: {spreadsheet_url}"
        if query_name and dataset_name:
             desc = description or f"Exported to Sheets: {spreadsheet_title}"
             save_msg = save_query(query_name, sql, desc, dataset_name, workspace_dataset, worksheet_url=spreadsheet_url)
             msg += f"\nTracking Note: {save_msg}"
             
        return msg
    except Exception as e:
        return f"Error exporting to Sheets: {e}"

def export_query_to_gcs(sql: str, bucket_name: str, file_name: str = "result.csv", query_name: str = None, description: str = None, dataset_name: str = None, workspace_dataset: str = None) -> str:
    """
    Executes a query and exports the full results to a Google Cloud Storage bucket.
    Creates the bucket if it does not exist.
    """
    if workspace_dataset is None: workspace_dataset = _get_user_workspace()
    try:
        from google.cloud import storage
        client_bq = _get_bq_client()
        client_gcs = _get_gcs_client()
        
        project_id = client_bq.project
        
        loc = "US"
        regional_ws = workspace_dataset
        if dataset_name:
             target = dataset_name if "." in dataset_name else f"{project_id}.{dataset_name}"
             try:
                 ds = client_bq.get_dataset(target)
                 loc = ds.location or "US"
             except Exception:
                 pass
             regional_ws = _get_workspace_for_dataset(client_bq, dataset_name, workspace_dataset)
        
        # 1. Create Bucket if not exists (in synced location)
        try:
             bucket = client_gcs.get_bucket(bucket_name)
        except Exception:
             bucket = client_gcs.create_bucket(bucket_name, location=loc)
             
        # 2. Save Query to Temp Table
        import time
        temp_table_id = f"{project_id}.{regional_ws}.temp_export_{int(time.time())}"
        
        job_config = bigquery.QueryJobConfig(
             destination=temp_table_id,
             write_disposition="WRITE_TRUNCATE"
        )
        query_job = client_bq.query(sql, job_config=job_config)
        query_job.result() # Wait for query
        
        # 3. Extract to GCS
        destination_uri = f"gs://{bucket_name}/{file_name}"
        extract_job = client_bq.extract_table(temp_table_id, destination_uri)
        extract_job.result() # Wait for extract
        
        # 4. Clean up temp table
        client_bq.delete_table(temp_table_id)
        
        msg = f"Successfully exported full query results to {destination_uri}"
        if query_name and dataset_name:
             desc = description or f"Exported to GCS: {destination_uri}"
             save_msg = save_query(query_name, sql, desc, dataset_name, workspace_dataset, gcs_bucket=destination_uri)
             msg += f"\nTracking Note: {save_msg}"
             
        return msg
        
    except Exception as e:
        return f"Error exporting to GCS: {e}"

def load_dataset_analysis(dataset_name: str, workspace_dataset: str = None) -> str:
    """
    Loads saved semantic analysis and insights for a given dataset scope.
    """
    if workspace_dataset is None: workspace_dataset = _get_user_workspace()
    try:
        from google.cloud import bigquery
        client = _get_bq_client()
        project_id = client.project
        
        regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace_dataset)
        table_id = f"{project_id}.{regional_ws}.dataset_analysis"
        
        query = f"SELECT analysis FROM `{table_id}` WHERE dataset_name = '{dataset_name}'"
        query_job = client.query(query)
        results = query_job.result()
        
        output = [f"Accumulated Insights for {dataset_name}:\n"]
        has_items = False
        for row in results:
             has_items = True
             output.append(f"- {row.analysis}")
             
        if not has_items:
             return f"No previous analysis found for dataset '{dataset_name}'."
        return "\n".join(output)
    except Exception as e:
        return f"No previous analysis found (Table might not exist yet)."

def set_default_dataset(dataset_name: str, workspace_dataset: str = None) -> str:
    """
    Sets a specific dataset as the default dataset for the workspace.
    """
    if workspace_dataset is None: workspace_dataset = _get_user_workspace()
    try:
        from google.cloud import bigquery
        client = _get_bq_client()
        project_id = client.project
        table_id = f"{project_id}.{workspace_dataset}.default_dataset"
        
        schema = [bigquery.SchemaField("dataset_name", "STRING", mode="REQUIRED")]
        table = bigquery.Table(table_id, schema=schema)
        
        try:
             client.get_table(table_id)
        except Exception:
             client.create_table(table)
             
        # 1. Clear previous default
        client.query(f"DELETE FROM `{table_id}` WHERE true").result()
        
        # 2. Insert new default
        rows = [{"dataset_name": dataset_name}]
        errors = client.insert_rows_json(table_id, rows)
        if errors:
             return f"Error setting default: {errors}"
        return f"Successfully set '{dataset_name}' as your default dataset."
    except Exception as e:
        return f"Error setting default dataset: {e}"

def load_default_dataset(workspace_dataset: str = None) -> str:
    """
    Loads the default dataset if one is set. Returns the string name or 'None'.
    """
    if workspace_dataset is None: workspace_dataset = _get_user_workspace()
    try:
        from google.cloud import bigquery
        client = _get_bq_client()
        project_id = client.project
        table_id = f"{project_id}.{workspace_dataset}.default_dataset"
        
        query = f"SELECT dataset_name FROM `{table_id}` LIMIT 1"
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
             return row.dataset_name
        return "None"
    except Exception:
        return "None"

def create_gcs_external_table(gcs_uri: str, table_name: str, dataset_name: str = None, file_format: str = "CSV", workspace_dataset: str = None) -> str:
    """
    Creates a BigLake external table in BigQuery linked to a Google Cloud Storage bucket.
    This automatically provisions a Cloud Resource connection and grants it IAM access to the bucket.
    
    Args:
        gcs_uri: The Google Cloud Storage URI to read from (e.g. 'gs://my-bucket/data/*' or just 'my-bucket').
        table_name: The name of the new BigQuery table to create.
        dataset_name: Active scope context to discover target region.
        file_format: The format of the files (e.g. 'CSV', 'PARQUET', 'AVRO', 'NEWLINE_DELIMITED_JSON'). Defaults to CSV.
        workspace_dataset: Destination base workspace holding the mounting table.
    """
    if workspace_dataset is None: workspace_dataset = _get_user_workspace()
    try:
        from google.cloud import bigquery
        from google.cloud import storage
        import subprocess
        import json
        
        client = _get_bq_client()
        project_id = client.project
        
        # 1. Parse URI
        gcs_uri = gcs_uri.strip()
        if not gcs_uri.startswith("gs://"):
            bucket_name = gcs_uri.split("/")[0]
            gcs_uri = f"gs://{gcs_uri}/*"
        else:
            bucket_name = gcs_uri.split("gs://")[1].split("/")[0]
            
        # 2. Get target location securely via multiplexer lookup
        loc = "US"
        regional_ws = workspace_dataset
        if dataset_name:
             target = dataset_name if "." in dataset_name else f"{project_id}.{dataset_name}"
             try:
                 ds = client.get_dataset(target)
                 loc = ds.location or "US"
             except Exception:
                 pass
             regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace_dataset)
             
        # 3. Connection Name format (Strictly isolated per bucket!)
        conn_id = f"gcs_conn_{bucket_name.replace('-', '_')}"
        full_conn_id = f"{project_id}.{loc}.{conn_id}"
        
        # 4. Provision Connection via local CLI
        res = subprocess.run(["bq", "show", "--connection", "--format=json", "--project_id="+project_id, "--location="+loc, conn_id], capture_output=True, text=True)
        if res.returncode != 0:
            res_create = subprocess.run(["bq", "mk", "--connection", "--connection_type=CLOUD_RESOURCE", "--project_id="+project_id, "--location="+loc, conn_id], capture_output=True, text=True)
            if res_create.returncode != 0:
                return f"Error creating connection '{conn_id}': {res_create.stderr}"
            res = subprocess.run(["bq", "show", "--connection", "--format=json", "--project_id="+project_id, "--location="+loc, conn_id], capture_output=True, text=True)
            
        if not res.stdout.strip():
             return f"Error: Could not retrieve connection details for '{conn_id}'."
             
        conn_info = json.loads(res.stdout)
        service_account = conn_info.get("cloudResource", {}).get("serviceAccountId")
        
        if not service_account:
            return "Error: Could not determine service account from BigLake connection."
            
        # 5. Grant IAM role to the service account on the bucket
        storage_client = _get_gcs_client()
        bucket = storage_client.get_bucket(bucket_name)
        policy = bucket.get_iam_policy(requested_policy_version=3)
        
        role = "roles/storage.objectViewer"
        member = f"serviceAccount:{service_account}"
        
        has_role = False
        for binding in policy.bindings:
            if binding["role"] == role and member in binding["members"]:
                 has_role = True
                 break
        
        if not has_role:
             policy.bindings.append({"role": role, "members": {member}})
             bucket.set_iam_policy(policy)
             import time
             time.sleep(5) # Wait for IAM propagation
             
        # 6. Create External Table
        table_id = f"{project_id}.{regional_ws}.{table_name}"
        
        file_format_upper = file_format.upper()
        if file_format_upper not in ["CSV", "PARQUET", "AVRO", "NEWLINE_DELIMITED_JSON"]:
            file_format_upper = "CSV"
            
        table = bigquery.Table(table_id)
        external_config = bigquery.ExternalConfig(file_format_upper)
        external_config.source_uris = [gcs_uri]
        external_config.connection_id = full_conn_id
        
        if file_format_upper == "CSV":
             import csv
             import io
             import re
             
             prefix = gcs_uri.split(f"gs://{bucket_name}/", 1)[-1].replace("*", "")
             blobs = storage_client.list_blobs(bucket_name, prefix=prefix, max_results=1)
             first_blob = next(blobs, None)
             
             if first_blob:
                  try:
                       chunk_bytes = first_blob.download_as_bytes(start=0, end=4096)
                       chunk_str = chunk_bytes.decode('utf-8', errors='ignore')
                       first_line = chunk_str.splitlines()[0]
                       
                       reader = csv.reader(io.StringIO(first_line))
                       headers = next(reader)
                       custom_schema = []
                       for i, h in enumerate(headers):
                            raw_name = h.strip()
                            clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', raw_name)
                            if not clean_name:
                                 clean_name = f"col_{i}"
                            if clean_name[0].isdigit():
                                 clean_name = f"_{clean_name}"
                            custom_schema.append(bigquery.SchemaField(clean_name, "STRING"))
                       table.schema = custom_schema
                  except Exception as e:
                       print("Warning: Failed manual schema CSV extraction:", e)
                       
             external_config.autodetect = False # We provided it manually
             csv_options = bigquery.CSVOptions()
             csv_options.skip_leading_rows = 1  # Ignore the header line inside the file data itself
             external_config.csv_options = csv_options
             
        elif file_format_upper == "NEWLINE_DELIMITED_JSON":
             external_config.autodetect = True
             
        table.external_data_configuration = external_config
        
        client.delete_table(table_id, not_found_ok=True)
        client.create_table(table)
        
        return f"Successfully established BigLake mount! External table created at `{table_id}` reading directly from {gcs_uri}."
        
    except Exception as e:
        return f"Error creating GCS external table: {e}"

async def create_pie_chart(title: str, labels: list[str], values: list[float], tool_context: ToolContext = None) -> str:
    """
    Creates an interactive Pie Chart visualization in the UI.
    Use this when the user asks for a pie chart or when data is well-suited for proportional visualization.
    
    Args:
        title: The title of the chart.
        labels: A list of string labels/categories.
        values: A list of numeric values corresponding to each label.
        tool_context: Internal ADK context object injected automatically.
    """
    if len(labels) != len(values):
         return "Error: labels and values lists must be the same length."
    
    if tool_context:
        import uuid
        unique_surface_id = f"chart_{uuid.uuid4().hex[:8]}"
        chart_component_id = f"pie_chart_{unique_surface_id}"
        
        flat_components = [{
            "id": chart_component_id,
            "component": {
                "PieChart": {
                    "chartTitle": {"literalString": title},
                    "labels": {"literalArray": [{"literalString": str(l)} for l in labels]},
                    "values": {"literalArray": [{"literalNumber": float(v)} for v in values]}
                }
            }
        }]
        
        a2ui_payload = [
            {
                "beginRendering": {
                    "surfaceId": unique_surface_id,
                    "root": chart_component_id
                }
            },
            {
                "surfaceUpdate": {
                    "surfaceId": unique_surface_id,
                    "components": flat_components
                }
            }
        ]
        
        try:
            tool_context.state['pending_bq_a2ui'] = a2ui_payload
            global _latest_a2ui_payload
            _latest_a2ui_payload = a2ui_payload
            print(f"DEBUG TOOL_CONTEXT: Successfully set pending_bq_a2ui for pie chart '{title}'")
        except Exception as e:
            print(f"DEBUG TOOL_CONTEXT ERROR: Could not set state: {e}")
    
    return f"Successfully created Pie Chart '{title}' in the UI. The chart is now visible in the results panel."

async def create_bar_chart(title: str, labels: list[str], values: list[float], chart_type: str = "bar", tool_context: ToolContext = None) -> str:
    """
    Creates an interactive Bar or Line Chart visualization in the UI.
    Use this when the user asks for a bar chart or line chart, or when data has categorical labels with numeric values.
    
    Args:
        title: The title of the chart.
        labels: A list of string labels/categories (x-axis).
        values: A list of numeric values corresponding to each label (y-axis).
        chart_type: The type of chart to create. Either "bar" or "line". Defaults to "bar".
        tool_context: Internal ADK context object injected automatically.
    """
    if len(labels) != len(values):
         return "Error: labels and values lists must be the same length."
    
    if chart_type not in ("bar", "line"):
        chart_type = "bar"
    
    if tool_context:
        import uuid
        unique_surface_id = f"chart_{uuid.uuid4().hex[:8]}"
        chart_component_id = f"bar_chart_{unique_surface_id}"
        
        flat_components = [{
            "id": chart_component_id,
            "component": {
                "BarChart": {
                    "chartTitle": {"literalString": title},
                    "labels": {"literalArray": [{"literalString": str(l)} for l in labels]},
                    "values": {"literalArray": [{"literalNumber": float(v)} for v in values]},
                    "chartType": {"literalString": chart_type}
                }
            }
        }]
        
        a2ui_payload = [
            {
                "beginRendering": {
                    "surfaceId": unique_surface_id,
                    "root": chart_component_id
                }
            },
            {
                "surfaceUpdate": {
                    "surfaceId": unique_surface_id,
                    "components": flat_components
                }
            }
        ]
        
        try:
            tool_context.state['pending_bq_a2ui'] = a2ui_payload
            global _latest_a2ui_payload
            _latest_a2ui_payload = a2ui_payload
            print(f"DEBUG TOOL_CONTEXT: Successfully set pending_bq_a2ui for {chart_type} chart '{title}'")
        except Exception as e:
            print(f"DEBUG TOOL_CONTEXT ERROR: Could not set state: {e}")
    
    chart_label = "Bar Chart" if chart_type == "bar" else "Line Chart"
    return f"Successfully created {chart_label} '{title}' in the UI. The chart is now visible in the results panel."


def import_web_data_to_bq(table_name: str, columns: list[dict], rows: list[list], dataset_name: str = None, workspace_dataset: str = None, description: str = None) -> str:
    """
    Imports structured data (e.g. extracted from web search results) into a BigQuery table
    in the user's workspace. The table is created in the same region as the active dataset so
    it can be JOINed with existing tables.

    Use this after searching the web and extracting tabular data to make it queryable in BigQuery.

    Args:
        table_name: Name for the new table (e.g., "european_gdp", "world_population").
                    Will be sanitized for BigQuery naming rules.
        columns: A list of column definitions. Each is a dict with "name" (str) and "type" (str).
                 Supported types: "STRING", "INTEGER", "FLOAT", "BOOLEAN", "DATE".
                 Example: [{"name": "country", "type": "STRING"}, {"name": "gdp_billions", "type": "FLOAT"}]
        rows: A list of rows, where each row is a list of values matching the columns order.
              Example: [["Germany", 4460.0], ["France", 3050.0]]
        dataset_name: The active dataset context (used to determine the correct region).
                      If provided, the table will be created in the same region.
        workspace_dataset: The base workspace dataset name. Uses per-user default if not specified.
        description: Optional description of the data source and contents.

    Returns:
        A success or failure message with the full table ID for querying.
    """
    if workspace_dataset is None: workspace_dataset = _get_user_workspace()
    try:
        import re
        client = _get_bq_client()
        project_id = client.project

        # Determine the correct regional workspace
        if dataset_name:
            regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace_dataset)
        else:
            regional_ws = workspace_dataset
            # Ensure the workspace dataset exists
            ws_ref = f"{project_id}.{regional_ws}"
            try:
                client.get_dataset(ws_ref)
            except Exception:
                new_ds = bigquery.Dataset(ws_ref)
                new_ds.location = "US"
                client.create_dataset(new_ds, exists_ok=True)

        # Sanitize table name
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name).lower()
        table_id = f"{project_id}.{regional_ws}.{clean_name}"

        # Build schema from column definitions
        type_map = {
            "STRING": "STRING",
            "INTEGER": "INTEGER",
            "INT": "INTEGER",
            "FLOAT": "FLOAT",
            "FLOAT64": "FLOAT",
            "NUMERIC": "NUMERIC",
            "BOOLEAN": "BOOLEAN",
            "BOOL": "BOOLEAN",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
        }

        schema = []
        for col in columns:
            col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col["name"]).lower()
            col_type = type_map.get(col.get("type", "STRING").upper(), "STRING")
            schema.append(bigquery.SchemaField(col_name, col_type))

        # Create or replace the table
        table = bigquery.Table(table_id, schema=schema)
        if description:
            table.description = description
        client.delete_table(table_id, not_found_ok=True)
        table = client.create_table(table)

        # Insert the rows
        if rows:
            col_names = [re.sub(r'[^a-zA-Z0-9_]', '_', c["name"]).lower() for c in columns]
            rows_to_insert = []
            for row in rows:
                row_dict = {}
                for i, val in enumerate(row):
                    if i < len(col_names):
                        row_dict[col_names[i]] = val
                rows_to_insert.append(row_dict)

            errors = client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                return f"Table `{table_id}` created but encountered insertion errors: {errors[:3]}"

        row_count = len(rows) if rows else 0
        return (
            f"Successfully imported {row_count} rows into `{table_id}`. "
            f"You can now query this table with SQL or JOIN it with other tables in the same region. "
            f"Example: SELECT * FROM `{table_id}` LIMIT 10"
        )

    except Exception as e:
        return f"Error importing web data to BigQuery: {e}"

# ============================================================================
# Phase 1 — New tools for Semantic Intelligence
# ============================================================================

def _assess_dataset_complexity(client, dataset_name: str) -> dict:
    """
    Quick metadata-only assessment of dataset size (< 5 seconds).
    
    Returns:
        {"tables": [{"name": ..., "rows": ..., "bytes": ...}],
         "total_tables": N, "total_bytes": X, "is_large": bool}
    """
    if "." in dataset_name:
        project_id, dataset_id = dataset_name.split(".", 1)
    else:
        project_id = client.project
        dataset_id = dataset_name

    dataset_ref = client.dataset(dataset_id, project=project_id)
    tables = list(client.list_tables(dataset_ref))

    # 2 GB / 5M rows thresholds (matches semantic_catalog.profile_table)
    _LARGE_BYTES = 2 * 1024**3
    _LARGE_ROWS = 5_000_000

    table_info = []
    total_bytes = 0
    is_large = False

    for t in tables:
        try:
            full = client.get_table(t.reference)
            info = {"name": full.table_id, "rows": full.num_rows or 0, "bytes": full.num_bytes or 0}
            table_info.append(info)
            total_bytes += info["bytes"]
            if info["bytes"] > _LARGE_BYTES or info["rows"] > _LARGE_ROWS:
                is_large = True
        except Exception:
            table_info.append({"name": t.table_id, "rows": 0, "bytes": 0})

    return {
        "tables": table_info,
        "total_tables": len(table_info),
        "total_bytes": total_bytes,
        "is_large": is_large,
    }


def start_background_profile(dataset_name: str) -> str:
    """
    Starts a background profiling job for a dataset using Cloud Run Jobs.
    
    Checks profiling status first to prevent duplicate runs.
    Returns immediately with a user-friendly message.
    
    Args:
        dataset_name: The dataset to profile (e.g., bigquery-public-data.stackoverflow).
        
    Returns:
        Status message about what was done.
    """
    try:
        client = _get_bq_client()
        workspace = _get_user_workspace()
        regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace)

        # Ensure catalog tables exist (including profiling_status)
        semantic_catalog.ensure_all_tables(client, regional_ws)

        # Check if already profiling or profiled
        status = semantic_catalog.get_profiling_status(client, regional_ws, dataset_name)
        if status["status"] == "running":
            phase = status.get("phase", "")
            return (
                f"⏳ Dataset `{dataset_name}` is already being profiled "
                f"(phase: {phase}). You can start querying now — "
                f"I'll have full context shortly."
            )
        if status["status"] == "completed":
            return (
                f"✅ Dataset `{dataset_name}` has already been profiled. "
                f"Use `load_semantic_context` to access the knowledge. "
                f"Say **'force re-profile'** if you want to redo it."
            )

        # Quick size assessment
        complexity = _assess_dataset_complexity(client, dataset_name)
        total_gb = complexity["total_bytes"] / 1e9

        # Submit Cloud Run Job
        job_execution_name = _submit_profiling_job(dataset_name, regional_ws)

        # Set status to running
        semantic_catalog.set_profiling_status(
            client, regional_ws, dataset_name, "running",
            phase="structural", job_name=job_execution_name,
        )

        if complexity["is_large"]:
            return (
                f"📊 **Background profiling started** for `{dataset_name}`\n"
                f"- {complexity['total_tables']} tables, {total_gb:.1f} GB total\n"
                f"- This is a large dataset — profiling will run in the background\n"
                f"- You can start querying immediately using schema information\n"
                f"- Full semantic context (descriptions, join inference) will be available in a few minutes"
            )
        else:
            return (
                f"📊 **Background profiling started** for `{dataset_name}`\n"
                f"- {complexity['total_tables']} tables, {total_gb:.1f} GB total\n"
                f"- Results should be ready in under a minute"
            )

    except Exception as e:
        return f"Error starting background profile: {e}"


def _submit_profiling_job(dataset_name: str, workspace: str) -> str:
    """
    Submit a Cloud Run Job execution for background profiling.
    
    Falls back to in-process threading if Cloud Run Jobs API is unavailable
    (e.g., local development).
    """
    try:
        from google.cloud import run_v2

        jobs_client = run_v2.JobsClient()
        project = os.environ.get("GOOGLE_CLOUD_PROJECT", "antoine-exp")
        region = os.environ.get("CLOUD_RUN_REGION", "us-central1")
        job_name = f"projects/{project}/locations/{region}/jobs/dataset-profiler"

        request = run_v2.RunJobRequest(
            name=job_name,
            overrides=run_v2.types.RunJobRequest.Overrides(
                container_overrides=[
                    run_v2.types.RunJobRequest.Overrides.ContainerOverride(
                        args=["--dataset", dataset_name, "--workspace", workspace],
                    )
                ],
            ),
        )
        operation = jobs_client.run_job(request=request)
        execution_name = operation.metadata.name if hasattr(operation, 'metadata') else "submitted"
        print(f"INFO: Submitted Cloud Run Job: {execution_name}")
        return execution_name

    except Exception as e:
        print(f"WARNING: Cloud Run Jobs API unavailable ({e}), falling back to in-process thread")
        return _run_profiling_in_thread(dataset_name, workspace)


def _run_profiling_in_thread(dataset_name: str, workspace: str) -> str:
    """Fallback: run profiling in a daemon thread (for local dev)."""
    import threading

    def _run():
        try:
            import subprocess
            import sys
            script_path = os.path.join(
                os.path.dirname(__file__), "agents", "bq_assistant", "profile_job.py"
            )
            print(f"INFO: Starting background profiling subprocess: {script_path}")
            # Use the same Python interpreter
            result = subprocess.run(
                [sys.executable, script_path, "--dataset", dataset_name, "--workspace", workspace],
                capture_output=True, text=True, timeout=1800,
                env={
                    **os.environ,
                    "DATA_PROJECT": _DATA_PROJECT,
                    "GOOGLE_CLOUD_PROJECT": os.environ.get("GOOGLE_CLOUD_PROJECT", "antoine-exp"),
                    "GOOGLE_GENAI_USE_VERTEXAI": "true",
                    "GOOGLE_CLOUD_LOCATION": os.environ.get("GOOGLE_CLOUD_LOCATION", "global"),
                },
            )
            if result.returncode != 0:
                print(f"WARNING: Background profiling failed (exit={result.returncode}):")
                print(f"  stderr: {result.stderr[:500]}")
                print(f"  stdout: {result.stdout[-200:]}")
            else:
                print(f"INFO: Background profiling completed: {result.stdout[-200:]}")
        except Exception as e:
            print(f"WARNING: Background profiling thread failed: {e}")

    thread = threading.Thread(target=_run, daemon=True, name=f"profiler-{dataset_name}")
    thread.start()
    return f"thread-{thread.name}"


def force_reset_profiling_status(dataset_name: str) -> str:
    """
    Reset the profiling status for a dataset, allowing a fresh re-profile.
    
    Use this when the user says 'force re-profile' or when a previous
    profiling run got stuck.
    
    Args:
        dataset_name: The dataset to reset profiling status for.
        
    Returns:
        Confirmation message.
    """
    try:
        client = _get_bq_client()
        workspace = _get_user_workspace()
        regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace)

        semantic_catalog.set_profiling_status(
            client, regional_ws, dataset_name, "reset",
            details="Manually reset by user",
        )
        return f"✅ Profiling status for `{dataset_name}` has been reset. You can now re-profile."
    except Exception as e:
        return f"Error resetting profiling status: {e}"


def check_profiling_status(dataset_name: str) -> str:
    """
    Check the current profiling status for a dataset.
    
    Use this to check whether a background profiling job is still running,
    has completed, or has failed.
    
    Args:
        dataset_name: The dataset to check profiling status for.
        
    Returns:
        A human-readable status message.
    """
    try:
        client = _get_bq_client()
        workspace = _get_user_workspace()
        regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace)

        status = semantic_catalog.get_profiling_status(client, regional_ws, dataset_name)

        if status["status"] == "unknown":
            return f"Dataset `{dataset_name}` has never been profiled."
        elif status["status"] == "running":
            phase = status.get("phase", "unknown")
            started = status.get("started_at", "?")
            return (
                f"⏳ Dataset `{dataset_name}` is currently being profiled.\n"
                f"- Phase: **{phase}** (structural → semantic)\n"
                f"- Started: {started}\n"
                f"- You can query now — full context will be available when profiling completes."
            )
        elif status["status"] == "completed":
            completed = status.get("completed_at", "?")
            details = status.get("details", "")
            return (
                f"✅ Dataset `{dataset_name}` profiling is complete.\n"
                f"- Completed: {completed}\n"
                f"- {details}"
            )
        elif status["status"] == "failed":
            details = status.get("details", "Unknown error")
            return (
                f"❌ Dataset `{dataset_name}` profiling failed.\n"
                f"- Error: {details}\n"
                f"- You can retry by asking to profile the dataset again."
            )
        else:
            return f"Unknown profiling status for `{dataset_name}`: {status}"

    except Exception as e:
        return f"Error checking profiling status: {e}"


def profile_dataset(dataset_name: str, tool_context: ToolContext = None) -> str:
    """
    Deep-profiles a BigQuery dataset using INFORMATION_SCHEMA and data sampling.
    
    This goes far beyond basic schema listing — it discovers:
    - Column-level data types, NULL percentages, and distinct value counts
    - Sample values for each column
    - Partition and clustering keys
    - Semantic type inference (identifier, metric, dimension, timestamp, etc.)
    - Potential join relationships across tables
    
    All discoveries are persisted in the Semantic Catalog so the agent
    remembers them for future queries. Run this once when starting work
    on a new dataset.

    Args:
        dataset_name: The dataset ID or full 'project.dataset'.
        tool_context: Internal execution context automatically provided by the ADK runtime.

    Returns:
        A structured summary of the profiling results.
    """
    try:
        client = _get_bq_client()
        workspace = _get_user_workspace()
        
        if "." in dataset_name:
            project_id, dataset_id = dataset_name.split(".", 1)
        else:
            project_id = client.project
            dataset_id = dataset_name
        
        regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace)
        
        # Duplicate protection: check if already profiling or profiled
        try:
            semantic_catalog.ensure_all_tables(client, regional_ws)
            status = semantic_catalog.get_profiling_status(client, regional_ws, dataset_name)
            if status["status"] == "running":
                return (
                    f"⏳ Dataset `{dataset_name}` is already being profiled "
                    f"(phase: {status.get('phase', '?')}). "
                    f"You can query now — full context will be available shortly."
                )
            if status["status"] == "completed":
                return (
                    f"✅ Dataset `{dataset_name}` has already been profiled. "
                    f"Say **'force re-profile'** if you want to redo it."
                )
        except Exception:
            pass  # If status check fails, proceed with profiling

        # Ensure workspace dataset exists
        ws_ref = f"{client.project}.{regional_ws}"
        try:
            client.get_dataset(ws_ref)
        except Exception:
            ds = bigquery.Dataset(ws_ref)
            ds.location = "US"
            client.create_dataset(ds, exists_ok=True)
        
        # Ensure catalog tables
        semantic_catalog.ensure_all_tables(client, regional_ws)
        
        # Migrate any legacy analysis
        migration_msg = semantic_catalog.migrate_legacy_analysis(client, regional_ws, dataset_name)
        
        # Get all tables
        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = list(client.list_tables(dataset_ref))
        
        if not tables:
            return f"Dataset '{dataset_name}' has no tables to profile."
        
        output = [f"## Deep Profile of `{dataset_name}`\n"]
        output.append(f"📊 Found **{len(tables)} tables** — profiling each one...\n")
        
        total_columns = 0
        
        for table in tables:
            output.append(f"### 🔍 Profiling `{table.table_id}`...")
            
            profile = semantic_catalog.profile_table(
                client, dataset_name, table.table_id, regional_ws
            )
            
            if "error" in profile:
                output.append(f"  ⚠️ {profile['error']}")
                continue
            
            # Save to catalog
            num_saved = semantic_catalog.save_profile_to_catalog(client, regional_ws, profile)
            total_columns += num_saved
            
            output.append(f"  - Rows: **{profile['total_rows']:,}** | Size: **{profile['total_bytes']:,} bytes**")
            if profile.get("schema_only"):
                output.append(f"  - ⚡ **Schema-only mode** (table too large for data profiling, >2GB or >5M rows)")
                output.append(f"  - Columns cataloged: **{len(profile['columns'])}** (types + structure only, no stats)")
            else:
                output.append(f"  - Columns profiled: **{len(profile['columns'])}**")
            
            for cm in profile["columns"]:
                extras = []
                if cm.get("semantic_type"):
                    extras.append(cm["semantic_type"])
                if cm.get("null_pct", 0) > 50:
                    extras.append(f"⚠️ {cm['null_pct']:.0f}% NULL")
                if cm.get("is_partition_key"):
                    extras.append("🔑 partition")
                if cm.get("is_clustering_key"):
                    extras.append("📎 clustering")
                
                extra_str = f" [{', '.join(extras)}]" if extras else ""
                distinct = cm.get("distinct_count", "?")
                output.append(f"    - `{cm['column_name']}` ({cm['data_type']}) — {distinct} distinct{extra_str}")
        
        # Discover joins
        table_names = [t.table_id for t in tables]
        joins = semantic_catalog.discover_joins(client, regional_ws, dataset_name, table_names)
        
        if joins:
            output.append(f"\n### 🔗 Discovered {len(joins)} potential join(s)")
            for j in joins:
                output.append(f"  - `{j['table_a']}.{j['column_a']}` ↔ `{j['table_b']}.{j['column_b']}`")
        
        output.append(f"\n✅ Profile complete! Saved **{total_columns} column entries** to the Semantic Catalog.")
        if migration_msg and "Migrated" in migration_msg:
            output.append(f"📦 {migration_msg}")
        output.append("The agent will now use this knowledge to write better queries.")
        
        return "\n".join(output)
        
    except Exception as e:
        return f"Error profiling dataset '{dataset_name}': {e}"


def _auto_profile_dataset(client, dataset_name: str, regional_ws: str) -> str:
    """Lightweight auto-profile: schema + basic stats, no sample values.
    
    Called automatically by load_semantic_context when a dataset has never
    been profiled. Keeps it fast by skipping per-column sample value queries
    and capping at 10 tables.
    """
    if "." in dataset_name:
        project_id, dataset_id = dataset_name.split(".", 1)
    else:
        project_id = client.project
        dataset_id = dataset_name

    # Ensure workspace dataset exists
    ws_ref = f"{client.project}.{regional_ws}"
    try:
        client.get_dataset(ws_ref)
    except Exception:
        ds = bigquery.Dataset(ws_ref)
        ds.location = "US"
        client.create_dataset(ds, exists_ok=True)

    semantic_catalog.ensure_all_tables(client, regional_ws)

    dataset_ref = client.dataset(dataset_id, project=project_id)
    tables = list(client.list_tables(dataset_ref))

    total_cols = 0
    for table in tables[:10]:  # Cap at 10 tables to keep fast
        try:
            profile = semantic_catalog.profile_table(
                client, dataset_name, table.table_id, regional_ws,
            )
            if "error" not in profile:
                num_saved = semantic_catalog.save_profile_to_catalog(client, regional_ws, profile)
                total_cols += num_saved
        except Exception as e:
            print(f"DEBUG: Auto-profile skipping {table.table_id}: {e}")
            continue

    # Discover joins
    table_names = [t.table_id for t in tables[:10]]
    try:
        semantic_catalog.discover_joins(client, regional_ws, dataset_name, table_names)
    except Exception:
        pass

    return f"Auto-profiled {len(tables[:10])} tables ({total_cols} columns) for {dataset_name}"


def probe_column(dataset_name: str, table_name: str, column_name: str, limit: int = 20) -> str:
    """
    Runs an exploratory query to inspect the actual values in a specific column.
    
    Use this BEFORE writing a query when you're unsure about what values exist
    in a column (e.g., what regions, statuses, categories are available).
    This helps avoid writing queries that return 0 rows due to incorrect filter values.

    Args:
        dataset_name: The dataset ID or full 'project.dataset'.
        table_name: The table name within the dataset.
        column_name: The column to inspect.
        limit: Maximum number of distinct values to return. Defaults to 20.

    Returns:
        A formatted summary showing value distribution, top values, and NULL stats.
    """
    try:
        client = _get_bq_client()
        
        if "." in dataset_name:
            table_ref = f"{dataset_name}.{table_name}"
        else:
            table_ref = f"{client.project}.{dataset_name}.{table_name}"
        
        return query_planner.probe_column_values(client, table_ref, column_name, limit)
    except Exception as e:
        return f"Error probing column '{column_name}': {e}"


def dry_run(sql: str) -> str:
    """
    Validates a SQL query without executing it and estimates the cost.
    
    Use this BEFORE running expensive queries to:
    1. Check for syntax errors
    2. Verify table/column references exist
    3. Estimate how much data will be scanned (and approximate cost)
    
    This does NOT consume any BigQuery resources or cost money.

    Args:
        sql: The SQL query string to validate.

    Returns:
        Validation result with estimated cost, or error details if invalid.
    """
    try:
        client = _get_bq_client()
        result = query_planner.dry_run_query(client, sql)
        
        if result["valid"]:
            bytes_str = _format_bytes(result["estimated_bytes"])
            cost = result["estimated_cost_usd"]
            cost_str = f"${cost:.4f}" if cost < 1 else f"${cost:.2f}"
            return (
                f"✅ **Query is valid!**\n"
                f"  - Estimated data scan: **{bytes_str}**\n"
                f"  - Estimated cost: **{cost_str}**\n"
                f"  - Ready to execute."
            )
        else:
            diagnosis = query_planner.diagnose_error(sql, result["error"])
            return f"❌ **Query validation failed.**\n\n{diagnosis}"
    except Exception as e:
        return f"Error during dry run: {e}"


def _format_bytes(num_bytes: int) -> str:
    """Format bytes into a human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def submit_feedback(
    dataset_name: str,
    feedback_type: str,
    table_name: str = None,
    column_name: str = None,
    business_name: str = None,
    description: str = None,
    raw_value: str = None,
    business_meaning: str = None,
    term: str = None,
    sql_expression: str = None,
    table_a: str = None,
    column_a: str = None,
    table_b: str = None,
    column_b: str = None,
    original_nl: str = None,
    original_sql: str = None,
    corrected_sql: str = None,
    notes: str = None,
) -> str:
    """
    Records human feedback to improve the agent's understanding of the dataset.
    
    Use this whenever the user corrects the agent's understanding, provides
    a business definition, or clarifies data semantics.
    
    The feedback is permanently stored so the agent learns and improves.

    Args:
        dataset_name: The dataset context for this feedback.
        feedback_type: One of:
            - "column_rename": Assign a business name/description to a column
            - "value_mapping": Define what a coded value means (e.g., status=4 → "Churned")
            - "glossary": Define a business term (e.g., "revenue" = SUM(gross_total))
            - "join_pattern": Define how two tables should be joined
            - "query_correction": Correct a SQL query the agent generated
        table_name: (for column_rename, value_mapping) The table name.
        column_name: (for column_rename, value_mapping) The column name.
        business_name: (for column_rename) The human-friendly name for the column.
        description: (for column_rename, glossary) Detailed description.
        raw_value: (for value_mapping) The raw coded value (e.g., "4").
        business_meaning: (for value_mapping) What the value means (e.g., "Churned").
        term: (for glossary) The business term.
        sql_expression: (for glossary) The SQL equivalent.
        table_a: (for join_pattern) First table.
        column_a: (for join_pattern) Join column in first table.
        table_b: (for join_pattern) Second table.
        column_b: (for join_pattern) Join column in second table.
        original_nl: (for query_correction) The original natural language question.
        original_sql: (for query_correction) The SQL the agent generated.
        corrected_sql: (for query_correction) The correct SQL.
        notes: (for query_correction) Explanation of what was wrong.

    Returns:
        Confirmation of what was recorded.
    """
    try:
        client = _get_bq_client()
        workspace = _get_user_workspace()
        regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace)
        
        # Build feedback_data dict based on feedback_type
        feedback_data = {}
        if feedback_type == "column_rename":
            feedback_data = {"table": table_name, "column": column_name, "business_name": business_name, "description": description}
        elif feedback_type == "value_mapping":
            feedback_data = {"table": table_name, "column": column_name, "raw_value": raw_value, "business_meaning": business_meaning}
        elif feedback_type == "glossary":
            feedback_data = {"term": term, "sql_expression": sql_expression, "description": description}
        elif feedback_type == "join_pattern":
            feedback_data = {"table_a": table_a, "column_a": column_a, "table_b": table_b, "column_b": column_b}
        elif feedback_type == "query_correction":
            feedback_data = {"original_nl": original_nl, "original_sql": original_sql, "corrected_sql": corrected_sql, "notes": notes}
        
        result = semantic_catalog.update_catalog_from_feedback(
            client, regional_ws, dataset_name, feedback_type, feedback_data
        )
        return f"✅ Feedback recorded! {result}"
    except Exception as e:
        return f"Error recording feedback: {e}"


def get_query_suggestions(dataset_name: str, question: str) -> str:
    """
    Finds similar past queries that were successfully executed against this dataset.
    
    Use this BEFORE writing a new SQL query — if a similar question was asked before,
    the agent can reuse or adapt the previous SQL, improving accuracy significantly.

    Args:
        dataset_name: The dataset to search within.
        question: The user's natural language question.

    Returns:
        A list of similar past queries with their SQL and outcomes.
    """
    try:
        client = _get_bq_client()
        workspace = _get_user_workspace()
        regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace)
        
        similar = semantic_catalog.find_similar_queries(client, regional_ws, question, dataset_name)
        
        if not similar:
            return "No similar past queries found. This appears to be a new type of question."
        
        lines = [f"## Similar Past Queries for `{dataset_name}`\n"]
        for i, q in enumerate(similar, 1):
            feedback_label = f" (User rated: {q['user_feedback']})" if q.get("user_feedback") else ""
            lines.append(f"### {i}. \"{q['natural_language']}\"{feedback_label}")
            lines.append(f"```sql\n{q['sql']}\n```")
            lines.append(f"Returned {q['result_row_count']} rows | Relevance: {q['relevance_score']}\n")
        
        return "\n".join(lines)
    except Exception as e:
        return f"Error searching past queries: {e}"


def load_semantic_context(dataset_name: str, tables: list[str] = None) -> str:
    """
    Loads the full semantic knowledge for a dataset — column metadata, join patterns,
    business glossary, and value mappings — formatted for query planning.
    
    Use this BEFORE translating a natural language question to SQL.
    The returned context helps the agent:
    - Know which columns exist and what they mean
    - Know common join patterns between tables
    - Understand business terms and their SQL equivalents
    - Know encoded values (e.g., status codes)
    
    **Auto-profile**: If this is the first time the agent encounters this dataset
    (no entries in the semantic catalog), it will automatically run a lightweight
    profile first. This may take 30-60 seconds on the first call.

    Args:
        dataset_name: The dataset to load context for.
        tables: Optional list of specific table names to focus on.

    Returns:
        A structured text summary of all known dataset semantics.
    """
    try:
        client = _get_bq_client()
        workspace = _get_user_workspace()
        regional_ws = _get_workspace_for_dataset(client, dataset_name, workspace)
        
        # Check profiling status and auto-trigger background profile if needed
        status_msg = ""
        catalog_count = semantic_catalog.count_catalog_entries(client, regional_ws, dataset_name)
        
        if catalog_count == 0:
            # No catalog entries — check if profiling is already in progress
            status = semantic_catalog.get_profiling_status(client, regional_ws, dataset_name)
            
            if status["status"] == "running":
                phase = status.get("phase", "?")
                status_msg = (
                    f"⏳ **Background profiling in progress** (phase: {phase}). "
                    f"Using schema-only context for now — full knowledge will be available shortly.\n\n"
                )
            elif status["status"] == "completed":
                # Completed but no catalog entries? Something went wrong — re-trigger
                print(f"WARNING: Status=completed but catalog empty for {dataset_name}, re-triggering")
                try:
                    bg_result = start_background_profile(dataset_name)
                    status_msg = f"📊 {bg_result}\n\n"
                except Exception as e:
                    status_msg = f"⚠️ Could not start background profiling: {e}\n\n"
            else:
                # Unknown or failed — trigger background profiling
                print(f"INFO: Dataset '{dataset_name}' has no catalog entries — starting background profiling...")
                try:
                    bg_result = start_background_profile(dataset_name)
                    status_msg = f"{bg_result}\n\n"
                except Exception as e:
                    status_msg = f"⚠️ Could not start background profiling: {e}\n\n"
        else:
            # Catalog has entries — check if profiling is still in progress
            status = semantic_catalog.get_profiling_status(client, regional_ws, dataset_name)
            if status["status"] == "running":
                phase = status.get("phase", "?")
                status_msg = (
                    f"⏳ Background profiling still in progress (phase: {phase}). "
                    f"Context may be incomplete.\n\n"
                )
        
        context = semantic_catalog.get_semantic_context(client, regional_ws, dataset_name, tables)
        return status_msg + context
    except Exception as e:
        return f"Error loading semantic context: {e}"


def diagnose_query(sql: str, dataset_name: str, error_message: str = None) -> str:
    """
    Diagnoses why a query failed or returned 0 rows.
    
    If error_message is provided, parses the BigQuery error for actionable suggestions.
    If error_message is None, assumes the query returned 0 rows and analyzes
    each WHERE clause filter independently to find which one is too restrictive.

    Args:
        sql: The SQL query that failed or returned empty.
        dataset_name: The dataset context.
        error_message: The BigQuery error message (if the query errored).
            If None, assumes 0-row result and diagnoses filters.

    Returns:
        A diagnostic report with specific suggestions for fixing the query.
    """
    try:
        client = _get_bq_client()
        
        if error_message:
            return query_planner.diagnose_error(sql, error_message)
        else:
            return query_planner.diagnose_empty_result(client, sql, dataset_name)
    except Exception as e:
        return f"Error diagnosing query: {e}"
