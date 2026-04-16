import sys
import os

print("--- Inspecting google-adk ---")
try:
    import adk
    print(f"adk version: {getattr(adk, '__version__', 'unknown')}")
    print("adk dir:", [name for name in dir(adk) if not name.startswith('_')])
except ImportError as e:
    print(f"Failed to import adk: {e}")

print("\n--- Inspecting google-cloud-bigquery ---")
try:
    from google.cloud import bigquery
    client = bigquery.Client()
    print("BigQuery Client created successfully.")
    
    # Test listing projects
    try:
        projects = list(client.list_projects())
        print(f"Found {len(projects)} projects.")
        for p in projects:
            print(f"  Project: {p.project_id} ({p.friendly_name})")
    except Exception as e:
        print(f"Failed to list projects: {e}")
        
except Exception as e:
    print(f"Error initializing BigQuery client: {e}")

print("\n--- Checking for Agent/Tool in ADK ---")
try:
    import adk
    # Try to find Agent or Tool related classes
    for item in dir(adk):
        if not item.startswith('_'):
            obj = getattr(adk, item)
            if isinstance(obj, type):
                print(f"Class: {item}")
except:
    pass
