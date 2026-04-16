from google.cloud import bigquery

def test_list_projects():
    try:
        client = bigquery.Client()
        print("BigQuery Client created.")
        projects = list(client.list_projects())
        print(f"Found {len(projects)} projects:")
        for p in projects:
            print(f"  - {p.project_id}")
            # Try listing datasets for this project
            try:
                datasets = list(client.list_datasets(project=p.project_id))
                print(f"    Datasets: {[d.dataset_id for d in datasets]}")
            except Exception as e:
                print(f"    Failed to list datasets for {p.project_id}: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_list_projects()
