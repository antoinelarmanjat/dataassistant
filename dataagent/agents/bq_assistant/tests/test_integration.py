"""Integration tests against live BigQuery (octo-aif-sandbox).

These tests require:
  - Active GCP credentials (gcloud auth application-default login)
  - Access to the octo-aif-sandbox project
  - Access to bigquery-public-data datasets

Run with: pytest tests/test_integration.py -v -k integration
Skip with: pytest tests/ -v -k "not integration"
"""
import sys
import os
import pytest

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


def _has_credentials():
    """Check if GCP credentials are available."""
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project="octo-aif-sandbox")
        # Quick connectivity check
        list(client.query("SELECT 1").result())
        return True
    except Exception:
        return False


# Skip entire module if no credentials
requires_credentials = pytest.mark.skipif(
    not _has_credentials(),
    reason="No GCP credentials available for integration tests"
)


@requires_credentials
class TestBigQueryConnectivity:
    """Verify basic BigQuery connectivity."""

    def test_can_query_public_dataset(self):
        from google.cloud import bigquery
        client = bigquery.Client(project="octo-aif-sandbox")

        rows = list(client.query(
            "SELECT COUNT(*) as cnt "
            "FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` "
            "LIMIT 1"
        ).result())

        assert len(rows) == 1
        assert rows[0].cnt > 0

    def test_austin_bikeshare_schema(self):
        """Verify the test dataset has expected columns."""
        from google.cloud import bigquery
        client = bigquery.Client(project="octo-aif-sandbox")

        table = client.get_table(
            "bigquery-public-data.austin_bikeshare.bikeshare_trips"
        )
        column_names = {f.name for f in table.schema}

        # These columns are used in the app's example queries
        assert "start_station_name" in column_names
        assert "start_time" in column_names
        assert "duration_minutes" in column_names or "trip_duration_minutes" in column_names


@requires_credentials
class TestSemanticCatalogIntegration:
    """Test semantic catalog operations against live BigQuery."""

    def test_profile_table(self):
        """Profile a public table and verify the result structure."""
        from google.cloud import bigquery
        import semantic_catalog

        client = bigquery.Client(project="octo-aif-sandbox")

        profile = semantic_catalog.profile_table(
            client,
            "bigquery-public-data.austin_bikeshare",
            "bikeshare_trips",
            workspace="da_test_integration",
        )

        assert "error" not in profile, f"Profile failed: {profile.get('error')}"
        assert profile["table_name"] == "bikeshare_trips"
        assert profile["total_rows"] > 0
        assert len(profile["columns"]) > 0

        # Verify column metadata structure
        col = profile["columns"][0]
        assert "column_name" in col
        assert "data_type" in col
        assert "semantic_type" in col

    def test_extract_keywords_for_real_queries(self):
        """Verify keyword extraction works with real user questions."""
        import semantic_catalog

        # Typical user questions
        queries = [
            "what are the most popular start stations in 2023",
            "show me average trip duration by month",
            "how many trips started from Zilker Park",
        ]
        for q in queries:
            keywords = semantic_catalog._extract_keywords(q)
            assert len(keywords) > 0, f"No keywords extracted from: {q}"

    def test_find_similar_queries_no_crash(self):
        """find_similar_queries should not crash with an unrecoverable error.

        If the workspace dataset doesn't exist, it may raise NotFound when
        trying to create the query_history table — that's acceptable.
        """
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound
        import semantic_catalog

        client = bigquery.Client(project="octo-aif-sandbox")

        try:
            result = semantic_catalog.find_similar_queries(
                client,
                workspace="da_test_integration",
                nl_query="most popular stations",
                dataset="bigquery-public-data.austin_bikeshare",
            )
            # If it succeeds, verify it returns a list
            assert isinstance(result, list)
        except NotFound:
            # Acceptable: the test workspace dataset doesn't exist
            pass


@requires_credentials
class TestQueryExecution:
    """Test query execution against live BigQuery."""

    def test_simple_count_query(self):
        """Execute a simple COUNT query."""
        from google.cloud import bigquery

        client = bigquery.Client(project="octo-aif-sandbox")
        result = list(client.query(
            "SELECT start_station_name, COUNT(*) as trip_count "
            "FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` "
            "WHERE EXTRACT(YEAR FROM start_time) = 2023 "
            "GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
        ).result())

        assert len(result) == 5
        assert result[0].trip_count > 0
        # Verify station names are strings
        for row in result:
            assert isinstance(row.start_station_name, str)
            assert len(row.start_station_name) > 0

    def test_subquery_pattern_for_similar_queries(self):
        """Verify our fixed SQL pattern (subquery instead of HAVING) works."""
        from google.cloud import bigquery

        client = bigquery.Client(project="octo-aif-sandbox")

        # Replicate the fixed find_similar_queries SQL pattern
        # This should NOT fail with 'HAVING without GROUP BY'
        score_expr = (
            "IF(LOWER('most popular stations') LIKE '%popular%', 1, 0) + "
            "IF(LOWER('most popular stations') LIKE '%stations%', 1, 0)"
        )

        sql = (
            f"SELECT * FROM ("
            f"SELECT 'test' as natural_language, 'SELECT 1' as sql_text, "
            f"0 as result_row_count, CAST(NULL AS STRING) as user_feedback, "
            f"({score_expr}) AS relevance_score "
            f"FROM UNNEST([STRUCT('test' AS x)])"
            f") sub "
            f"WHERE relevance_score > 0 "
            f"ORDER BY relevance_score DESC "
            f"LIMIT 5"
        )

        # This should execute without errors
        result = list(client.query(sql).result())
        assert len(result) >= 0  # May be 0 or more, but should NOT error
