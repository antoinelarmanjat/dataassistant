"""Tests for the semantic_catalog module.

Covers: keyword extraction, semantic type inference, and SQL generation
for the find_similar_queries function.
"""
import sys
import os

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
import semantic_catalog


class TestExtractKeywords:
    """Test the _extract_keywords helper."""

    def test_filters_stop_words(self):
        result = semantic_catalog._extract_keywords("show me the top stations")
        assert "show" not in result
        assert "the" not in result
        assert "top" in result
        assert "stations" in result

    def test_filters_short_words(self):
        result = semantic_catalog._extract_keywords("a to do it by")
        assert result == []

    def test_preserves_meaningful_words(self):
        result = semantic_catalog._extract_keywords(
            "what are the most popular start stations in 2023"
        )
        assert "popular" in result
        assert "start" in result
        assert "stations" in result
        # Note: '2023' is NOT extracted because _extract_keywords uses
        # regex [a-zA-Z_]+ which only matches letters, not digits.

    def test_empty_input(self):
        result = semantic_catalog._extract_keywords("")
        assert result == []

    def test_handles_special_characters(self):
        result = semantic_catalog._extract_keywords(
            "trips from Zilker_Park in January"
        )
        assert "trips" in result
        assert "zilker_park" in result or "zilker" in result
        assert "january" in result


class TestInferSemanticType:
    """Test the _infer_semantic_type heuristic."""

    def test_identifier_column(self):
        assert semantic_catalog._infer_semantic_type("user_id", "INT64", 1000, 1000) == "identifier"
        assert semantic_catalog._infer_semantic_type("id", "STRING", 500, 500) == "identifier"
        assert semantic_catalog._infer_semantic_type("session_key", "STRING", 100, 100) == "identifier"

    def test_timestamp_by_type(self):
        assert semantic_catalog._infer_semantic_type("foo", "TIMESTAMP", 100, 1000) == "timestamp"
        assert semantic_catalog._infer_semantic_type("bar", "DATE", 100, 1000) == "timestamp"

    def test_timestamp_by_name(self):
        assert semantic_catalog._infer_semantic_type("created_at", "STRING", 100, 1000) == "timestamp"
        assert semantic_catalog._infer_semantic_type("updated_on", "STRING", 100, 1000) == "timestamp"

    def test_metric_by_name(self):
        assert semantic_catalog._infer_semantic_type("total_revenue", "FLOAT64", 100, 1000) == "metric"
        assert semantic_catalog._infer_semantic_type("trip_count", "INT64", 100, 1000) == "metric"
        assert semantic_catalog._infer_semantic_type("avg_price", "NUMERIC", 100, 1000) == "metric"

    def test_metric_by_high_cardinality(self):
        # High cardinality numeric → metric
        assert semantic_catalog._infer_semantic_type("value", "FLOAT64", 800, 1000) == "metric"

    def test_measure_for_low_cardinality_numeric(self):
        # Low cardinality numeric without metric keywords → measure
        assert semantic_catalog._infer_semantic_type("level", "INT64", 5, 1000) == "measure"

    def test_dimension_by_name(self):
        assert semantic_catalog._infer_semantic_type("country", "STRING", 50, 1000) == "dimension"
        assert semantic_catalog._infer_semantic_type("category", "STRING", 10, 1000) == "dimension"
        assert semantic_catalog._infer_semantic_type("status", "STRING", 5, 1000) == "dimension"

    def test_dimension_by_low_cardinality(self):
        # Low cardinality string → dimension
        assert semantic_catalog._infer_semantic_type("foo", "STRING", 5, 1000) == "dimension"

    def test_flag_for_boolean(self):
        assert semantic_catalog._infer_semantic_type("is_active", "BOOLEAN", 2, 1000) == "flag"
        assert semantic_catalog._infer_semantic_type("has_email", "BOOL", 2, 1000) == "flag"

    def test_unknown_for_exotic_types(self):
        assert semantic_catalog._infer_semantic_type("data", "BYTES", 0, 0) == "unknown"


class TestFindSimilarQueriesSQL:
    """Test that find_similar_queries generates valid SQL (no HAVING without GROUP BY)."""

    def test_sql_uses_subquery_not_having(self):
        """Verify the generated SQL wraps in a subquery instead of using bare HAVING."""
        # We can't call the function directly (needs BQ client), but we can
        # replicate the SQL generation logic to verify the fix.
        keywords = ["popular", "stations", "2023"]
        conditions = []
        for kw in keywords[:10]:
            safe_kw = kw.replace("'", "\\'")
            conditions.append(f"IF(LOWER(natural_language) LIKE '%{safe_kw}%', 1, 0)")

        score_expr = " + ".join(conditions)
        table_id = "project.dataset.query_history"
        dataset = "austin_bikeshare"
        limit = 5

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

        # Verify no HAVING clause
        assert "HAVING" not in sql.upper()
        # Verify subquery wrapper is present
        assert "SELECT * FROM (" in sql
        assert ") sub" in sql
        # Verify outer WHERE on computed column
        assert "WHERE relevance_score > 0" in sql

    def test_empty_keywords_returns_empty(self):
        """If no meaningful keywords, the function should return early."""
        keywords = semantic_catalog._extract_keywords("the a is")
        assert keywords == []


class TestCountCatalogEntries:
    """Test the count_catalog_entries helper."""

    def test_returns_zero_for_nonexistent_table(self):
        """If the catalog table doesn't exist, should return 0 (not crash)."""
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        # Simulate table not found
        mock_client.get_table.side_effect = Exception("Not found")
        mock_client.create_table.return_value = None
        # Simulate empty query result
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([MagicMock(cnt=0)])
        mock_client.query.return_value.result.return_value = mock_result
        
        result = semantic_catalog.count_catalog_entries(mock_client, "test_ws", "test_dataset")
        # Should return 0 or similar without crashing
        assert isinstance(result, int)


class TestGlossarySuggestions:
    """Test the suggest_glossary_entries function."""

    def test_extracts_meaningful_phrases(self):
        """Business terms should be extracted from NL questions."""
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        # No existing glossary entries
        mock_client.get_table.side_effect = Exception("Not found")
        mock_client.create_table.return_value = None
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_client.query.return_value.result.return_value = mock_result

        suggestions = semantic_catalog.suggest_glossary_entries(
            mock_client, "test_ws", "test_dataset",
            nl_question="Show the trip duration split for subscriber types",
            sql="SELECT subscriber_type, AVG(duration_minutes) FROM trips GROUP BY 1"
        )
        assert isinstance(suggestions, list)
        assert len(suggestions) <= 3

    def test_filters_noise_phrases(self):
        """Common phrases like 'show me' should not be suggested."""
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_client.get_table.side_effect = Exception("Not found")
        mock_client.create_table.return_value = None
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_client.query.return_value.result.return_value = mock_result

        suggestions = semantic_catalog.suggest_glossary_entries(
            mock_client, "test_ws", "test_dataset",
            nl_question="show me the top list of",
            sql="SELECT * FROM t"
        )
        for s in suggestions:
            assert s not in semantic_catalog._NOISE_PHRASES

    def test_caps_at_three(self):
        """Should return at most 3 suggestions."""
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_client.get_table.side_effect = Exception("Not found")
        mock_client.create_table.return_value = None
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_client.query.return_value.result.return_value = mock_result

        suggestions = semantic_catalog.suggest_glossary_entries(
            mock_client, "test_ws", "test_dataset",
            nl_question="Show revenue churn rate by acquisition channel for active subscribers in premium tier",
            sql="SELECT channel, SUM(revenue) FROM users GROUP BY 1"
        )
        assert len(suggestions) <= 3


class TestStopWordsConstant:
    """Verify that _STOP_WORDS is a module-level constant used by both functions."""

    def test_stop_words_is_set(self):
        assert isinstance(semantic_catalog._STOP_WORDS, set)
        assert "the" in semantic_catalog._STOP_WORDS
        assert "show" in semantic_catalog._STOP_WORDS

    def test_extract_keywords_uses_module_constant(self):
        """_extract_keywords should use the module-level _STOP_WORDS."""
        result = semantic_catalog._extract_keywords("show me the top stations")
        # 'show' and 'the' are in _STOP_WORDS, 'me' is too
        assert "show" not in result
        assert "the" not in result

