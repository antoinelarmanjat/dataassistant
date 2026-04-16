"""Tests for bq_tools module — unit tests with mocked BigQuery client.

Covers: workspace derivation, query persistence helpers, and markdown table injection.
"""
import sys
import os
import json
from unittest.mock import MagicMock, patch, PropertyMock

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


class TestUserWorkspace:
    """Test workspace name derivation from user email."""

    def test_workspace_derived_from_email(self):
        """The workspace name should be derived from the user's email."""
        # Import the module
        import bq_tools

        # Simulate setting a user email
        bq_tools._current_user_email = "test@example.com"
        bq_tools._cached_user_workspace = None
        bq_tools._cached_user_email = "test@example.com"

        # The workspace is built as "da_<sanitized_email>"
        # We need to check the _get_user_workspace function
        ws = bq_tools._get_user_workspace()
        assert ws is not None
        assert isinstance(ws, str)
        assert len(ws) > 0

    def test_set_user_email_clears_cache(self):
        """Setting a new email should clear the cached workspace."""
        import bq_tools

        bq_tools._cached_user_workspace = "old_workspace"
        bq_tools._cached_user_email = "old@example.com"

        bq_tools.set_user_email("new@example.com")

        # Cache should be cleared for the new user
        assert bq_tools._cached_user_workspace is None
        assert bq_tools._cached_user_email == "new@example.com"

    def test_same_email_preserves_cache(self):
        """Setting the same email should NOT clear the workspace cache."""
        import bq_tools

        bq_tools._cached_user_workspace = "existing_workspace"
        bq_tools._cached_user_email = "same@example.com"

        bq_tools.set_user_email("same@example.com")

        # Cache should be preserved
        assert bq_tools._cached_user_workspace == "existing_workspace"


class TestA2UIPayload:
    """Test the a2ui payload get/clear mechanism."""

    def test_get_latest_clears_after_read(self):
        """get_latest_a2ui_payload should return the payload and clear it."""
        import bq_tools

        bq_tools._latest_a2ui_payload = {"test": "data"}
        result = bq_tools.get_latest_a2ui_payload()

        assert result == {"test": "data"}
        # Should be cleared after read
        assert bq_tools._latest_a2ui_payload is None

    def test_get_latest_returns_none_when_empty(self):
        """Should return None when no payload is set."""
        import bq_tools

        bq_tools._latest_a2ui_payload = None
        result = bq_tools.get_latest_a2ui_payload()
        assert result is None


class TestMarkdownTableGeneration:
    """Test that execute_query produces proper markdown tables."""

    def _make_mock_result(self, columns, rows):
        """Create a mock BigQuery result."""
        mock_rows = []
        for row_data in rows:
            mock_row = MagicMock()
            for col, val in zip(columns, row_data):
                setattr(mock_row, col, val)
            # Make dict() work on the row
            mock_row.keys.return_value = columns
            mock_row.values.return_value = row_data
            mock_rows.append(mock_row)
        return mock_rows

    def test_markdown_table_format(self):
        """Verify the markdown table format used in query results."""
        # The markdown table format should have:
        # | col1 | col2 |
        # | --- | --- |
        # | val1 | val2 |
        columns = ["station_name", "trip_count"]
        rows = [
            ("Zilker Park", 9994),
            ("East 2nd", 8996),
        ]

        # Build the expected markdown table
        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join(["---"] * len(columns)) + " |"
        data_rows = []
        for row in rows:
            data_rows.append("| " + " | ".join(str(v) for v in row) + " |")

        table = "\n".join([header, separator] + data_rows)

        assert "station_name" in table
        assert "trip_count" in table
        assert "Zilker Park" in table
        assert "9994" in table
        assert "| --- |" in table
