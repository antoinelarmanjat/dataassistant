"""Tests for the __main__.py proxy handler and configuration.

Covers: tool-to-status mapping completeness and auth configuration.
"""
import sys
import os

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# We need to import just the mapping function without starting the server.
# Since __main__.py is a script with side effects, we extract the function.
# The simplest approach: define the expected mapping and test against it.


# --------------------------------------------------------------------------
# Tool-to-status mapping test
# --------------------------------------------------------------------------

class TestToolNameToStatusMapping:
    """Verify all important tools have spinner status mappings."""

    # All tool names used in agent.py that should have status messages
    EXPECTED_TOOLS = [
        # Dataset management
        'load_selected_datasets',
        'load_default_dataset',
        'save_selected_datasets',
        'remove_selected_datasets',
        'set_default_dataset',
        'scan_datasets',
        # Analysis
        'analyze_dataset',
        'load_dataset_analysis',
        'save_dataset_analysis',
        'profile_dataset',
        # Query
        'load_saved_queries',
        'save_query',
        'execute_query',
        'dry_run',
        # Semantic
        'load_semantic_context',
        'get_query_suggestions',
        'probe_column',
        'diagnose_query',
        'submit_feedback',
        'check_profiling_status',
        'start_background_profile',
        'force_reset_profiling_status',
        # Export
        'export_query_to_sheets',
        'export_query_to_gcs',
        # Charts
        'create_pie_chart',
        'create_bar_chart',
        # Sub-agents
        'QueryPlannerAgent',
        'WebSearchAgent',
    ]

    def _get_mapping(self):
        """Load the mapping from __main__.py without running the server."""
        # Import the module-level function by reading the source
        import importlib.util
        main_path = os.path.join(os.path.dirname(__file__), '..', '__main__.py')

        # Read and extract just the mapping function
        with open(main_path, 'r') as f:
            source = f.read()

        # Find and extract the mapping dict from _tool_name_to_status
        import re
        match = re.search(r"mapping\s*=\s*\{([^}]+)\}", source, re.DOTALL)
        assert match, "Could not find 'mapping = {...}' in __main__.py"

        # Parse the mapping
        mapping = {}
        for line in match.group(1).split('\n'):
            line = line.strip()
            if line.startswith('#') or not line or line.startswith('}'):
                continue
            kv_match = re.match(r"'([^']+)'\s*:\s*'([^']+)'", line)
            if kv_match:
                mapping[kv_match.group(1)] = kv_match.group(2)

        return mapping

    def test_all_agent_tools_have_status(self):
        """Every tool registered in agent.py should have a spinner status."""
        mapping = self._get_mapping()
        missing = []
        for tool in self.EXPECTED_TOOLS:
            if tool not in mapping:
                missing.append(tool)
        assert not missing, f"Missing spinner status for tools: {missing}"

    def test_status_messages_have_emoji(self):
        """All status messages should start with an emoji for visual consistency."""
        mapping = self._get_mapping()
        for tool, status in mapping.items():
            # Emoji characters are in the range above ASCII
            first_char = status[0] if status else ''
            assert ord(first_char) > 127, (
                f"Status for '{tool}' doesn't start with emoji: {status!r}"
            )

    def test_sub_agents_are_mapped(self):
        """Critical: sub-agent tool calls must be mapped for spinner updates."""
        mapping = self._get_mapping()
        assert 'QueryPlannerAgent' in mapping, "QueryPlannerAgent not mapped!"
        assert 'WebSearchAgent' in mapping, "WebSearchAgent not mapped!"

    def test_no_empty_status_values(self):
        """No tool should map to an empty string."""
        mapping = self._get_mapping()
        for tool, status in mapping.items():
            assert status.strip(), f"Empty status for tool: {tool}"


# --------------------------------------------------------------------------
# Auth configuration test
# --------------------------------------------------------------------------

class TestAuthConfiguration:
    """Verify auth-related configuration in __main__.py."""

    def test_auth_env_vars_are_read(self):
        """The auth flags should be read from environment variables."""
        main_path = os.path.join(os.path.dirname(__file__), '..', '__main__.py')
        with open(main_path, 'r') as f:
            source = f.read()

        assert 'AUTH_REQUIRED' in source
        assert 'IAP_ENABLED' in source
        assert 'OAUTH_CLIENT_ID' in source

    def test_session_db_is_configurable(self):
        """SESSION_DB env var should be used for session persistence."""
        main_path = os.path.join(os.path.dirname(__file__), '..', '__main__.py')
        with open(main_path, 'r') as f:
            source = f.read()

        assert 'SESSION_DB' in source
        assert 'DatabaseSessionService' in source
        assert 'sqlite' in source.lower()
