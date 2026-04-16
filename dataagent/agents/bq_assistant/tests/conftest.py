"""Shared test fixtures for BQ Assistant test suite."""
import os
import sys
import pytest

# Add the BQ Assistant module paths so tests can import them
_bq_assistant_dir = os.path.join(os.path.dirname(__file__), '..')
_dataagent_dir = os.path.join(_bq_assistant_dir, '..', '..')
sys.path.insert(0, _bq_assistant_dir)
sys.path.insert(0, _dataagent_dir)

# Set required env vars before any module imports
os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "true")
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "antoine-exp")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "global")
os.environ.setdefault("DATA_PROJECT", "octo-aif-sandbox")
