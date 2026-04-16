import sys
import os
sys.path.append("/usr/local/google/home/larmanjat/dataagent")

from bq_tools import execute_query
from google.adk.tools.tool_context import ToolContext
import json

class MockState(dict):
    pass

class MockContext:
    def __init__(self):
        self.state = {}

ctx = MockContext()
res = execute_query("SELECT * FROM `antoine-279922.bqml_tutorial.themes` LIMIT 5", ctx)

print("Query Result length:", len(res))
if "pending_bq_a2ui" in ctx.state:
    print("A2UI Generated successfully! Components count:", len(ctx.state["pending_bq_a2ui"][0]["surfaceUpdate"]["components"]))
    with open("a2ui_dump.json", "w") as f:
        json.dump(ctx.state["pending_bq_a2ui"], f, indent=2)
else:
    print("Failed to generate A2UI payload.")
