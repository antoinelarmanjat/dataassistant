import os
import google.adk as adk
from google.adk.events.ui_widget import UiWidget
from google.adk.agents.callback_context import CallbackContext
from google.adk.tools.tool_context import ToolContext
from google.adk.models.llm_response import LlmResponse

# Configure for Vertex AI using ambient credentials (same as BQ Assistant)
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "true"
os.environ["GOOGLE_CLOUD_PROJECT"] = "antoine-279922"
os.environ["GOOGLE_CLOUD_LOCATION"] = "global"

# Define basic row definition to avoid boilerplate
def make_rows(text_label):
    return [
        {"type": "Row", "properties": {"distribution": "spaceBetween", "children": [
            {"type": "Text", "properties": {"text": text_label}}
        ]}}
    ]

# 1. Test payload as strict {"type", "properties"}
def test_strict_a2a_schema(tool_context: ToolContext = None) -> str:
    """Test 1: Use strict type/properties schema directly in payload."""
    payload = {
        "type": "List",
        "properties": {
            "direction": "vertical",
            "children": make_rows("Rendered Test 1")
        }
    }
    tool_context.render_ui_widget(UiWidget(id="test1", provider="a2ui", payload=payload))
    return "Test 1 executed."

# 2. Test payload wrapped in {"component": ...}
def test_wrapped_schema(tool_context: ToolContext = None) -> str:
    """Test 2: Wrap strict schema in {"component": ...}"""
    payload = {
        "component": {
            "type": "List",
            "properties": {
                "direction": "vertical",
                "children": make_rows("Rendered Test 2")
            }
        }
    }
    tool_context.render_ui_widget(UiWidget(id="test2", provider="a2ui", payload=payload))
    return "Test 2 executed."

# 3. Test generic {"List": ...} schema as documented historically
def test_generic_schema(tool_context: ToolContext = None) -> str:
    """Test 3: Generic A2UI without "type" property"""
    payload = {
        "List": {
            "direction": "vertical",
            "children": {"explicitList": [
                {"Row": {"distribution": "spaceBetween", "children": {"explicitList": [
                     {"Text": {"text": "Rendered Test 3"}}
                ]}}}
            ]}
        }
    }
    tool_context.render_ui_widget(UiWidget(id="test3", provider="a2ui", payload=payload))
    return "Test 3 executed."

# 4. Test wrapped generic {"component": {"List": ...}} schema 
def test_wrapped_generic_schema(tool_context: ToolContext = None) -> str:
    """Test 4: Generic A2UI wrapper without "type" property"""
    payload = {
        "component": {
            "List": {
                "direction": "vertical",
                "children": {"explicitList": [
                    {"Row": {"distribution": "spaceBetween", "children": {"explicitList": [
                         {"Text": {"text": "Rendered Test 4"}}
                    ]}}}
                ]}
            }
        }
    }
    tool_context.render_ui_widget(UiWidget(id="test4", provider="a2ui", payload=payload))
    return "Test 4 executed."

# 5. Test A2A Part Injection
def test_model_response_injection(tool_context: ToolContext = None) -> str:
    """Test 5: Inject A2A DataPart into the model response using special bytes encoding."""
    import json
    payload = [
        {
            "surfaceUpdate": {
                "surfaceId": "@default",
                "components": [
                    {
                        "id": "root_card",
                        "component": {
                            "Card": {
                                "title": { "literal": "Success" },
                                "child": "text_child"
                            }
                        }
                    },
                    {
                        "id": "text_child",
                        "component": {
                            "Text": {
                                "text": { "literal": "This is a dynamically injected A2UI payload test!" }
                            }
                        }
                    }
                ]
            }
        },
        {
            "beginRendering": {
                "surfaceId": "@default",
                "root": "root_card"
            }
        }
    ]
    tool_context.state['test_injection'] = payload
    tool_context.state['system:a2ui_enabled'] = True
    return "Test 5 executed. Callback will inject."

# 6. Test HTML table inside Text component
def test_html_table(tool_context: ToolContext = None) -> str:
    """Test 6: Inject an HTML table inside a Text component."""
    html_table = "<table border='1' cellpadding='5' cellspacing='0' style='width: 100%; border-collapse: collapse; text-align: left;'><thead><tr style='background-color: #f2f2f2;'><th>ID</th><th>Name</th><th>Role</th></tr></thead><tbody><tr><td>1</td><td>Alice</td><td>Admin</td></tr><tr><td>2</td><td>Bob</td><td>Editor</td></tr><tr><td>3</td><td>Charlie</td><td>Viewer</td></tr></tbody></table>"
    
    # We simply return the raw HTML. The LLM will output it, and the Markdown renderer should process it.
    return "Here is the HTML table: " + html_table

def widget_callback(callback_context: CallbackContext, llm_response: LlmResponse) -> LlmResponse:
    has_text = False
    if llm_response.content and hasattr(llm_response.content, 'parts'):
        has_text = any(hasattr(p, 'text') and p.text for p in llm_response.content.parts)
        
    if has_text and callback_context.state.get('test_injection'):
        payloads = callback_context.state.get('test_injection')
        callback_context.state['test_injection'] = None
        import json
        from google.genai import types as genai_types
        
        # Merge all stacked payloads from the tool context
        # payloads is a list of arrays (e.g. [[surfaceUpdate,...]])
        flattened_payloads = []
        for p in payloads:
            if isinstance(p, list):
                flattened_payloads.extend(p)
            else:
                flattened_payloads.append(p)
                
        # The frontend regex extracts the JSON and does n.a2ui.push(JSON.parse(...))
        # Since r.type==='data' usually pushes r.data, and r.data is an array of SurfaceUpdates,
        # we must serialize the array DIRECTLY.
        a2ui_json = json.dumps(flattened_payloads, separators=(',', ':'))
        
        # CRITICAL: Remove literal newlines, otherwise the browser JSON.parse throws SyntaxError!
        a2ui_json = a2ui_json.replace('\\n', '')
        
        magic_text = f"<a2a_datapart_json>{a2ui_json}</a2a_datapart_json>"
        magic_part = genai_types.Part.from_text(text=magic_text)
        
        # Add to model's visible output response
        llm_response.content.parts.append(magic_part)

    return llm_response

root_agent = adk.Agent(
    name="A2UITester",
    model="gemini-3-flash-preview",
    tools=[test_html_table],
    after_model_callback=widget_callback,
    instruction="You are a test agent. Greet the user, and ask them to test HTML tables with run `test_html_table`."
)

