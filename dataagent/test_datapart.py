import json
from a2a import types as a2a_types

payload = {
    "type": "List",
    "properties": {
        "direction": "vertical", 
        "children": [
            {"type": "Row", "properties": {"distribution": "spaceBetween", "children": [
                {"type": "Text", "properties": {"text": "Rendered Test 5 (Inline Blob DataPart)"}}
            ]}}
        ]
    }
}

a2ui_datapart = {
    "data": payload,
    "metadata": {
        "mimeType": "application/json+a2ui"
    }
}
a2ui_json = json.dumps(a2ui_datapart, separators=(',', ':')).encode('utf-8')

try:
    print("Testing model_validate_json...")
    res = a2a_types.DataPart.model_validate_json(a2ui_json)
    print("Success:", res)
except Exception as e:
    import traceback
    traceback.print_exc()
