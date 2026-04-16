try:
    from google.genai.types import Part
    print("Found in google.genai.types")
except ImportError:
    try:
        from vertexai.generative_models import Part
        print("Found in vertexai.generative_models")
    except ImportError:
        print("Part not found")
