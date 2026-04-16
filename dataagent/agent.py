import asyncio
import os
from google import adk
from google.adk.runners import InMemoryRunner
from bq_tools import scan_datasets, save_selected_datasets, load_selected_datasets

# Ensure we are using the correct model
MODEL_NAME = "gemini-3-flash-preview"

# Configure for Vertex AI using ambient credentials
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "true"
os.environ["GOOGLE_CLOUD_PROJECT"] = "antoine-279922"
os.environ["GOOGLE_CLOUD_LOCATION"] = "global"

async def main():
    print(f"Initializing agent with model: {MODEL_NAME}")
    
    # Create the agent
    agent = adk.Agent(
        name="BigQueryAssistant",
        model=MODEL_NAME,
        instruction=(
            "You are a helpful assistant that helps users manage BigQuery datasets. "
            "1. At the VERY BEGINNING of the conversation, always use `load_selected_datasets` to check if the user has already saved datasets in their workspace. "
            "2. If datasets ARE found, greet the user with that list, and ask if they want to work on those or add more. "
            "3. If NO datasets are found, tell the user, and use `scan_datasets` to suggest lists to work on. "
            "4. Once the user selects datasets, use `save_selected_datasets` to store the selection. "
            "Be conversational and guide the user."
        ),
        tools=[scan_datasets, save_selected_datasets, load_selected_datasets]
    )
    
    # Create a runner
    # Note: InMemoryRunner handles session state automatically for debugging
    runner = InMemoryRunner(agent=agent)
    
    print("\n--- Starting Conversation ---")
    
    # Initial trigger
    print("User: Scan my datasets")
    events = await runner.run_debug("Scan my datasets", verbose=True)
    
    print("\n--- Agent Response ---")
    for event in events:
        if hasattr(event, 'text') and event.text:
             print(event.text)
        elif hasattr(event, 'content') and event.content:
             print(event.content)
             
    # TURN 2: Simulate user making a selection
    print("\n--- Next Turn ---")
    print("User: Save my selection: 'antoine-279922.demo_dataset'")
    events = await runner.run_debug("Save my selection: 'antoine-279922.demo_dataset'", verbose=True)
    
    print("\n--- Agent Response ---")
    for event in events:
        if hasattr(event, 'text') and event.text:
             print(event.text)
        elif hasattr(event, 'content') and event.content:
             print(event.content)

if __name__ == "__main__":
    asyncio.run(main())
