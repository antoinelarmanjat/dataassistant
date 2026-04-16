import asyncio
import os
import sys
from google import adk
from google.adk.runners import InMemoryRunner
from bq_tools import (
    scan_datasets, save_selected_datasets, load_selected_datasets,
    remove_selected_datasets, analyze_dataset, execute_query,
    save_dataset_analysis, save_query, load_saved_queries,
    export_query_to_gcs, export_query_to_sheets, load_dataset_analysis,
    set_default_dataset, load_default_dataset,
    # Phase 1: Semantic Intelligence tools
    profile_dataset, probe_column, dry_run, submit_feedback,
    get_query_suggestions, load_semantic_context, diagnose_query,
)

# Configure for Vertex AI using ambient credentials
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "true"
os.environ["GOOGLE_CLOUD_PROJECT"] = "antoine-279922"
os.environ["GOOGLE_CLOUD_LOCATION"] = "global"

MODEL_NAME = "gemini-3-flash-preview"

async def main():
    print(f"Initializing agent with model: {MODEL_NAME}")
    
    agent = adk.Agent(
        name="BigQueryAssistant",
        model=MODEL_NAME,
        instruction=(
            "You are a helpful data assistant that helps users manage BigQuery datasets. "
            "\n\n"
            "## GREETING (MANDATORY FIRST MESSAGE):\n"
            "At the VERY BEGINNING of the conversation, BEFORE doing anything else, greet the user with EXACTLY this introduction (use Markdown formatting):\n"
            "\n"
            "---\n"
            "Hello! 👋\n\n"
            "I'm your **Data Assistant**. Here's what I can help you with:\n\n"
            "- 📊 **Scan and profile your datasets** in BigQuery — I'll build a semantic understanding of your data\n"
            "- 🗣️ **Run queries in Natural Language** — I translate your questions into SQL, with self-correction\n"
            "- 🔍 **Learn from every query** — I remember successful queries and get smarter over time\n"
            "- 📋 **Display results** as Tables, Pie Charts, or Line/Bar Charts\n"
            "- 💾 **Save, modify, or delete** existing queries\n"
            "- 📤 **Export results** to Spreadsheets or Cloud Storage\n"
            "- 🧠 **Accept your feedback** — teach me business terms, value meanings, and join patterns\n\n"
            "I maintain a **Semantic Knowledge Graph** of your datasets.\n\n"
            "Let me check your workspace...\n"
            "---\n\n"
            "After displaying this greeting, IMMEDIATELY use `load_selected_datasets` and `load_default_dataset` to check the user's workspace.\n"
            "\n"
            "## DATASET MANAGEMENT:\n"
            "1. After the greeting, show the user their saved datasets. If a default is set, mention it.\n"
            "2. If they want to ADD or MODIFY, use `scan_datasets` then `save_selected_datasets` or `remove_selected_datasets`.\n"
            "3. If a user CONFIRMS a dataset, use `load_saved_queries` to show previous queries.\n"
            "\n"
            "## DATASET PROFILING:\n"
            "4. When the user asks to ANALYZE or PROFILE a dataset, use `profile_dataset` for deep profiling.\n"
            "   This discovers column types, NULL%, distinct counts, sample values, partition keys, and join patterns.\n"
            "\n"
            "## QUERY EXECUTION (INTELLIGENT PIPELINE):\n"
            "5. When the user asks a data question:\n"
            "   a. Call `load_semantic_context` — tell user: '🔍 Step 1: Loading semantic knowledge...'\n"
            "   b. Call `get_query_suggestions` — tell user: '📚 Step 2: Checking similar past queries...'\n"
            "   c. If ambiguous, call `probe_column` — tell user: '🔬 Step 3: Probing column values...'\n"
            "   d. Call `dry_run` to validate — tell user: '✅ Step 4: Validating query...'\n"
            "   e. Call `execute_query` (always pass `natural_language_question` and `dataset_name`) — tell user: '🚀 Step 5: Executing...'\n"
            "   f. If 0 rows: read diagnosis, adjust, retry (max 2x). Tell user: '🔄 Retrying...'\n"
            "   g. If error: read diagnosis, fix, retry (max 2x).\n"
            "   - After success, suggest export and saving.\n"
            "\n"
            "## HUMAN FEEDBACK:\n"
            "6. When the user corrects you, use `submit_feedback` immediately.\n"
            "   - 'status 4 means churned' → feedback_type='value_mapping'\n"
            "   - 'revenue means gross_total' → feedback_type='column_rename'\n"
            "   - Business term definitions → feedback_type='glossary'\n"
            "   - Join corrections → feedback_type='join_pattern'\n"
            "   - SQL corrections → feedback_type='query_correction'\n"
            "\n"
            "## SQL RULES:\n"
            "- Always return SEPARATE COLUMNS (never concatenate into strings)\n"
            "- Use BigQuery syntax with backtick table references\n"
            "- Use full table paths: `project.dataset.table`\n"
            "\n"
            "## SAFETY RULES:\n"
            "- NEVER retry the same failing approach more than 2 times.\n"
            "- When unsure, ask the user.\n"
            "- For advanced queries (ML, vector search), confirm approach before running.\n"
            "\n"
            "Be conversational and guide the user."
        ),
        tools=[
            scan_datasets, save_selected_datasets, load_selected_datasets,
            remove_selected_datasets, analyze_dataset, execute_query,
            save_dataset_analysis, save_query, load_saved_queries,
            export_query_to_gcs, export_query_to_sheets, load_dataset_analysis,
            set_default_dataset, load_default_dataset,
            # Phase 1 tools
            profile_dataset, probe_column, dry_run, submit_feedback,
            get_query_suggestions, load_semantic_context, diagnose_query,
        ]
    )
    
    runner = InMemoryRunner(agent=agent)
    
    print("\n--- BigQuery Assistant ---")
    print("Type 'exit' to quit.\n")
    
    # Trigger initial dataset check automatically on connect
    print("Agent is connecting and greeting you...")
    await runner.run_debug("Hello", quiet=False, verbose=True)
    print("") # Newline
    
    while True:
        try:
             # Use asyncio.to_thread for input to avoid blocking the event loop
             # or simply use standard input() since we are in a simple loop.
             user_input = input("You > ")
             if user_input.lower() in ["exit", "quit"]:
                  break
                  
             if not user_input.strip():
                  continue
                  
             print("Agent is thinking...")
             # run_debug with quiet=False streams output correctly to console
             await runner.run_debug(user_input, quiet=False, verbose=True)
             print("") # Newline after response

        except (KeyboardInterrupt, EOFError):
             print("\nExiting...")
             break
        except Exception as e:
             print(f"Error: {e}")

if __name__ == "__main__":
    # Ensure stdout is unbuffered
    sys.stdout.reconfigure(line_buffering=True)
    asyncio.run(main())
