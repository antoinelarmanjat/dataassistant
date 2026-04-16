import os
import sys
import logging
import json
import uuid
import re
from datetime import datetime, timezone
import click
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
from starlette.requests import Request
from starlette.responses import StreamingResponse, JSONResponse, RedirectResponse
from starlette.routing import Route
from a2a.server.apps import A2AStarletteApplication
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import AgentCard, AgentCapabilities
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks import TaskUpdater
from a2a.types import TaskState, TextPart, Part, DataPart
from a2a.utils import new_agent_parts_message, new_agent_text_message, new_task
from google.adk.agents import run_config

from google.adk.runners import Runner
from google.adk.artifacts import InMemoryArtifactService
from google.adk.sessions import DatabaseSessionService
from google.adk.memory.in_memory_memory_service import InMemoryMemoryService
import uvicorn

from agent import root_agent
from bq_tools import set_user_email, get_latest_a2ui_payload

# Add parent dir to path so we can import shared modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from user_credentials import (
    credential_store, build_oauth_authorize_url,
    exchange_code_for_credentials, OAUTH_DATA_CLIENT_ID,
)

# --- Auth Configuration ---
# AUTH_REQUIRED: if true, authentication is mandatory (either IAP or OAuth).
# IAP_ENABLED: if true (default in Cloud Run), trust IAP headers for user identity.
#              if false (local dev), fall back to Google Sign-In OAuth token verification.
AUTH_REQUIRED = os.environ.get("AUTH_REQUIRED", "true").lower() == "true"
IAP_ENABLED = os.environ.get("IAP_ENABLED", "false").lower() == "true"
OAUTH_CLIENT_ID = "845556473362-hn577kpi7nco8muojdsv09svttdcjd9s.apps.googleusercontent.com"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _email_to_user_id(email: str | None) -> str:
    """Convert an email address to a stable ADK user_id.
    
    e.g. 'antoine@google.com' -> 'user_antoine_google_com'
    Falls back to 'anonymous' for local dev without auth.
    """
    if not email:
        return "anonymous"
    safe = re.sub(r'[^a-zA-Z0-9]', '_', email.lower())
    return f"user_{safe}"


def _extract_iap_user_email(request: Request) -> str | None:
    """Extract user email from IAP-injected headers.
    
    When IAP is enabled, Cloud Run receives:
    - X-Goog-Authenticated-User-Email: accounts.google.com:<email>
    - X-Goog-Authenticated-User-Id: accounts.google.com:<id>
    - X-Goog-IAP-JWT-Assertion: <signed JWT>
    """
    iap_email_header = request.headers.get('x-goog-authenticated-user-email', '')
    if iap_email_header:
        # Format is "accounts.google.com:user@google.com" — strip the prefix
        email = iap_email_header.split(':', 1)[-1] if ':' in iap_email_header else iap_email_header
        logger.info(f"[IAP Auth] User from IAP header: {email}")
        return email
    return None


def _get_auth_email(request: Request) -> str | None:
    """Unified auth: try IAP headers first (if IAP_ENABLED), then fall back to OAuth token."""
    if IAP_ENABLED:
        email = _extract_iap_user_email(request)
        if email:
            return email
        logger.warning("[Auth] IAP_ENABLED but no IAP headers found")
        return None
    
    # Fall back to OAuth token verification (local dev mode)
    return _verify_auth_token(request.headers.get('authorization', ''))


def _verify_auth_token(auth_header: str):
    """Verify the OAuth ID token and return the email, or None."""
    if not auth_header:
        return None
    token = auth_header.replace('Bearer ', '') if auth_header.startswith('Bearer ') else auth_header
    if not token:
        return None
    try:
        from google.oauth2 import id_token
        from google.auth.transport import requests as google_requests
        idinfo = id_token.verify_oauth2_token(
            token, google_requests.Request(), OAUTH_CLIENT_ID
        )
        email = idinfo.get('email', '')
        logger.info(f"[Auth] Verified OAuth user: {email}")
        return email
    except Exception as e:
        logger.warning(f"[Auth] Token verification failed: {e}")
        return None


async def auth_user_handler(request: Request):
    """GET /auth/user — Returns the auth mode, user info, and data authorization status.
    
    The frontend calls this to discover:
    1. Whether IAP is active (no Google Sign-In needed) or local mode (use Google Sign-In)
    2. If IAP, the currently authenticated user email
    3. Whether the user has granted data access scopes (BigQuery, Sheets, etc.)
    """
    if IAP_ENABLED:
        email = _extract_iap_user_email(request)
        # When USER_OAUTH_ENABLED, check if user has granted data access.
        # Otherwise (SA mode), data access is always available via SA.
        user_oauth_enabled = os.environ.get("USER_OAUTH_ENABLED", "false").lower() == "true"
        if user_oauth_enabled:
            data_authorized = credential_store.has(email) if email else False
        else:
            data_authorized = True  # SA handles all data operations
        return JSONResponse({
            "mode": "iap",
            "authenticated": email is not None,
            "email": email or "",
            "dataAuthorized": data_authorized,
            "dataAuthAvailable": user_oauth_enabled and bool(OAUTH_DATA_CLIENT_ID),
        })
    else:
        return JSONResponse({
            "mode": "oauth",
            "authenticated": False,
            "email": "",
            "oauth_client_id": OAUTH_CLIENT_ID,
            "dataAuthorized": True,  # Local dev uses ADC — always authorized
        })


def _get_public_base_url(request: Request) -> str:
    """Derive the public-facing base URL from proxy headers.
    
    Behind IAP / Cloud Run load balancer, request.base_url returns
    http://localhost:8080. We need the actual public URL for OAuth redirects.
    """
    proto = request.headers.get('x-forwarded-proto', 'https')
    host = request.headers.get('host', '')
    if host:
        return f"{proto}://{host}"
    # Fallback to request.base_url
    return str(request.base_url).rstrip('/')


async def auth_authorize_handler(request: Request):
    """GET /auth/authorize — Redirects to Google OAuth consent for data access.
    
    Only used in IAP mode. The user clicks 'Grant Data Access' and is redirected
    to Google's OAuth consent screen to approve BigQuery/Sheets/Drive/GCS scopes.
    """
    if not IAP_ENABLED:
        return JSONResponse({"error": "OAuth data consent only available in IAP mode"}, status_code=400)
    
    email = _extract_iap_user_email(request)
    if not email:
        return JSONResponse({"error": "Not authenticated via IAP"}, status_code=401)
    
    # Build redirect URI using the public-facing URL (not internal base_url)
    base_url = _get_public_base_url(request)
    redirect_uri = base_url + '/auth/callback'
    
    # Use email as state to correlate the callback with the user
    authorize_url = build_oauth_authorize_url(
        redirect_uri=redirect_uri,
        state=email,
    )
    
    logger.info(f"[OAuth Data] Redirecting {email} to consent screen (redirect_uri={redirect_uri})")
    return RedirectResponse(url=authorize_url)


async def auth_callback_handler(request: Request):
    """GET /auth/callback — Handles the OAuth callback after user grants consent.
    
    Google redirects here with ?code=xxx&state=email after the user approves.
    We exchange the code for tokens and store them in memory.
    """
    code = request.query_params.get('code')
    state_email = request.query_params.get('state', '')
    error = request.query_params.get('error')
    
    if error:
        logger.warning(f"[OAuth Data] User denied consent: {error}")
        return RedirectResponse(url='/?auth_error=denied')
    
    if not code:
        return JSONResponse({"error": "Missing authorization code"}, status_code=400)
    
    # Verify the user is still authenticated via IAP
    iap_email = _extract_iap_user_email(request)
    if not iap_email:
        return JSONResponse({"error": "Not authenticated via IAP"}, status_code=401)
    
    # Security check: the state (email from authorize) should match the IAP email
    if state_email and state_email != iap_email:
        logger.warning(f"[OAuth Data] State mismatch: state={state_email}, IAP={iap_email}")
        return JSONResponse({"error": "Authentication mismatch"}, status_code=403)
    
    try:
        base_url = _get_public_base_url(request)
        redirect_uri = base_url + '/auth/callback'
        credentials = exchange_code_for_credentials(code, redirect_uri)
        credential_store.store(iap_email, credentials)
        logger.info(f"[OAuth Data] Successfully stored credentials for {iap_email}")
        return RedirectResponse(url='/?data_auth=success')
    except Exception as e:
        logger.error(f"[OAuth Data] Token exchange failed: {e}")
        return RedirectResponse(url='/?auth_error=token_exchange_failed')


async def auth_revoke_handler(request: Request):
    """POST /auth/revoke — Revokes the user's data access tokens."""
    if not IAP_ENABLED:
        return JSONResponse({"error": "Not in IAP mode"}, status_code=400)
    
    email = _extract_iap_user_email(request)
    if email:
        credential_store.remove(email)
        logger.info(f"[OAuth Data] Revoked credentials for {email}")
    return JSONResponse({"ok": True})


class SimpleBqExecutor(AgentExecutor):
    def __init__(self, runner):
        self._runner = runner
        
    async def execute(self, context: RequestContext, event_queue: EventQueue):
        if context.message and context.message.parts:
            # Extract query from message
            parts = context.message.parts
            query = ""
            auth_email = None
            for p in parts:
                if isinstance(p.root, TextPart):
                    query += p.root.text
                elif isinstance(p.root, DataPart) and 'request' in p.root.data:
                    query += p.root.data['request']
            
            # Extract the auth token from message metadata
            if context.message.metadata and 'authorization' in context.message.metadata:
                auth_email = _verify_auth_token(context.message.metadata['authorization'])
                if not auth_email and AUTH_REQUIRED:
                    task = context.current_task
                    if not task:
                        task = new_task(context.message)
                        await event_queue.enqueue_event(task)
                    updater = TaskUpdater(event_queue, task.id, task.context_id)
                    await updater.update_status(
                        TaskState.completed,
                        new_agent_text_message("Authentication failed. Please sign in again.", task.context_id, task.id),
                        final=True
                    )
                    return
            elif AUTH_REQUIRED:
                logger.warning("[Auth] No authorization header found and AUTH_REQUIRED=true")
            
            # Set the user email in bq_tools so workspace is derived correctly
            set_user_email(auth_email)
            
            task = context.current_task
            if not task:
                task = new_task(context.message)
                await event_queue.enqueue_event(task)
                
            updater = TaskUpdater(event_queue, task.id, task.context_id)
            
            from google.genai import types
            msg = types.Content(role="user", parts=[types.Part.from_text(text=query)])
            
            session_id = context.context_id if context.context_id else "default"
            session = await self._runner.session_service.get_session(
                app_name=self._runner.app_name,
                user_id="remote_user",
                session_id=session_id,
            )
            if session is None:
                await self._runner.session_service.create_session(
                    app_name=self._runner.app_name,
                    user_id="remote_user",
                    state={},
                    session_id=session_id,
                )
            
            # Inject the authenticated user's email into session state
            if auth_email:
                try:
                    actual_session = self._runner.session_service.sessions[self._runner.app_name]["remote_user"][session_id]
                    actual_session.state['user_email'] = auth_email
                    logger.info(f"[Auth] Set session user_email={auth_email}")
                except Exception as e:
                    logger.warning(f"[Auth] Could not set user_email in session: {e}")
            
            
            final_text = ""
            
            async for event in self._runner.run_async(
                user_id="remote_user",
                session_id=session_id,
                run_config=run_config.RunConfig(
                    streaming_mode=run_config.StreamingMode.SSE,
                    max_llm_calls=30,
                ),
                new_message=msg
            ):
                if event.content and event.content.parts:
                    current_text = ""
                    for p in event.content.parts:
                        if hasattr(p, 'text') and p.text:
                            current_text += p.text
                    final_text = current_text
                                
            final_parts = []
            
            # Check for direct state instead of scraping text
            updated_session = await self._runner.session_service.get_session(
                app_name=self._runner.app_name,
                user_id="remote_user",
                session_id=session_id,
            )
            a2ui_payload = updated_session.state.get('pending_bq_a2ui')
            if a2ui_payload:
                final_parts.extend([Part(root=DataPart(data=pld, metadata={"mimeType": "application/json+a2ui"})) for pld in a2ui_payload])
                print("DEBUG: Successfully extracted a2ui_payload from state and appended DataPart!")
                
                # Clear the state manually from the in-memory map
                try:
                    actual_session = self._runner.session_service.sessions[self._runner.app_name]["remote_user"][session_id]
                    actual_session.state['pending_bq_a2ui'] = None
                except Exception as e:
                    logger.warning(f"Failed to clear state: {e}")
                
            if "<a2a_datapart_json>" in final_text:
                try:
                    json_part = final_text.split("<a2a_datapart_json>")[1].split("</a2a_datapart_json>")[0]
                    parsed = json.loads(json_part)
                    if not a2ui_payload:
                        final_parts.extend([Part(root=DataPart(data=pld, metadata={"mimeType": "application/json+a2ui"})) for pld in parsed])
                    final_text = final_text.replace(f"<a2a_datapart_json>{json_part}</a2a_datapart_json>", "")
                except Exception as e:
                    logger.error(f"Failed to parse injected A2UI Data: {e}")
                    
            if final_text.strip():
                final_parts.insert(0, Part(root=TextPart(text=final_text)))
                
            if not final_parts:
                final_parts.append(Part(root=TextPart(text="Task completed without output.")))
                
            await updater.update_status(TaskState.completed, new_agent_parts_message(final_parts, task.context_id, task.id), final=True)
            
    async def cancel(self, request, event_queue):
        pass


# --- Direct /a2a proxy route (replaces Vite middleware for production) ---
# This is a module-level reference, set by main() before the server starts.
_runner: Runner | None = None

async def _run_agent_and_collect(query: str, auth_email: str | None, session_id: str):
    """Run the agent with the given query and return (final_text, a2ui_payloads)."""
    from google.genai import types
    msg = types.Content(role="user", parts=[types.Part.from_text(text=query)])

    # Set user email for workspace resolution
    set_user_email(auth_email)

    session = await _runner.session_service.get_session(
        app_name=_runner.app_name,
        user_id="remote_user",
        session_id=session_id,
    )
    if session is None:
        await _runner.session_service.create_session(
            app_name=_runner.app_name,
            user_id="remote_user",
            state={},
            session_id=session_id,
        )

    # Inject user email into session state
    if auth_email:
        try:
            actual_session = _runner.session_service.sessions[_runner.app_name]["remote_user"][session_id]
            actual_session.state['user_email'] = auth_email
        except Exception:
            pass

    final_text = ""
    async for event in _runner.run_async(
        user_id="remote_user",
        session_id=session_id,
        run_config=run_config.RunConfig(
            streaming_mode=run_config.StreamingMode.SSE,
            max_llm_calls=30,
        ),
        new_message=msg
    ):
        if event.content and event.content.parts:
            current_text = ""
            for p in event.content.parts:
                if hasattr(p, 'text') and p.text:
                    current_text += p.text
            final_text = current_text

    # Gather a2ui payloads from session state
    updated_session = await _runner.session_service.get_session(
        app_name=_runner.app_name,
        user_id="remote_user",
        session_id=session_id,
    )
    a2ui_payloads = updated_session.state.get('pending_bq_a2ui') or []
    if a2ui_payloads:
        try:
            actual_session = _runner.session_service.sessions[_runner.app_name]["remote_user"][session_id]
            actual_session.state['pending_bq_a2ui'] = None
        except Exception:
            pass

    # Clean inline a2ui markers from text
    if "<a2a_datapart_json>" in final_text:
        try:
            json_part = final_text.split("<a2a_datapart_json>")[1].split("</a2a_datapart_json>")[0]
            parsed = json.loads(json_part)
            if not a2ui_payloads:
                a2ui_payloads = parsed
            final_text = final_text.replace(f"<a2a_datapart_json>{json_part}</a2a_datapart_json>", "")
        except Exception:
            pass

    return final_text, a2ui_payloads


def _tool_name_to_status(tool_name: str) -> str:
    """
    Map an ADK function_call tool name to a user-friendly spinner status.
    Called when the agent starts calling a tool.
    """
    mapping = {
        # Workspace / dataset management
        'load_selected_datasets': '📂 Loading your workspace...',
        'load_default_dataset': '📂 Loading your workspace...',
        'save_selected_datasets': '💾 Saving workspace settings...',
        'remove_selected_datasets': '🗑️ Updating workspace...',
        'set_default_dataset': '📌 Setting default dataset...',
        'scan_datasets': '🔎 Scanning available datasets...',
        
        # Dataset analysis
        'analyze_dataset': '🧠 Analyzing dataset structure...',
        'load_dataset_analysis': '📋 Loading dataset insights...',
        'save_dataset_analysis': '💾 Saving analysis...',
        'profile_dataset': '📊 Deep profiling dataset...',
        
        # Query operations
        'load_saved_queries': '📚 Loading your saved queries...',
        'save_query': '💾 Saving query...',
        'execute_query': '🚀 Executing query...',
        'dry_run': '✅ Validating query syntax...',
        
        # Semantic catalog
        'load_semantic_context': '🧠 Loading semantic knowledge...',
        'get_query_suggestions': '📚 Finding similar past queries...',
        'probe_column': '🔬 Probing column values...',
        'diagnose_query': '🔍 Diagnosing query issues...',
        'submit_feedback': '📝 Recording your feedback...',
        'check_profiling_status': '🔄 Checking profiling status...',
        'start_background_profile': '🚀 Starting background profiling...',
        'force_reset_profiling_status': '🔄 Resetting profiling status...',
        
        # Export
        'export_query_to_sheets': '📤 Exporting to Google Sheets...',
        'export_query_to_gcs': '📤 Exporting to Cloud Storage...',
        
        # Charts
        'create_pie_chart': '📊 Building pie chart...',
        'create_bar_chart': '📊 Building bar chart...',
        
        # Web / Import
        'web_search_and_import': '🌐 Searching and importing web data...',
        'import_gcs_to_bigquery': '📥 Importing from Cloud Storage...',
        
        # Sub-agents (AgentTool calls appear as function_call with the agent name)
        'QueryPlannerAgent': '🧠 Planning and executing query...',
        'WebSearchAgent': '🌐 Searching the web...',
    }
    return mapping.get(tool_name, '')


def _tool_response_to_status(tool_name: str) -> str:
    """
    Map an ADK function_response tool name to a user-friendly 'processing' status.
    Called when a tool returns results and the agent is thinking about next steps.
    """
    mapping = {
        'load_selected_datasets': '🤔 Reviewing your datasets...',
        'load_default_dataset': '🤔 Reviewing your datasets...',
        'scan_datasets': '🤔 Processing dataset list...',
        'analyze_dataset': '🤔 Reviewing analysis results...',
        'load_dataset_analysis': '🤔 Reviewing dataset insights...',
        'profile_dataset': '🤔 Processing profile results...',
        'load_saved_queries': '🤔 Reviewing saved queries...',
        'execute_query': '📋 Processing query results...',
        'dry_run': '🤔 Reviewing validation results...',
        'load_semantic_context': '🤔 Processing semantic context...',
        'get_query_suggestions': '🤔 Evaluating suggestions...',
        'probe_column': '🤔 Analyzing column data...',
        'diagnose_query': '🤔 Reviewing diagnosis...',
    }
    return mapping.get(tool_name, '')


# ---------------------------------------------------------------------------
# Session management endpoints
# ---------------------------------------------------------------------------

async def list_sessions_handler(request: Request):
    """GET /sessions — List the user's conversations."""
    auth_email = _get_auth_email(request)
    if not auth_email and AUTH_REQUIRED:
        return JSONResponse({"error": "Auth required"}, status_code=401)

    user_id = _email_to_user_id(auth_email)
    logger.info(f"[Sessions] Listing sessions for user_id={user_id} (email={auth_email})")
    try:
        response = await _runner.session_service.list_sessions(
            app_name=_runner.app_name, user_id=user_id
        )
    except Exception as e:
        logger.error(f"[Sessions] list_sessions failed: {e}")
        return JSONResponse({"conversations": []})

    logger.info(f"[Sessions] Found {len(response.sessions)} sessions")
    conversations = []
    for session in response.sessions:
        title = session.state.get("conversation_title", "New conversation")
        last_active = session.last_update_time
        logger.info(f"[Sessions]   - {session.id[:12]}... title='{title}' last_active={last_active} (type={type(last_active).__name__})")
        conversations.append({
            "id": session.id,
            "title": title,
            "last_active": last_active,
            "default_dataset": session.state.get("default_dataset"),
        })

    # Sort by last_active descending (most recent first)
    conversations.sort(key=lambda c: c["last_active"] or 0, reverse=True)
    return JSONResponse({"conversations": conversations})


async def create_session_handler(request: Request):
    """POST /sessions — Create a new conversation."""
    auth_email = _get_auth_email(request)
    if not auth_email and AUTH_REQUIRED:
        return JSONResponse({"error": "Auth required"}, status_code=401)

    user_id = _email_to_user_id(auth_email)
    session_id = str(uuid.uuid4())

    session = await _runner.session_service.create_session(
        app_name=_runner.app_name,
        user_id=user_id,
        state={
            "conversation_title": "New conversation",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "user_email": auth_email or "",
        },
        session_id=session_id,
    )
    logger.info(f"[Sessions] Created session {session.id} for {user_id}")
    return JSONResponse({"session_id": session.id})


async def get_session_handler(request: Request):
    """GET /sessions/{session_id} — Load conversation history for resume."""
    session_id = request.path_params["session_id"]
    auth_email = _get_auth_email(request)
    if not auth_email and AUTH_REQUIRED:
        return JSONResponse({"error": "Auth required"}, status_code=401)

    user_id = _email_to_user_id(auth_email)

    session = await _runner.session_service.get_session(
        app_name=_runner.app_name,
        user_id=user_id,
        session_id=session_id,
    )
    if not session:
        return JSONResponse({"error": "Not found"}, status_code=404)

    # Extract user/assistant message pairs from events
    messages = []
    for event in session.events:
        if not event.content or not event.content.parts:
            continue

        text = ""
        for part in event.content.parts:
            if hasattr(part, 'text') and part.text:
                text += part.text
            # Skip tool calls / function responses — internal
            if hasattr(part, 'function_call') and part.function_call:
                text = ""  # Don't surface tool-call events
                break
            if hasattr(part, 'function_response') and part.function_response:
                text = ""
                break

        if text.strip():
            messages.append({
                "role": event.content.role,  # "user" or "model"
                "text": text,
            })

    return JSONResponse({
        "session_id": session.id,
        "title": session.state.get("conversation_title", "Untitled"),
        "created_at": session.state.get("created_at"),
        "messages": messages,
    })


async def delete_session_handler(request: Request):
    """DELETE /sessions/{session_id} — Delete a conversation."""
    session_id = request.path_params["session_id"]
    auth_email = _get_auth_email(request)
    if not auth_email and AUTH_REQUIRED:
        return JSONResponse({"error": "Auth required"}, status_code=401)

    user_id = _email_to_user_id(auth_email)
    try:
        await _runner.session_service.delete_session(
            app_name=_runner.app_name,
            user_id=user_id,
            session_id=session_id,
        )
        logger.info(f"[Sessions] Deleted session {session_id}")
    except Exception as e:
        logger.error(f"[Sessions] Delete failed: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# /a2a proxy — now with per-conversation session support
# ---------------------------------------------------------------------------

async def a2a_proxy_handler(request: Request):
    """
    Direct /a2a POST handler — replaces the Node.js Vite middleware.
    Accepts plain text or JSON from the frontend, runs the agent, returns SSE.
    
    Now streams intermediate progress events so the frontend spinner text
    updates live as the agent works through its pipeline steps.
    
    The frontend sends {request: "...", session_id: "..."} to route messages
    to the correct conversation session.
    """
    body = await request.body()
    body_str = body.decode('utf-8')

    # Unified auth: IAP headers in production, OAuth token in local dev
    auth_email = _get_auth_email(request)
    if not auth_email and AUTH_REQUIRED:
        return JSONResponse({"error": "Authentication required. Please sign in."}, status_code=401)

    user_id = _email_to_user_id(auth_email)

    # Parse body: could be JSON (UI event) or plain text (user query)
    query = ""
    session_id = None
    try:
        parsed = json.loads(body_str)
        if isinstance(parsed, dict):
            session_id = parsed.get("session_id")
            if 'request' in parsed:
                query = parsed['request']
            elif 'text' in parsed:
                query = parsed['text']
            else:
                query = body_str
        else:
            query = body_str
    except (json.JSONDecodeError, ValueError):
        query = body_str

    if not query.strip():
        return JSONResponse({"error": "Empty query"}, status_code=400)

    # If no session_id provided, create a new one (backward compat)
    if not session_id:
        session_id = str(uuid.uuid4())
        logger.info(f"[/a2a proxy] No session_id provided, created: {session_id}")

    logger.info(f"[/a2a proxy] Query: {query[:100]}... | session={session_id[:8]}")

    # Stream the response as SSE — with intermediate progress chunks
    async def event_stream():
        try:
            from google.genai import types
            msg = types.Content(role="user", parts=[types.Part.from_text(text=query)])

            # Set user email for workspace resolution
            set_user_email(auth_email)

            session = await _runner.session_service.get_session(
                app_name=_runner.app_name,
                user_id=user_id,
                session_id=session_id,
            )
            if session is None:
                await _runner.session_service.create_session(
                    app_name=_runner.app_name,
                    user_id=user_id,
                    state={
                        "conversation_title": "New conversation",
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "user_email": auth_email or "",
                    },
                    session_id=session_id,
                )

            # Inject user email into session state
            if auth_email:
                try:
                    actual_session = _runner.session_service.sessions[_runner.app_name][user_id][session_id]
                    actual_session.state['user_email'] = auth_email
                except Exception:
                    pass

            final_text = ""
            last_streamed_status = ""
            event_count = 0

            async for event in _runner.run_async(
                user_id=user_id,
                session_id=session_id,
                run_config=run_config.RunConfig(
                    streaming_mode=run_config.StreamingMode.SSE,
                    max_llm_calls=30,
                ),
                new_message=msg
            ):
                if event.content and event.content.parts:
                    current_text = ""
                    for p in event.content.parts:
                        if hasattr(p, 'text') and p.text:
                            current_text += p.text
                        
                        # Detect tool calls by function_call name — this is
                        # the reliable way to track agent progress via ADK events
                        if hasattr(p, 'function_call') and p.function_call:
                            tool_name = getattr(p.function_call, 'name', '') or ''
                            status = _tool_name_to_status(tool_name)
                            if status and status != last_streamed_status:
                                last_streamed_status = status
                                progress_chunk = [{"kind": "text", "text": status}]
                                yield f"data: {json.dumps(progress_chunk)}\n\n"
                        
                        # Detect tool responses — show "processing" status
                        if hasattr(p, 'function_response') and p.function_response:
                            tool_name = getattr(p.function_response, 'name', '') or ''
                            status = _tool_response_to_status(tool_name)
                            if status and status != last_streamed_status:
                                last_streamed_status = status
                                progress_chunk = [{"kind": "text", "text": status}]
                                yield f"data: {json.dumps(progress_chunk)}\n\n"
                    
                    if current_text:
                        final_text = current_text

            # Auto-title: after the first exchange, set the conversation title.
            # We must update the DB directly because modifying session.state
            # in-memory does NOT auto-persist with DatabaseSessionService.
            try:
                updated_session = await _runner.session_service.get_session(
                    app_name=_runner.app_name,
                    user_id=user_id,
                    session_id=session_id,
                )
                if updated_session and updated_session.state.get("conversation_title") == "New conversation":
                    title = query[:60].strip()
                    if len(query) > 60:
                        title += "..."
                    # Direct DB update for title persistence
                    import json as _json
                    from sqlalchemy import text as _sql_text
                    async with _runner.session_service.database_session_factory() as sql_sess:
                        # Read current state, merge title, write back
                        result = await sql_sess.execute(
                            _sql_text("SELECT state FROM sessions WHERE app_name = :app AND user_id = :uid AND id = :sid"),
                            {"app": _runner.app_name, "uid": user_id, "sid": session_id}
                        )
                        row = result.fetchone()
                        if row:
                            current_state = _json.loads(row[0]) if isinstance(row[0], str) else row[0]
                            current_state["conversation_title"] = title
                            await sql_sess.execute(
                                _sql_text("UPDATE sessions SET state = :state WHERE app_name = :app AND user_id = :uid AND id = :sid"),
                                {"state": _json.dumps(current_state), "app": _runner.app_name, "uid": user_id, "sid": session_id}
                            )
                            await sql_sess.commit()
                            logger.info(f"[Sessions] Auto-titled session {session_id[:8]}... -> '{title}'")
            except Exception as e:
                logger.warning(f"[Sessions] Auto-title failed: {e}")

            # Gather a2ui payloads from module-level variable.
            # This bypasses ADK session state which doesn't reliably
            # propagate from sub-agent tool_context to parent session.
            a2ui_payloads = get_latest_a2ui_payload() or []
            if a2ui_payloads:
                # Log what table is being sent to the frontend — including first row
                for pld in a2ui_payloads:
                    su = pld.get('surfaceUpdate', {})
                    for comp in su.get('components', []):
                        tbl = comp.get('component', {}).get('Table', {})
                        t = tbl.get('tableTitle', {}).get('literalString', '')
                        if t:
                            logger.info(f"[/a2a proxy] Sending a2ui table: '{t}'")
                            # Log first 3 rows to verify data correctness
                            rows = tbl.get('rows', {}).get('literalArray', [])
                            for i, row in enumerate(rows[:3]):
                                cells = [c.get('literalString', '') for c in row.get('literalArray', [])]
                                logger.info(f"[/a2a proxy]   row {i}: {cells}")

            # Clean inline a2ui markers from text
            if "<a2a_datapart_json>" in final_text:
                try:
                    json_part = final_text.split("<a2a_datapart_json>")[1].split("</a2a_datapart_json>")[0]
                    parsed = json.loads(json_part)
                    if not a2ui_payloads:
                        a2ui_payloads = parsed
                    final_text = final_text.replace(f"<a2a_datapart_json>{json_part}</a2a_datapart_json>", "")
                except Exception:
                    pass

            # Send the final complete response
            parts = []

            # Add a2ui data parts
            for pld in a2ui_payloads:
                parts.append({"kind": "data", "data": pld, "metadata": {"mimeType": "application/json+a2ui"}})

            # Add text part
            if final_text.strip():
                parts.insert(0, {"kind": "text", "text": final_text})

            if not parts:
                parts.append({"kind": "text", "text": "Task completed without output."})

            # Include session_id in response so frontend can track it
            yield f"data: {json.dumps({"session_id": session_id, "parts": parts})}\n\n"

        except Exception as e:
            logger.error(f"[/a2a proxy] Error: {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@click.command()
@click.option("--host", default="localhost")
@click.option("--port", default=10005)
def main(host, port):
    global _runner

    # Dummy agent card to bypass strict a2a requirements
    agent_card = AgentCard(
        name="BigQueryAssistant",
        description="BigQuery Assistant Agent",
        url=f"http://{host}:{port}",
        version="1.0.0",
        default_input_modes=["text"],
        default_output_modes=["text"],
        skills=[],
        capabilities=AgentCapabilities(streaming=True)
    )

    # Session persistence: SQLite by default (survives server restarts in local dev).
    # Override with SESSION_DB env var for managed DB (e.g. Cloud SQL postgres).
    session_db_url = os.environ.get("SESSION_DB", "sqlite+aiosqlite:///sessions.db")
    logger.info(f"Session persistence: {session_db_url}")

    _runner = Runner(
        app_name=root_agent.name,
        agent=root_agent,
        artifact_service=InMemoryArtifactService(),
        session_service=DatabaseSessionService(db_url=session_db_url),
        memory_service=InMemoryMemoryService(),
    )

    executor = SimpleBqExecutor(runner=_runner)

    request_handler = DefaultRequestHandler(
        agent_executor=executor,
        task_store=InMemoryTaskStore()
    )

    server = A2AStarletteApplication(
        agent_card=agent_card,
        http_handler=request_handler
    )

    app = server.build()

    # ---- Add direct /a2a proxy route (replaces Vite middleware in production) ----
    app.routes.insert(0, Route("/a2a", a2a_proxy_handler, methods=["POST"]))
    # ---- Session management endpoints ----
    app.routes.insert(0, Route("/sessions", list_sessions_handler, methods=["GET"]))
    app.routes.insert(0, Route("/sessions", create_session_handler, methods=["POST"]))
    app.routes.insert(0, Route("/sessions/{session_id}", get_session_handler, methods=["GET"]))
    app.routes.insert(0, Route("/sessions/{session_id}", delete_session_handler, methods=["DELETE"]))
    # ---- Auth endpoints ----
    app.routes.insert(0, Route("/auth/user", auth_user_handler, methods=["GET"]))
    app.routes.insert(0, Route("/auth/authorize", auth_authorize_handler, methods=["GET"]))
    app.routes.insert(0, Route("/auth/callback", auth_callback_handler, methods=["GET"]))
    app.routes.insert(0, Route("/auth/revoke", auth_revoke_handler, methods=["POST"]))

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    static_ui_path = os.path.join(os.path.dirname(__file__), "static_ui")
    if os.path.isdir(static_ui_path):
        logger.info(f"Serving static UI from {static_ui_path}")
        app.mount("/", StaticFiles(directory=static_ui_path, html=True), name="static")

    port = int(os.environ.get("PORT", port))
    logger.info(f"Running A2A server on host={host} port={port}")
    logger.info(f"Auth mode: {'IAP' if IAP_ENABLED else 'OAuth'} | Auth required: {AUTH_REQUIRED}")
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    main()

