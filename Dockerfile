FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    "a2a-sdk>=0.3.0" \
    "google-adk>=1.28.0" \
    "google-genai>=1.27.0" \
    "google-cloud-bigquery" \
    "google-cloud-storage" \
    "google-api-python-client" \
    "google-auth" \
    "pandas" \
    "db-dtypes" \
    "uvicorn" \
    "starlette" \
    "pydantic" \
    "httpx" \
    "click" \
    "sse-starlette" \
    "sqlalchemy>=2.0" \
    "aiosqlite"

# Copy backend code + pre-built frontend (in static_ui/)
COPY dataagent/ /app/dataagent/

WORKDIR /app/dataagent/agents/bq_assistant

# Cloud Run provides PORT env var (default 8080)
ENV PORT=8080
ENV GOOGLE_GENAI_USE_VERTEXAI=true
ENV GOOGLE_CLOUD_PROJECT=antoine-exp
ENV DATA_PROJECT=octo-aif-sandbox
ENV GOOGLE_CLOUD_LOCATION=global
ENV AUTH_REQUIRED=true
ENV IAP_ENABLED=true

# OAuth client for user data access consent flow (BigQuery, Sheets, Drive, GCS)
# The client ID can be set here; the client SECRET should be set via Cloud Run secrets
ENV OAUTH_DATA_CLIENT_ID=875254624092-6dibq0ems3u2gike0lr051h3785374a5.apps.googleusercontent.com
# OAUTH_DATA_CLIENT_SECRET is set as a Cloud Run secret - do not hardcode here

EXPOSE 8080

CMD ["python", "__main__.py", "--host", "0.0.0.0"]
