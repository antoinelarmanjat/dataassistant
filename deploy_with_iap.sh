#!/usr/bin/env bash
# deploy_with_iap.sh — Build, deploy to Cloud Run, and configure IAP
#
# Prerequisites:
#   - gcloud CLI authenticated with appropriate permissions
#   - Docker (or gcloud builds submit) available
#   - APIs enabled: run.googleapis.com, iap.googleapis.com
#
# Usage:
#   ./deploy_with_iap.sh
#
# This script will:
#   1. Build the frontend and copy to static_ui/
#   2. Build and push the Docker image
#   3. Deploy to Cloud Run (requires IAM auth — no --allow-unauthenticated)
#   4. Enable IAP directly on the Cloud Run service
#   5. Grant access to the google.com domain
#

set -euo pipefail

# ---- Configuration ----
PROJECT_ID="antoine-exp"
REGION="us-central1"
SERVICE_NAME="data-assistant"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
# Service account for Cloud Run (needs BigQuery, GCS, Vertex AI access)
# If you have a dedicated SA, set it here. Otherwise Cloud Run default SA is used.
# SERVICE_ACCOUNT="data-assistant-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "============================================"
echo " Data Assistant — Cloud Run + IAP Deployment"
echo "============================================"
echo ""
echo "Project:  ${PROJECT_ID}"
echo "Region:   ${REGION}"
echo "Service:  ${SERVICE_NAME}"
echo ""

# ---- Step 0: Set project ----
echo "▸ Setting active project to ${PROJECT_ID}..."
gcloud config set project "${PROJECT_ID}"

# ---- Step 1: Enable required APIs ----
echo "▸ Enabling required APIs..."
gcloud services enable \
  run.googleapis.com \
  iap.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  --quiet

# ---- Step 2: Build frontend ----
echo "▸ Building frontend..."
FRONTEND_DIR="samples/client/lit/custom-components-example"
if [ -d "${FRONTEND_DIR}" ]; then
  cd "${FRONTEND_DIR}"
  npm ci --silent 2>/dev/null || npm install --silent
  # Use Vite production build (bundles HTML + JS) instead of tsc
  npx vite build --config vite.prod.config.ts
  cd -

  # Copy Vite's bundled output to backend's static_ui directory
  echo "▸ Copying frontend to static_ui..."
  rm -rf dataagent/agents/bq_assistant/static_ui
  cp -r "${FRONTEND_DIR}/dist" dataagent/agents/bq_assistant/static_ui
else
  echo "  ⚠ Frontend directory not found. Assuming static_ui/ is already populated."
fi

# ---- Step 3: Build and push Docker image ----
echo "▸ Building Docker image with Cloud Build..."
gcloud builds submit --tag "${IMAGE_NAME}" --timeout=600 --quiet

# ---- Step 4: Deploy to Cloud Run ----
echo "▸ Deploying to Cloud Run..."

# Prompt for OAuth client secret if not set
if [ -z "${OAUTH_DATA_CLIENT_SECRET:-}" ]; then
  echo ""
  echo "  ╔══════════════════════════════════════════════════════════════╗"
  echo "  ║  OAuth client secret needed for user data access consent.   ║"
  echo "  ║  Get it from:                                               ║"
  echo "  ║  https://console.cloud.google.com/apis/credentials          ║"
  echo "  ║  → IAP-Data-Assistant → Client secret                       ║"
  echo "  ╚══════════════════════════════════════════════════════════════╝"
  echo ""
  read -rp "  Enter OAUTH_DATA_CLIENT_SECRET (or press Enter to skip): " OAUTH_DATA_CLIENT_SECRET
fi

EXTRA_ENV_VARS=""
if [ -n "${OAUTH_DATA_CLIENT_SECRET:-}" ]; then
  EXTRA_ENV_VARS="--set-env-vars=OAUTH_DATA_CLIENT_SECRET=${OAUTH_DATA_CLIENT_SECRET}"
fi

gcloud run deploy "${SERVICE_NAME}" \
  --image "${IMAGE_NAME}" \
  --region "${REGION}" \
  --platform managed \
  --memory 2Gi \
  --cpu 2 \
  --timeout 300 \
  --min-instances 0 \
  --max-instances 10 \
  --no-allow-unauthenticated \
  ${EXTRA_ENV_VARS} \
  --quiet

# Get the Cloud Run URL
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --region "${REGION}" --format='value(status.url)')
echo "  ✓ Deployed: ${SERVICE_URL}"

# ---- Step 5: Create IAP service agent ----
echo "▸ Creating IAP service agent..."
gcloud beta services identity create \
  --service=iap.googleapis.com \
  --project="${PROJECT_ID}" 2>/dev/null || true

# Get project number
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')

# Grant Cloud Run Invoker to IAP service account
echo "▸ Granting Cloud Run Invoker to IAP service account..."
gcloud run services add-iam-policy-binding "${SERVICE_NAME}" \
  --region="${REGION}" \
  --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-iap.iam.gserviceaccount.com" \
  --role="roles/run.invoker" \
  --quiet 2>/dev/null || true

# ---- Step 6: Enable IAP on the Cloud Run service ----
echo "▸ Enabling IAP on Cloud Run service..."
echo ""
echo "  ╔══════════════════════════════════════════════════════════════╗"
echo "  ║  NOTE: Enable IAP directly on Cloud Run via the Console:   ║"
echo "  ║                                                            ║"
echo "  ║  1. Go to: https://console.cloud.google.com/security/iap   ║"
echo "  ║     ?project=${PROJECT_ID}"
echo "  ║  2. Find '${SERVICE_NAME}' and toggle IAP ON               ║"
echo "  ║  3. Configure OAuth consent screen if prompted             ║"
echo "  ║                                                            ║"
echo "  ║  Alternatively, use gcloud (if available for direct CR):   ║"
echo "  ║  gcloud run services update ${SERVICE_NAME} \\             ║"
echo "  ║    --region=${REGION} --iap                                ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo ""

# ---- Step 7: Grant access to google.com domain ----
echo "▸ Granting IAP access to google.com domain..."
echo ""
echo "  Run this command to grant all Google employees access:"
echo ""
echo "  gcloud iap web add-iam-policy-binding \\"
echo "    --resource-type=cloud-run \\"
echo "    --service=${SERVICE_NAME} \\"
echo "    --region=${REGION} \\"
echo "    --member='domain:google.com' \\"
echo "    --role='roles/iap.httpsResourceAccessor'"
echo ""

# ---- Step 8: Ensure Cloud Run SA has BigQuery access ----
echo "▸ Checking Cloud Run service account permissions..."
echo ""
echo "  The Cloud Run service account needs these roles:"
echo "  - roles/bigquery.user (run queries)"
echo "  - roles/bigquery.dataViewer (read data)"
echo "  - roles/storage.objectAdmin (GCS export)"
echo "  - roles/aiplatform.user (Vertex AI / Gemini)"
echo ""
echo "  If using the default Compute Engine SA, it may already have these."
echo "  Otherwise, grant them with:"
echo ""
echo "  SA=\$(gcloud run services describe ${SERVICE_NAME} --region=${REGION} --format='value(spec.template.spec.serviceAccountName)')"
echo "  for ROLE in roles/bigquery.user roles/bigquery.dataViewer roles/storage.objectAdmin roles/aiplatform.user; do"
echo "    gcloud projects add-iam-policy-binding ${PROJECT_ID} --member=serviceAccount:\${SA} --role=\${ROLE}"
echo "  done"
echo ""

echo "============================================"
echo " Deployment complete!"
echo "============================================"
echo ""
echo " Cloud Run URL: ${SERVICE_URL}"
echo ""
echo " Next steps:"
echo " 1. Enable IAP via the Cloud Console (see instructions above)"
echo " 2. Grant google.com domain access (see command above)"
echo " 3. Verify: open ${SERVICE_URL} — should redirect to Google Sign-In via IAP"
echo " 4. After signing in with @google.com, the app should work"
echo ""
echo " To access from ANY workstation:"
echo "   Any Google employee can visit ${SERVICE_URL}"
echo "   IAP will prompt them to sign in with their @google.com account"
echo ""
