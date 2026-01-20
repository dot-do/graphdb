#!/bin/bash
#
# Deploy GraphDB to production (graphdb.workers.do)
#
# Usage:
#   ./scripts/deploy-prod.sh
#
# Prerequisites:
#   - wrangler authenticated with Cloudflare
#   - R2 bucket 'graphdb-lakehouse-prod' created
#   - KV namespace created with correct ID in wrangler.jsonc
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "=========================================="
echo "Deploying GraphDB to graphdb.workers.do"
echo "=========================================="

# Check for wrangler
if ! command -v wrangler &> /dev/null; then
    echo "Error: wrangler not found. Install with: npm install -g wrangler"
    exit 1
fi

# Build check
echo ""
echo "1. Type checking..."
npx tsc --noEmit

echo ""
echo "2. Deploying to production..."
wrangler deploy

echo ""
echo "3. Verifying deployment..."
sleep 2

# Health check
HEALTH_RESPONSE=$(curl -s https://graphdb.workers.do/health)
echo "Health check response: $HEALTH_RESPONSE"

if echo "$HEALTH_RESPONSE" | grep -q '"status":"healthy"'; then
    echo ""
    echo "=========================================="
    echo "Deployment successful!"
    echo "=========================================="
    echo ""
    echo "Endpoints:"
    echo "  - https://graphdb.workers.do/"
    echo "  - https://graphdb.workers.do/health"
    echo "  - https://graphdb.workers.do/benchmark/scenarios"
    echo ""
else
    echo ""
    echo "Warning: Health check did not return expected response"
    echo "Please verify deployment manually"
fi
