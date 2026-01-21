#!/bin/bash
#
# Check Snippet Bundle Size
#
# Cloudflare Snippets have a strict 32KB script size limit.
# This script bundles the bloom snippet and verifies it stays under the limit.
#
# Usage:
#   ./scripts/check-snippet-size.sh
#
# Exit codes:
#   0 - Bundle size is under 32KB limit
#   1 - Bundle size exceeds 32KB limit or build error
#
# Constraints (Cloudflare Snippets):
#   - 32KB max script size (32768 bytes)
#   - 5ms max compute time
#   - 32MB max memory
#   - No Node.js APIs - pure JS only
#

set -e

# Configuration
SNIPPET_PATH="src/snippet/bloom.snippet.js"
SIZE_LIMIT_KB=32
SIZE_LIMIT_BYTES=$((SIZE_LIMIT_KB * 1024))  # 32768 bytes

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=========================================="
echo "Cloudflare Snippet Size Check"
echo "=========================================="
echo ""

# Check if snippet file exists
if [ ! -f "$SNIPPET_PATH" ]; then
    echo -e "${RED}Error: Snippet file not found: $SNIPPET_PATH${NC}"
    exit 1
fi

# Create temp directory for bundle output
TEMP_DIR=$(mktemp -d)
BUNDLE_PATH="$TEMP_DIR/bloom.snippet.min.js"

# Cleanup on exit
cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

echo "Source: $SNIPPET_PATH"
echo "Limit:  ${SIZE_LIMIT_KB}KB (${SIZE_LIMIT_BYTES} bytes)"
echo ""

# Get source file size
SOURCE_SIZE=$(wc -c < "$SNIPPET_PATH" | tr -d ' ')
echo "Source size: ${SOURCE_SIZE} bytes ($(echo "scale=2; $SOURCE_SIZE / 1024" | bc)KB)"

# Bundle and minify with esbuild
echo ""
echo "Bundling with esbuild (minified)..."

npx esbuild "$SNIPPET_PATH" \
    --bundle \
    --minify \
    --format=esm \
    --platform=browser \
    --target=es2022 \
    --outfile="$BUNDLE_PATH" \
    2>&1

if [ ! -f "$BUNDLE_PATH" ]; then
    echo -e "${RED}Error: Bundle failed - output file not created${NC}"
    exit 1
fi

# Get bundled size
BUNDLE_SIZE=$(wc -c < "$BUNDLE_PATH" | tr -d ' ')
BUNDLE_SIZE_KB=$(echo "scale=2; $BUNDLE_SIZE / 1024" | bc)
PERCENTAGE=$(echo "scale=1; ($BUNDLE_SIZE * 100) / $SIZE_LIMIT_BYTES" | bc)

echo ""
echo "=========================================="
echo "Results"
echo "=========================================="
echo ""
echo "Bundle size: ${BUNDLE_SIZE} bytes (${BUNDLE_SIZE_KB}KB)"
echo "Limit:       ${SIZE_LIMIT_BYTES} bytes (${SIZE_LIMIT_KB}KB)"
echo "Usage:       ${PERCENTAGE}%"
echo ""

# Check if under limit
if [ "$BUNDLE_SIZE" -gt "$SIZE_LIMIT_BYTES" ]; then
    OVER_BY=$((BUNDLE_SIZE - SIZE_LIMIT_BYTES))
    echo -e "${RED}FAILED: Bundle exceeds ${SIZE_LIMIT_KB}KB limit by ${OVER_BY} bytes${NC}"
    echo ""
    echo "To fix:"
    echo "  1. Remove unused code from the snippet"
    echo "  2. Simplify regex patterns"
    echo "  3. Use shorter variable names in hot paths"
    echo "  4. Consider code-splitting if possible"
    exit 1
else
    HEADROOM=$((SIZE_LIMIT_BYTES - BUNDLE_SIZE))
    HEADROOM_KB=$(echo "scale=2; $HEADROOM / 1024" | bc)
    echo -e "${GREEN}PASSED: Bundle is under ${SIZE_LIMIT_KB}KB limit${NC}"
    echo "Headroom: ${HEADROOM} bytes (${HEADROOM_KB}KB remaining)"
    exit 0
fi
