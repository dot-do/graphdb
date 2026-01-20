#!/bin/bash
#
# Run GraphDB Production Benchmarks
#
# Usage:
#   ./scripts/run-benchmarks.sh [dataset] [iterations]
#
# Arguments:
#   dataset    - Dataset size: tiny, small, medium, onet, imdb (default: small)
#   iterations - Number of iterations per scenario (default: 100)
#
# Example:
#   ./scripts/run-benchmarks.sh small 100
#   ./scripts/run-benchmarks.sh medium 500
#

set -e

BASE_URL="${GRAPHDB_URL:-https://graphdb.workers.do}"
DATASET="${1:-small}"
ITERATIONS="${2:-100}"

echo "=========================================="
echo "GraphDB Production Benchmarks"
echo "=========================================="
echo ""
echo "Base URL:   $BASE_URL"
echo "Dataset:    $DATASET"
echo "Iterations: $ITERATIONS"
echo ""

# Create results directory
RESULTS_DIR="./benchmark-results"
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="$RESULTS_DIR/benchmark_${DATASET}_${TIMESTAMP}.json"

# Check available scenarios
echo "1. Checking available scenarios..."
curl -s "$BASE_URL/benchmark/scenarios" | jq .
echo ""

# Reset previous data
echo "2. Resetting previous benchmark data..."
curl -s -X DELETE "$BASE_URL/benchmark/reset" | jq .
echo ""

# Seed test data
echo "3. Seeding $DATASET dataset..."
echo "   This may take a while for larger datasets..."
SEED_RESULT=$(curl -s -X POST "$BASE_URL/benchmark/seed?dataset=$DATASET")
echo "$SEED_RESULT" | jq .
echo ""

TRIPLE_COUNT=$(echo "$SEED_RESULT" | jq -r '.tripleCount // 0')
echo "   Seeded $TRIPLE_COUNT triples"
echo ""

# Run individual benchmarks
echo "4. Running benchmarks..."
echo ""

SCENARIOS=(
    "point-lookup"
    "traversal-1hop"
    "traversal-3hop"
    "write-throughput"
    "bloom-filter-hit-rate"
    "edge-cache-hit-rate"
)

ALL_RESULTS="[]"

for SCENARIO in "${SCENARIOS[@]}"; do
    echo "   Running: $SCENARIO"
    RESULT=$(curl -s -X POST "$BASE_URL/benchmark/run/$SCENARIO?iterations=$ITERATIONS&dataset=$DATASET")

    # Extract key metrics for display
    if echo "$RESULT" | jq -e '.latency' > /dev/null 2>&1; then
        P50=$(echo "$RESULT" | jq -r '.latency.p50 // "N/A"')
        P95=$(echo "$RESULT" | jq -r '.latency.p95 // "N/A"')
        P99=$(echo "$RESULT" | jq -r '.latency.p99 // "N/A"')
        echo "      p50: ${P50}ms, p95: ${P95}ms, p99: ${P99}ms"
    elif echo "$RESULT" | jq -e '.throughput' > /dev/null 2>&1; then
        OPS=$(echo "$RESULT" | jq -r '.throughput.operationsPerSecond // "N/A"')
        echo "      Throughput: ${OPS} ops/sec"
    elif echo "$RESULT" | jq -e '.cache' > /dev/null 2>&1; then
        HIT_RATE=$(echo "$RESULT" | jq -r '.cache.hitRate // "N/A"')
        echo "      Hit rate: ${HIT_RATE}"
    fi

    # Append to results
    ALL_RESULTS=$(echo "$ALL_RESULTS" | jq --argjson result "$RESULT" '. + [$result]')
    echo ""
done

# Save results
echo "5. Saving results to $RESULTS_FILE..."
cat > "$RESULTS_FILE" << EOF
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "dataset": "$DATASET",
  "iterations": $ITERATIONS,
  "tripleCount": $TRIPLE_COUNT,
  "baseUrl": "$BASE_URL",
  "results": $ALL_RESULTS
}
EOF

echo ""
echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="
echo ""
echo "Results saved to: $RESULTS_FILE"
echo ""

# Print summary table
echo "Summary:"
echo "--------"
printf "%-25s %10s %10s %10s\n" "Scenario" "p50 (ms)" "p95 (ms)" "p99 (ms)"
printf "%-25s %10s %10s %10s\n" "------------------------" "----------" "----------" "----------"

for i in $(seq 0 $((${#SCENARIOS[@]} - 1))); do
    SCENARIO="${SCENARIOS[$i]}"
    RESULT=$(echo "$ALL_RESULTS" | jq ".[$i]")

    if echo "$RESULT" | jq -e '.latency' > /dev/null 2>&1; then
        P50=$(echo "$RESULT" | jq -r '.latency.p50 // "-"' | xargs printf "%.2f")
        P95=$(echo "$RESULT" | jq -r '.latency.p95 // "-"' | xargs printf "%.2f")
        P99=$(echo "$RESULT" | jq -r '.latency.p99 // "-"' | xargs printf "%.2f")
        printf "%-25s %10s %10s %10s\n" "$SCENARIO" "$P50" "$P95" "$P99"
    elif echo "$RESULT" | jq -e '.throughput' > /dev/null 2>&1; then
        OPS=$(echo "$RESULT" | jq -r '.throughput.operationsPerSecond // "-"' | xargs printf "%.0f")
        printf "%-25s %10s ops/sec\n" "$SCENARIO" "$OPS"
    elif echo "$RESULT" | jq -e '.cache' > /dev/null 2>&1; then
        HIT_RATE=$(echo "$RESULT" | jq -r '.cache.hitRate // "-"' | xargs printf "%.2f")
        printf "%-25s %10s hit rate\n" "$SCENARIO" "$HIT_RATE"
    fi
done

echo ""
echo "Full results: $RESULTS_FILE"
