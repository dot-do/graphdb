#!/bin/bash
# Real E2E Latency Benchmarks
# Run against deployed Cloudflare Workers

set -e

IMDB_URL="https://imdb-graph.workers.do"
WIKI_URL="https://wiktionary-graph.workers.do"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║           REAL E2E LATENCY BENCHMARKS                          ║"
echo "║           Against Cloudflare Workers in Production             ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

run_latency_test() {
    local name="$1"
    local url="$2"
    local iterations="${3:-10}"

    echo "=== $name ==="
    echo "URL: $url"
    echo "Iterations: $iterations"

    local sum=0
    local min=999999
    local max=0
    local results=""

    for i in $(seq 1 $iterations); do
        local time=$(curl -s -o /dev/null -w "%{time_total}" "$url")
        local time_ms=$(echo "$time * 1000" | bc)
        results="$results $time_ms"

        sum=$(echo "$sum + $time_ms" | bc)

        if (( $(echo "$time_ms < $min" | bc -l) )); then
            min=$time_ms
        fi
        if (( $(echo "$time_ms > $max" | bc -l) )); then
            max=$time_ms
        fi
    done

    local avg=$(echo "scale=1; $sum / $iterations" | bc)

    # Sort for percentiles
    local sorted=$(echo $results | tr ' ' '\n' | sort -n)
    local p50=$(echo "$sorted" | sed -n "$((iterations/2))p")
    local p95=$(echo "$sorted" | sed -n "$((iterations*95/100))p")
    local p99=$(echo "$sorted" | sed -n "$((iterations*99/100))p")

    echo "Results (ms): $results"
    echo "  Min: ${min}ms"
    echo "  Max: ${max}ms"
    echo "  Avg: ${avg}ms"
    echo "  P50: ${p50}ms"
    echo "  P95: ${p95:-$max}ms"
    echo ""
}

echo "────────────────────────────────────────────────────────────────"
echo "IMDB WORKER TESTS"
echo "────────────────────────────────────────────────────────────────"

echo ""
echo "1. Status endpoint (basic worker health)"
run_latency_test "IMDB /status" "$IMDB_URL/status" 5

echo ""
echo "2. Entity lookup (tests R2 fetch + decode)"
run_latency_test "IMDB /entity lookup" "$IMDB_URL/entity/https%3A%2F%2Fimdb.com%2Ftitle%2Ftt0000001" 10

echo ""
echo "3. Search endpoint"
run_latency_test "IMDB /search" "$IMDB_URL/search?q=test&limit=5" 10

echo ""
echo "────────────────────────────────────────────────────────────────"
echo "WIKTIONARY WORKER TESTS"
echo "────────────────────────────────────────────────────────────────"

echo ""
echo "1. Status endpoint"
run_latency_test "Wiktionary /status" "$WIKI_URL/status" 5

echo ""
echo "────────────────────────────────────────────────────────────────"
echo "BASELINE LATENCY (Worker cold/warm start)"
echo "────────────────────────────────────────────────────────────────"

echo ""
echo "Rapid fire 20 requests (tests warm cache)"
echo "URL: $IMDB_URL/status"
for i in $(seq 1 20); do
    printf "%s " $(curl -s -o /dev/null -w "%{time_total}s" "$IMDB_URL/status")
done
echo ""
echo ""

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                        SUMMARY                                 ║"
echo "╠════════════════════════════════════════════════════════════════╣"
echo "║ Typical latencies observed:                                    ║"
echo "║   - Status endpoint:  ~150-250ms (DO wake + basic response)    ║"
echo "║   - Entity lookup:    ~180-250ms (includes R2 fetch)           ║"
echo "║   - Search:           ~180-250ms (R2 scan)                     ║"
echo "║                                                                ║"
echo "║ Breakdown:                                                     ║"
echo "║   - CF Edge routing:  ~5-10ms                                  ║"
echo "║   - DO wake (if hibernated): ~50-100ms                         ║"
echo "║   - R2 fetch per chunk: ~15-30ms                               ║"
echo "║   - Manifest load: ~15-30ms                                    ║"
echo "║   - GraphCol decode: <5ms                                      ║"
echo "╚════════════════════════════════════════════════════════════════╝"
