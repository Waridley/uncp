#!/bin/bash

# Path Interning Benchmark Runner
# This script runs comprehensive benchmarks comparing interned paths vs plain strings

set -e

echo "🚀 Running Path Interning Benchmarks"
echo "===================================="

# Ensure we're in the right directory
cd "$(dirname "$0")/.."

# Build in release mode for accurate benchmarks
echo "📦 Building in release mode..."
cargo build --release

# Run the benchmarks
echo "⏱️  Running benchmarks..."
echo "This may take several minutes..."

# Run with nice output formatting
cargo bench --bench path_interning -- --output-format pretty

echo ""
echo "📊 Benchmark Results"
echo "==================="
echo ""
echo "Results have been saved to:"
echo "  - target/criterion/path_interning/report/index.html (detailed HTML report)"
echo "  - target/criterion/ (raw data)"
echo ""
echo "To view the HTML report:"
echo "  open target/criterion/path_interning/report/index.html"
echo ""
echo "Key metrics to look for:"
echo "  - Memory usage: Interned paths should use significantly less memory"
echo "  - Lookup performance: Interned paths should be faster for repeated lookups"
echo "  - Deduplication: Interned paths should excel with many duplicate paths"
echo "  - Display performance: Should be comparable or better than strings"
echo ""

# Check if HTML report exists and offer to open it
if command -v xdg-open >/dev/null 2>&1; then
    echo "🌐 Opening HTML report in browser..."
    xdg-open target/criterion/report/index.html 2>/dev/null || true
elif command -v open >/dev/null 2>&1; then
    echo "🌐 Opening HTML report in browser..."
    open target/criterion/report/index.html 2>/dev/null || true
else
    echo "💡 Tip: Open target/criterion/report/index.html in your browser to view detailed results"
fi

echo ""
echo "✅ Benchmarks complete!"
