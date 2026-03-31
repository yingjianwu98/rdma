#!/bin/bash

# Run CPU metrics comparison for mild vs aggressive backoff

NODE="apt161.apt.emulab.net"

echo "=========================================="
echo "CPU METRICS COMPARISON TEST"
echo "=========================================="
echo ""

# Test 1: Mild backoff (100→1000→yield) - already running, just capture output
echo "Test 1: Running with MILD backoff (100→1000→yield)..."
echo "  Resetting backoff to mild parameters..."

ssh -o StrictHostKeyChecking=accept-new stevie98@$NODE << 'ENDSSH'
cd /local/rdma
# Update to mild backoff
sed -i 's/if (empty_polls < [0-9]\+)/if (empty_polls < 100)/' src/pipelines/watch_pipeline.cpp
sed -i 's/} else if (empty_polls < [0-9]\+)/} else if (empty_polls < 1000)/' src/pipelines/watch_pipeline.cpp
cd build && cmake .. && make -j8 > /dev/null 2>&1
ENDSSH

echo "  Running 100K ops with mild backoff..."
ssh stevie98@$NODE "cd /local/rdma/build && sudo bash -c 'NODE_ID=0 IS_CLIENT=1 ./rdma 2>&1'" > /tmp/cpu_mild_backoff.txt &
MILD_PID=$!

sleep 10  # Let it complete

echo ""
echo "Test 2: Running with AGGRESSIVE backoff (10→50→yield)..."
echo "  Updating to aggressive parameters..."

ssh -o StrictHostKeyChecking=accept-new stevie98@$NODE << 'ENDSSH'
cd /local/rdma
# Update to aggressive backoff
sed -i 's/if (empty_polls < [0-9]\+)/if (empty_polls < 10)/' src/pipelines/watch_pipeline.cpp
sed -i 's/} else if (empty_polls < [0-9]\+)/} else if (empty_polls < 50)/' src/pipelines/watch_pipeline.cpp
cd build && cmake .. && make -j8 > /dev/null 2>&1
ENDSSH

echo "  Running 100K ops with aggressive backoff..."
ssh stevie98@$NODE "cd /local/rdma/build && sudo bash -c 'NODE_ID=0 IS_CLIENT=1 ./rdma 2>&1'" > /tmp/cpu_aggressive_backoff.txt

echo ""
echo "=========================================="
echo "EXTRACTING CPU METRICS"
echo "=========================================="
echo ""

echo "=== MILD BACKOFF (100→1000→yield) ==="
grep -A 20 "BOTTLENECK DIAGNOSIS" /tmp/cpu_mild_backoff.txt | head -25

echo ""
echo "=== AGGRESSIVE BACKOFF (10→50→yield) ==="
grep -A 20 "BOTTLENECK DIAGNOSIS" /tmp/cpu_aggressive_backoff.txt | head -25

echo ""
echo "=========================================="
echo "Comparison complete!"
echo "=========================================="
