#!/bin/bash

# Direct CPU comparison between mild and aggressive backoff

echo "======================================================================"
echo "CPU COMPARISON: MILD vs AGGRESSIVE BACKOFF (100K ops)"
echo "======================================================================"
echo ""

# Server nodes
SERVERS=("apt083" "apt081" "apt138" "apt176" "apt072")
CLIENT="apt161"

# Test 1: AGGRESSIVE BACKOFF (already on aggressive: 10→50→yield)
echo "[1/2] Testing AGGRESSIVE backoff (10→50→yield)..."
echo "  Current backoff is already aggressive, running test..."

# Stop all processes
for node in "${SERVERS[@]}" "$CLIENT"; do
    ssh -o ConnectTimeout=5 stevie98@${node}.apt.emulab.net \
        "sudo pkill -9 rdma" 2>/dev/null &
done
wait
sleep 2

# Start servers
for i in "${!SERVERS[@]}"; do
    ssh -o ConnectTimeout=5 stevie98@${SERVERS[$i]}.apt.emulab.net \
        "cd /local/rdma/build && sudo bash -c 'NODE_ID=$i IS_CLIENT=0 nohup ./rdma > /dev/null 2>&1 < /dev/null &'" &
done
wait
sleep 3

# Run client and capture stderr (which has CPU metrics)
echo "  Running 100K ops benchmark..."
ssh stevie98@${CLIENT}.apt.emulab.net \
    "cd /local/rdma/build && sudo bash -c 'NODE_ID=0 IS_CLIENT=1 ./rdma 2>&1'" \
    > /tmp/aggressive_full_output.txt

echo "  ✓ Aggressive backoff test complete"
echo ""

# Test 2: MILD BACKOFF (100→1000→yield)
echo "[2/2] Testing MILD backoff (100→1000→yield)..."
echo "  Updating code to mild backoff..."

# Update to mild backoff on all nodes
ssh stevie98@${CLIENT}.apt.emulab.net << 'ENDSSH'
cd /local/rdma
sed -i 's/if (empty_polls < [0-9]\+) {/if (empty_polls < 100) {/' src/pipelines/watch_pipeline.cpp
sed -i 's/} else if (empty_polls < [0-9]\+) {/} else if (empty_polls < 1000) {/' src/pipelines/watch_pipeline.cpp
cd build && cmake .. > /dev/null && make -j8 > /dev/null 2>&1
ENDSSH

for node in "${SERVERS[@]}"; do
    ssh stevie98@${node}.apt.emulab.net << 'ENDSSH' &
cd /local/rdma
sed -i 's/if (empty_polls < [0-9]\+) {/if (empty_polls < 100) {/' src/pipelines/watch_pipeline.cpp
sed -i 's/} else if (empty_polls < [0-9]\+) {/} else if (empty_polls < 1000) {/' src/pipelines/watch_pipeline.cpp
cd build && cmake .. > /dev/null && make -j8 > /dev/null 2>&1
ENDSSH
done
wait

echo "  ✓ Code updated, rebuilding..."
sleep 2

# Stop all processes
for node in "${SERVERS[@]}" "$CLIENT"; do
    ssh -o ConnectTimeout=5 stevie98@${node}.apt.emulab.net \
        "sudo pkill -9 rdma" 2>/dev/null &
done
wait
sleep 2

# Start servers
for i in "${!SERVERS[@]}"; do
    ssh -o ConnectTimeout=5 stevie98@${SERVERS[$i]}.apt.emulab.net \
        "cd /local/rdma/build && sudo bash -c 'NODE_ID=$i IS_CLIENT=0 nohup ./rdma > /dev/null 2>&1 < /dev/null &'" &
done
wait
sleep 3

# Run client and capture stderr
echo "  Running 100K ops benchmark..."
ssh stevie98@${CLIENT}.apt.emulab.net \
    "cd /local/rdma/build && sudo bash -c 'NODE_ID=0 IS_CLIENT=1 ./rdma 2>&1'" \
    > /tmp/mild_full_output.txt

echo "  ✓ Mild backoff test complete"
echo ""

# Extract and compare CPU metrics
echo "======================================================================"
echo "RESULTS: CPU UTILIZATION COMPARISON"
echo "======================================================================"
echo ""

echo "=== AGGRESSIVE BACKOFF (10→50→yield) ==="
grep -A 20 "BOTTLENECK DIAGNOSIS" /tmp/aggressive_full_output.txt | head -25
echo ""

echo "=== MILD BACKOFF (100→1000→yield) ==="
grep -A 20 "BOTTLENECK DIAGNOSIS" /tmp/mild_full_output.txt | head -25
echo ""

echo "======================================================================"
echo "Test complete! Full outputs saved to:"
echo "  /tmp/aggressive_full_output.txt"
echo "  /tmp/mild_full_output.txt"
echo "======================================================================"
