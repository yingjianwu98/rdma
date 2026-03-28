#!/bin/bash

# Run all 6 experiments with phase-separated metrics

# Kill any leftover rdma processes before starting
echo "Cleaning up any leftover rdma processes..."
for host in apt130 apt131 apt129 apt132 apt136; do
    ssh stevie98@${host}.apt.emulab.net "sudo pkill -9 rdma || true" > /dev/null 2>&1 &
done
wait
echo "Cleanup complete."

EXPERIMENTS=(
    "10000:Experiment 1"
    "30000:Experiment 2"
    "50000:Experiment 3"
    "100000:Experiment 4"
    "200000:Experiment 5"
    "500000:Experiment 6"
)

for exp in "${EXPERIMENTS[@]}"; do
    NUM_OPS="${exp%%:*}"
    NAME="${exp##*:}"

    echo "========================================="
    echo "Running $NAME: $NUM_OPS ops"
    echo "========================================="

    # Update NUM_OPS in common.h
    sed -i '' "s/constexpr size_t NUM_OPS = [0-9]*;.*/constexpr size_t NUM_OPS = $NUM_OPS;  \/\/ $NAME/" include/rdma/common.h

    # Commit and push
    git add -f include/rdma/common.h
    git commit -m "$NAME: $NUM_OPS ops"
    git push

    # Rebuild all nodes in parallel
    for host in apt130 apt131 apt129 apt132 apt136; do
        ssh stevie98@${host}.apt.emulab.net "cd /local/rdma && git checkout yingjianw/wip && git pull origin yingjianw/wip && cd build && make -j" > /dev/null 2>&1 &
    done
    wait  # Wait for all rebuilds to complete

    # Restart servers
    for i in 0 1 2 3 4; do
        case $i in
            0) host=apt130 ;;
            1) host=apt131 ;;
            2) host=apt129 ;;
            3) host=apt132 ;;
            4) host=apt136 ;;
        esac
        ssh stevie98@${host}.apt.emulab.net "sudo pkill -9 rdma || true; cd /local/rdma/build && sudo bash -c 'NODE_ID=$i IS_CLIENT=0 nohup ./rdma > server_$i.log 2>&1 < /dev/null &'" > /dev/null 2>&1
    done

    sleep 3

    # Run client on apt130 (leader node) - syndra_watch requires all server nodes to be free for peer connections
    ssh stevie98@apt130.apt.emulab.net "cd /local/rdma/build && sudo IS_CLIENT=1 MACHINE_ID=0 timeout 240 ./rdma 2>&1" > "/tmp/exp_${NUM_OPS}.log"

    echo ""
    echo "========================================="
    echo "Results for $NAME:"
    echo "========================================="
    echo ""
    echo "--- Phase Throughput & Latency ---"
    grep -A 20 "PHASE THROUGHPUT" "/tmp/exp_${NUM_OPS}.log" | head -25
    echo ""
    echo "--- Overall Benchmark Summary ---"
    grep -A 20 "RDMA LOCK BENCHMARK RESULTS" "/tmp/exp_${NUM_OPS}.log" | head -25
    echo ""

done

echo "All experiments completed!"
