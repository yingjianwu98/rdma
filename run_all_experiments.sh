#!/bin/bash

# Run all 6 experiments with phase-separated metrics

# Kill any leftover rdma processes before starting
echo "Cleaning up any leftover rdma processes..."
for host in apt128 apt132 apt095 apt104 apt112; do
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
    for host in apt128 apt132 apt095 apt104 apt112; do
        ssh stevie98@${host}.apt.emulab.net "cd /local/rdma && git checkout yingjianw/wip && git pull origin yingjianw/wip && cd build && make -j" > /dev/null 2>&1 &
    done
    wait  # Wait for all rebuilds to complete

    # Kill servers
    echo "Stopping servers..."
    for host in apt128 apt132 apt095 apt104 apt112; do
        ssh stevie98@${host}.apt.emulab.net "sudo pkill -9 rdma || true" &
    done
    wait

    # Verify all servers are stopped
    echo "Verifying servers are stopped..."
    for attempt in {1..10}; do
        all_stopped=true
        for host in apt128 apt132 apt095 apt104 apt112; do
            if ssh stevie98@${host}.apt.emulab.net "pgrep -x rdma > /dev/null 2>&1"; then
                all_stopped=false
                break
            fi
        done
        if [ "$all_stopped" = true ]; then
            echo "✓ All servers stopped"
            break
        fi
        echo "  Waiting for servers to stop (attempt $attempt/10)..."
        sleep 1
    done

    # Start servers
    echo "Starting servers..."
    for i in 0 1 2 3 4; do
        case $i in
            0) host=apt128 ;;
            1) host=apt132 ;;
            2) host=apt095 ;;
            3) host=apt104 ;;
            4) host=apt112 ;;
        esac
        ssh stevie98@${host}.apt.emulab.net "cd /local/rdma/build && sudo bash -c 'NODE_ID=$i IS_CLIENT=0 nohup ./rdma > server_$i.log 2>&1 < /dev/null &'" > /dev/null 2>&1
    done

    sleep 5
    echo "✓ Servers started"

    # Run client on apt128 (leader node) - syndra_watch requires all server nodes to be free for peer connections
    ssh stevie98@apt128.apt.emulab.net "cd /local/rdma/build && sudo IS_CLIENT=1 MACHINE_ID=0 timeout 240 ./rdma 2>&1" > "/tmp/exp_${NUM_OPS}.log"

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
