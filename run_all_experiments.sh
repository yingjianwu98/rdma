#!/bin/bash

# Run all 5 experiments with phase-separated metrics

EXPERIMENTS=(
    "640000:Experiment 3"
    "960000:Experiment 4"
    "1280000:Experiment 5"
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

    # Rebuild all nodes
    for host in apt130 apt131 apt129 apt132 apt136; do
        ssh stevie98@${host}.apt.emulab.net "cd /local/rdma && git checkout yingjianw/wip && git pull origin yingjianw/wip && cd build && make -j" > /dev/null 2>&1
    done

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

    # Run client on apt131 (follower node) to avoid contention with leader on apt130
    ssh stevie98@apt131.apt.emulab.net "cd /local/rdma/build && sudo IS_CLIENT=1 MACHINE_ID=0 timeout 300 ./rdma 2>&1" > "/tmp/exp_${NUM_OPS}.log"

    echo ""
    echo "Results for $NAME:"
    grep -A 50 "PHASE THROUGHPUT\|REGISTRATION LATENCY\|NOTIFICATION LATENCY\|Wall Clock" "/tmp/exp_${NUM_OPS}.log" | head -30
    echo ""

done

echo "All experiments completed!"
