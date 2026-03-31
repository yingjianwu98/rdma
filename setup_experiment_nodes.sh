#!/bin/bash

# RDMA Experiment Node Setup Script
# Configuration: 5 servers + 6 client nodes (3 threads each = 18 total client threads)

set -e  # Exit on error

# Node configuration
# First 5 nodes are servers, last 6 are client nodes
SERVER_NODES=(apt083 apt081 apt138 apt176 apt072)
CLIENT_NODES=(apt161 apt150 apt139 apt180 apt177 apt136)
ALL_NODES=("${SERVER_NODES[@]}" "${CLIENT_NODES[@]}")

USER="stevie98"
DOMAIN="apt.emulab.net"

echo "========================================="
echo "RDMA Experiment Node Setup"
echo "========================================="
echo "Configuration:"
echo "  Servers: 5 nodes (${SERVER_NODES[*]})"
echo "  Clients: 6 nodes × 3 threads = 18 total threads"
echo "  Total nodes: 11"
echo ""

# Step 1: Start ibacm.service on all nodes
echo "Step 1: Starting ibacm.service on all nodes..."
echo "  (InfiniBand Communication Manager Assistant)"
echo "  Provides scalable name/address/route resolution"
echo ""

for node in "${ALL_NODES[@]}"; do
    echo "  Starting ibacm on ${node}..."
    ssh ${USER}@${node}.${DOMAIN} "sudo systemctl start ibacm.service" &
done
wait
echo "✓ ibacm.service started on all nodes"
echo ""

# Step 2: Verify ibacm is running
echo "Step 2: Verifying ibacm.service status..."
echo ""

all_running=true
for node in "${ALL_NODES[@]}"; do
    if ssh ${USER}@${node}.${DOMAIN} "sudo systemctl is-active --quiet ibacm.service"; then
        echo "  ✓ ${node}: ibacm running"
    else
        echo "  ✗ ${node}: ibacm NOT running"
        all_running=false
    fi
done

if [ "$all_running" = false ]; then
    echo ""
    echo "⚠ WARNING: ibacm not running on all nodes!"
    echo "Continuing anyway..."
fi
echo ""

# Step 3: Clone repo and run setup.sh on all nodes
echo "Step 3: Cloning repo and running setup.sh on all nodes..."
echo "  (Installs packages, configures RDMA interfaces)"
echo ""

for node in "${ALL_NODES[@]}"; do
    echo "  Setting up ${node}..."
    ssh ${USER}@${node}.${DOMAIN} "
        cd /local && \
        rm -rf rdma && \
        git clone https://github.com/yingjianwu98/rdma.git && \
        cd rdma && \
        git checkout mu-watch-no-global-ordering && \
        bash scripts/setup.sh
    " > /tmp/setup_${node}.log 2>&1 &
done
wait
echo "✓ Setup scripts completed on all nodes"
echo ""

# Step 3b: Install build dependencies (CMake 3.28 + libc++)
echo "Step 3b: Installing build dependencies (CMake 3.28, libc++)..."
echo ""

for node in "${ALL_NODES[@]}"; do
    echo "  Installing dependencies on ${node}..."
    ssh ${USER}@${node}.${DOMAIN} "
        sudo apt install -y libc++-20-dev libc++abi-20-dev wget && \
        wget -q https://github.com/Kitware/CMake/releases/download/v3.28.1/cmake-3.28.1-linux-x86_64.tar.gz && \
        sudo tar -xzf cmake-3.28.1-linux-x86_64.tar.gz -C /opt && \
        sudo ln -sf /opt/cmake-3.28.1-linux-x86_64/bin/cmake /usr/local/bin/cmake && \
        rm cmake-3.28.1-linux-x86_64.tar.gz
    " > /tmp/deps_${node}.log 2>&1 &
done
wait
echo "✓ Dependencies installed on all nodes"
echo ""

# Step 3c: Build with proper flags
echo "Step 3c: Building with libc++ and pthread support..."
echo ""

for node in "${ALL_NODES[@]}"; do
    echo "  Building on ${node}..."
    ssh ${USER}@${node}.${DOMAIN} "
        cd /local/rdma && \
        mkdir -p build && \
        cd build && \
        CC=clang CXX=clang++ /usr/local/bin/cmake -DCMAKE_CXX_FLAGS='-stdlib=libc++' -DCMAKE_EXE_LINKER_FLAGS='-lpthread' .. && \
        make -j
    " > /tmp/build_${node}.log 2>&1 &
done
wait
echo "✓ Build completed on all nodes"
echo ""

# Check if any builds failed
setup_failed=false
for node in "${ALL_NODES[@]}"; do
    if grep -q "Built target rdma" /tmp/build_${node}.log 2>/dev/null; then
        echo "  ✓ ${node}: Build successful"
    else
        echo "  ✗ ${node}: Build FAILED - Check /tmp/build_${node}.log"
        setup_failed=true
    fi
done

if [ "$setup_failed" = true ]; then
    echo ""
    echo "⚠ WARNING: Some nodes may have setup errors!"
    echo "Check log files in /tmp/setup_*.log"
    echo "Continuing with connectivity test..."
fi
echo ""

# Step 4: Test RDMA network connectivity
echo "Step 4: Testing RDMA network connectivity..."
echo "  Testing from apt083 to all nodes on RDMA network"
echo ""

# Expected RDMA IPs:
# 192.168.1.1-5  = Servers (apt083, apt081, apt138, apt176, apt072)
# 192.168.1.6-11 = Clients (apt161, apt150, apt139, apt180, apt177, apt136)

all_reachable=true
for i in {1..11}; do
    echo -n "  Testing 192.168.1.$i... "
    if ssh ${USER}@apt083.${DOMAIN} "ping -c 1 -W 1 192.168.1.$i > /dev/null 2>&1"; then
        echo "✓ reachable"
    else
        echo "✗ UNREACHABLE"
        all_reachable=false
    fi
done
echo ""

if [ "$all_reachable" = false ]; then
    echo "⚠ WARNING: Not all RDMA IPs are reachable!"
    echo "This may cause experiment failures."
    echo ""
    echo "Debug steps:"
    echo "  1. Check RDMA interface on each node:"
    echo "     ssh stevie98@apt083.apt.emulab.net 'ip addr show | grep 192.168.1'"
    echo "  2. Verify ibacm is running:"
    echo "     ssh stevie98@apt083.apt.emulab.net 'sudo systemctl status ibacm'"
    echo "  3. Check InfiniBand link state:"
    echo "     ssh stevie98@apt083.apt.emulab.net 'ibstat'"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo "✓ All RDMA IPs reachable"
fi
echo ""

# Step 5: Verify RDMA devices
echo "Step 5: Verifying RDMA devices on all nodes..."
echo ""

for node in "${ALL_NODES[@]}"; do
    echo "  Checking ${node}..."
    device_count=$(ssh ${USER}@${node}.${DOMAIN} "ibstat 2>/dev/null | grep -c 'CA type:' || echo 0")
    if [ "$device_count" -gt 0 ]; then
        echo "    ✓ Found $device_count RDMA device(s)"
    else
        echo "    ✗ No RDMA devices found!"
        all_reachable=false
    fi
done
echo ""

# Step 6: Final summary
echo "========================================="
echo "Setup Complete!"
echo "========================================="
echo ""
echo "Node Status Summary:"
echo "  • ibacm.service: $([ "$all_running" = true ] && echo "✓ Running" || echo "⚠ Some nodes not running")"
echo "  • setup.sh: $([ "$setup_failed" = false ] && echo "✓ Success" || echo "⚠ Some nodes had errors")"
echo "  • Network: $([ "$all_reachable" = true ] && echo "✓ All nodes reachable" || echo "⚠ Some nodes unreachable")"
echo ""

if [ "$all_running" = true ] && [ "$setup_failed" = false ] && [ "$all_reachable" = true ]; then
    echo "✓ All checks passed! Ready to run experiments."
    echo ""
    echo "Next steps:"
    echo "  1. Update common.h for 18-thread configuration:"
    echo "     - TOTAL_CLIENTS = 18 (6 nodes × 3 threads)"
    echo "     - NUM_CLIENTS_PER_MACHINE = 3"
    echo ""
    echo "  2. Run Syndra watch experiments FIRST:"
    echo "     git checkout <syndra-watch-branch>"
    echo "     bash run_all_experiments.sh 2>&1 | tee /tmp/syndra_watch_results.log"
    echo ""
    echo "  3. Then run MU watch experiments:"
    echo "     git checkout mu-watch-no-global-ordering"
    echo "     bash run_all_experiments.sh 2>&1 | tee /tmp/mu_watch_results.log"
    exit 0
else
    echo "⚠ Some checks failed. Review errors above before running experiments."
    exit 1
fi
