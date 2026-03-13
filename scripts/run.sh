#!/bin/bash

IS_CLIENT=${1:-0}
MACHINE_ID=${2:-0}

RAW_ID=$(ifconfig enp8s0d1 | grep 'inet ' | awk '{print $2}' | cut -d'.' -f4)

if [ -z "$RAW_ID" ]; then
    echo "Error: Could not find IP for enp8s0d1"
    exit 1
fi

NODE_ID=$((RAW_ID - 1))

echo "NODE_ID=$NODE_ID | IS_CLIENT=$IS_CLIENT | MACHINE_ID=$MACHINE_ID"

cd /local/rdma || exit
git pull
mkdir -p build && cd build
rm -f CMakeCache.txt
cmake -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++ ..

make -j

sudo NODE_ID=$NODE_ID IS_CLIENT=$IS_CLIENT MACHINE_ID=$MACHINE_ID ./rdma