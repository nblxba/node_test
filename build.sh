#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TON_DIR="$HOME/ton"

# Clone TON if not present
if [ ! -d "$TON_DIR" ]; then
    echo "Cloning TON source..."
    git clone --recursive https://github.com/ton-blockchain/ton.git "$TON_DIR"
fi

# Apply patches
echo "Applying patches..."
cd "$TON_DIR"
git checkout -- .
for patch in "$SCRIPT_DIR/patches/"*.patch; do
    echo "Applying: $(basename $patch)"
    git apply "$patch"
done

# Build
echo "Building validator-engine..."
mkdir -p build && cd build
cmake -GNinja .. \
    -DCMAKE_C_COMPILER=clang-21 \
    -DCMAKE_CXX_COMPILER=clang++-21 \
    -DTON_USE_JEMALLOC=ON \
    -DCMAKE_BUILD_TYPE=Release
ninja validator-engine

echo "Done: $TON_DIR/build/validator-engine/validator-engine"
./validator-engine/validator-engine -V
