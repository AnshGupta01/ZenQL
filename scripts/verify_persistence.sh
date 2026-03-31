#!/bin/bash
set -e

# Configuration
SERVER_BIN="./bin/flexql_server"
BENCH_BIN="./bin/flexql_master_benchmark"
DATA_DIR="./data/tables"
PORT=9000

echo "[[ Persistence Verification Test ]]"

# 1. Clean up existing data
echo "1. Cleaning up existing data..."
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

# 2. Start server in background
echo "2. Starting FlexQL server on port $PORT..."
$SERVER_BIN $PORT > server_test.log 2>&1 &
SERVER_PID=$!
sleep 1

# 3. Insert some data
echo "3. Inserting data..."
$BENCH_BIN 100000 2>/dev/null > /dev/null

# 4. Force kill the server (SIGKILL)
echo "4. Killing server with SIGKILL (simulating crash)..."
kill -9 $SERVER_PID
sleep 1

# 5. Restart server
echo "5. Restarting server..."
$SERVER_BIN $PORT >> server_test.log 2>&1 &
RESTARTED_PID=$!
sleep 1

# 6. Verify data exists
echo "6. Verifying data persistence..."
# We'll use the benchmark tool in unit-test mode to verify
if $BENCH_BIN --unit-test | grep -q "passed"; then
    echo "[PASS] Persistence verification successful!"
else
    echo "[FAIL] Persistence verification failed!"
    kill $RESTARTED_PID
    exit 1
fi

# 7. Clean up
echo "7. Cleaning up..."
kill $RESTARTED_PID
echo "Test complete."
