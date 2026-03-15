#!/bin/bash

echo "Finding available nodes..."

AVAILABLE=$(/share/ifi/available-nodes.sh)
CUDA=$(/share/ifi/list-cuda-hosts.sh)

NODES=$(echo "$AVAILABLE" | grep -v -f <(echo "$CUDA"))

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Stopping processes and cleaning logs..."

for NODE in $NODES; do
    echo "Cleaning $NODE..."

    ssh $NODE "
        cd $PROJECT_DIR 2>/dev/null || exit 0

        # Kill only your project processes
        pkill -f 'uvicorn cluster_controller:app' 2>/dev/null
        pkill -f local_controller.py 2>/dev/null
        pkill -f agent.py 2>/dev/null

        # Remove logs
        rm -f *_cluster.log *_local.log *_replica.log *_local_restarted.log *_cluster_restarted.log *_restarted.log

        # Remove raft persistent state
        rm -f *_raft_state.json

        # Remove cluster info
        rm -f cluster_info.json

        # Remove labaled data
        if [ -d \"labeled-data\" ]; then
            rm -rf labeled-data
            echo \"Removed labeled-data on $NODE\"
        fi
    "
done

# Also clean local node
echo "Cleaning local node..."
pkill -f 'uvicorn cluster_controller:app' 2>/dev/null
pkill -f local_controller.py 2>/dev/null
pkill -f agent.py 2>/dev/null

rm -f *_cluster.log *_local.log *_replica.log *_local_restarted.log *_cluster_restarted.log *_restarted.log
rm -f *_raft_state.json
rm -f cluster_info.json

if [ -d "labeled-data" ]; then
    rm -rf labeled-data
fi

echo "Cleanup complete."