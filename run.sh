#!/bin/bash

# Default values
CLUSTER_CONTROLLERS=${1:-3}
LOCAL_CONTROLLERS=${2:-2}
REPLICA_CONTROLLERS=${3:-1}
AGENTS_PER_NODE=${4:-2}

if [ "$1" == "--help" ]; then
    echo "Usage: ./run.sh <cluster_controllers> <local_controllers> <replicas> <agents_per_node>"
    exit 0
fi

PORT_START=8055
PORT_END=8100

echo "Finding available nodes..."

AVAILABLE=$(/share/ifi/available-nodes.sh)
CUDA=$(/share/ifi/list-cuda-hosts.sh)

# Remove CUDA nodes
NODES=$(echo "$AVAILABLE" | grep -v -f <(echo "$CUDA"))

NODE_LIST=($NODES)

TOTAL_NODES=$((CLUSTER_CONTROLLERS + LOCAL_CONTROLLERS + REPLICA_CONTROLLERS))

if [ ${#NODE_LIST[@]} -lt $TOTAL_NODES ]; then
    echo "Not enough available non-CUDA nodes."
    exit 1
fi

CLUSTER_CONTROLLER_NODES=("${NODE_LIST[@]:0:$CLUSTER_CONTROLLERS}")
LOCAL_CONTROLLER_NODES=("${NODE_LIST[@]:$CLUSTER_CONTROLLERS:$LOCAL_CONTROLLERS}")
REPLICA_CONTROLLER_NODES=("${NODE_LIST[@]:$((CLUSTER_CONTROLLERS + LOCAL_CONTROLLERS)):$REPLICA_CONTROLLERS}")

echo "Cluster controllers will run on: ${CLUSTER_CONTROLLER_NODES[@]}"
echo "Local controllers: ${LOCAL_CONTROLLER_NODES[@]}"
echo "Replica local controllers: ${REPLICA_CONTROLLER_NODES[@]}"

PROJECT_DIR=$(pwd)

if [ ! -d "$PROJECT_DIR/venv" ]; then
    echo "venv not found. Please create and install requirements first."
    exit 1
fi

echo "Assigning ports..."

declare -A NODE_PORTS

ALL_NODES=("${CLUSTER_CONTROLLER_NODES[@]}" "${LOCAL_CONTROLLER_NODES[@]}" "${REPLICA_CONTROLLER_NODES[@]}")

for NODE in "${ALL_NODES[@]}"; do
        echo "Searching free port on $NODE..."

        FOUND_PORT=""

        for ((p=$PORT_START; p<=$PORT_END; p++)); do
            if ! ssh $NODE "ss -tuln | grep -q ':$p '"; then
                FOUND_PORT=$p
                break
            fi
        done
    
    if [ -z "$FOUND_PORT" ]; then
        echo "No free port found on $NODE"
        exit 1
    fi

    NODE_PORTS[$NODE]=$FOUND_PORT
    echo "$NODE will use port $FOUND_PORT"
done

CLUSTER_ADDRESSES=()

for NODE in "${CLUSTER_CONTROLLER_NODES[@]}"; do
    PORT=${NODE_PORTS[$NODE]}
    CLUSTER_ADDRESSES+=("$NODE:$PORT")
done

CLUSTER_STRING=$(IFS=,; echo "${CLUSTER_ADDRESSES[*]}")

echo "Starting cluster controllers..."

for NODE in "${CLUSTER_CONTROLLER_NODES[@]}"; do
    PORT=${NODE_PORTS[$NODE]}

    ssh -n -f $NODE "
        cd $PROJECT_DIR &&
        MY_ADDRESS=$NODE:$PORT \
        CLUSTER_NODES=$CLUSTER_STRING \
        nohup venv/bin/python -m uvicorn cluster_controller:app \
            --host 0.0.0.0 \
            --port $PORT \
            > \$(hostname)_cluster.log 2>&1
    "
done

sleep 5

echo "Starting local controllers..."

for NODE in "${LOCAL_CONTROLLER_NODES[@]}"; do
    PORT=${NODE_PORTS[$NODE]}

    ssh -n -f $NODE "
        cd $PROJECT_DIR &&
        nohup venv/bin/python -u local_controller.py \
            $AGENTS_PER_NODE \
            $CLUSTER_STRING \
            $PORT \
            > \$(hostname)_local.log 2>&1
    "
done

echo "Starting replica local controllers..."

for NODE in "${REPLICA_CONTROLLER_NODES[@]}"; do
    PORT=${NODE_PORTS[$NODE]}

    ssh -n -f $NODE "
    cd $PROJECT_DIR &&
    nohup venv/bin/python -u local_controller.py \
        0 \
        $CLUSTER_STRING \
        $PORT \
        > \$(hostname)_replica.log 2>&1
    "
done

echo "Deployment complete."
echo "Cluster controllers started:"
for NODE in "${CLUSTER_CONTROLLER_NODES[@]}"; do
    echo "  $NODE:${NODE_PORTS[$NODE]}"
done

echo "Worker nodes started:"
for NODE in "${LOCAL_CONTROLLER_NODES[@]}"; do
    echo "$NODE:${NODE_PORTS[$NODE]}"
done

echo "Replica nodes started:"
for NODE in "${REPLICA_CONTROLLER_NODES[@]}"; do
    echo "$NODE:${NODE_PORTS[$NODE]}"
done

INFO_FILE="cluster_info.json"

echo "Writing cluster information to $INFO_FILE"

echo "{" > $INFO_FILE

echo "  \"cluster_controllers\": [" >> $INFO_FILE
for NODE in "${CLUSTER_CONTROLLER_NODES[@]}"; do
    echo "    \"${NODE}:${NODE_PORTS[$NODE]}\"," >> $INFO_FILE
done
sed -i '$ s/,$//' $INFO_FILE
echo "  ]," >> $INFO_FILE

echo "  \"local_controllers\": [" >> $INFO_FILE
for NODE in "${LOCAL_CONTROLLER_NODES[@]}"; do
    echo "    \"${NODE}:${NODE_PORTS[$NODE]}\"," >> $INFO_FILE
done
sed -i '$ s/,$//' $INFO_FILE
echo "  ]," >> $INFO_FILE

echo "  \"replicas\": [" >> $INFO_FILE
for NODE in "${REPLICA_CONTROLLER_NODES[@]}"; do
    echo "    \"${NODE}:${NODE_PORTS[$NODE]}\"," >> $INFO_FILE
done
sed -i '$ s/,$//' $INFO_FILE
echo "  ]" >> $INFO_FILE

echo "}" >> $INFO_FILE
