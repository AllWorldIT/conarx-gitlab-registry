#!/bin/bash
set -e

if [ -z "$KV_TYPE" ]; then
    echo "ERROR: KV_TYPE environment variable is not set"
    echo "Valid roles: redis, valkey/valkey, valkey"
    exit 1
fi

if [ "$KV_TYPE" = "valkey/valkey" ]; then
    KV_TYPE="valkey"
fi

if [ -z "$KV_ROLE" ]; then
    echo "ERROR: KV_ROLE environment variable is not set"
    echo "Valid roles: primary, replica, sentinel, cluster-node, cluster-starter"
    exit 1
fi

case "$KV_ROLE" in
primary | replica | sentinel | cluster-node | cluster-starter)
    echo "Starting KV in role: $KV_ROLE"
    ;;
*)
    echo "ERROR: Invalid KV_ROLE: $KV_ROLE"
    echo "Valid roles: primary, replica, sentinel, cluster-node, cluster-starter"
    exit 1
    ;;
esac

# Set default values for environment variables
export KV_ANNOUNCE_IP="${KV_ANNOUNCE_IP:-$(hostname)}"
export CLUSTER_INIT_MAX_ATTEMPTS="${CLUSTER_INIT_MAX_ATTEMPTS:-30}"
export CLUSTER_STARTER_KEEP_ALIVE="${CLUSTER_STARTER_KEEP_ALIVE:-true}"

KV_AUTH_TEMPLATE='
user default off
user ${KV_USERNAME} on >${KV_PASSWORD} allcommands allkeys'

SENTINEL_AUTH_TEMPLATE='
user default off
user ${KV_SENTINEL_USERNAME} on >${KV_SENTINEL_PASSWORD} +@all'

# Doc: https://github.com/redis/redis/blob/unstable/redis.conf
PRIMARY_TEMPLATE='
loglevel verbose

appendonly no
save ""

repl-diskless-sync yes
repl-diskless-sync-delay 0
repl-diskless-load swapdb

protected-mode no

replica-announce-ip ${KV_ANNOUNCE_IP}
replica-announce-port 6379

min-replicas-to-write 0
min-replicas-max-lag 10'

# Doc: https://github.com/redis/redis/blob/unstable/redis.conf
REPLICA_TEMPLATE='
loglevel verbose

appendonly no
save ""

repl-diskless-sync yes
repl-diskless-sync-delay 0
repl-diskless-load swapdb

protected-mode no

replicaof ${KV_PRIMARY_HOST} 6379

replica-announce-ip ${KV_ANNOUNCE_IP}
replica-announce-port 6379'

# Doc: https://github.com/redis/redis/blob/unstable/redis.conf
SENTINEL_TEMPLATE='
port 6379
loglevel verbose
dir /tmp

sentinel monitor ${KV_MAIN_NAME} ${KV_MONITOR_HOST} 6379 1
sentinel announce-ip ${KV_ANNOUNCE_IP}
# NOTE(prozlach): We need to re-use the Redis/ValKey port to make Gitlab
# Runner healthchecks happy:
sentinel announce-port 6379
sentinel announce-hostnames no
sentinel resolve-hostnames yes

sentinel down-after-milliseconds ${KV_MAIN_NAME} 1500
sentinel failover-timeout ${KV_MAIN_NAME} 3000
sentinel parallel-syncs ${KV_MAIN_NAME} 1

sentinel deny-scripts-reconfig yes'

# Doc: https://github.com/redis/redis/blob/unstable/redis.conf
CLUSTER_NODE_TEMPLATE='
loglevel verbose

cluster-enabled yes
cluster-config-file /etc/kv-nodes.conf
cluster-announce-ip ${KV_ANNOUNCE_IP}
cluster-announce-port 6379
cluster-node-timeout 2000

appendonly no
save ""

repl-diskless-sync yes
repl-diskless-sync-delay 0
repl-diskless-load swapdb

protected-mode no
min-replicas-to-write 0
min-replicas-max-lag 10'

# Function to handle primary role
configure_as_primary() {
    echo "Configuring KV as primary node..."
    local config_file="/etc/kv-server.conf"
    local template="$PRIMARY_TEMPLATE"

    if [ -n "$KV_USERNAME" ] && [ -n "$KV_PASSWORD" ]; then
        template="${PRIMARY_TEMPLATE}${KV_AUTH_TEMPLATE}"
    fi

    echo "$template" | envsubst >"$config_file"
    exec ${KV_TYPE}-server "$config_file"
}

# Function to handle replica role
configure_as_replica() {
    echo "Configuring KV as replica node..."

    if [ -z "$KV_PRIMARY_HOST" ]; then
        echo "ERROR: KV_PRIMARY_HOST must be set for replica role"
        exit 1
    fi

    local config_file="/etc/kv-server.conf"
    local template="$REPLICA_TEMPLATE"

    if [ -n "$KV_USERNAME" ] && [ -n "$KV_PASSWORD" ]; then
        template="${REPLICA_TEMPLATE}${KV_AUTH_TEMPLATE}"
    fi

    echo "$template" | envsubst >"$config_file"
    exec ${KV_TYPE}-server "$config_file"
}

# Function to handle sentinel role
configure_as_sentinel() {
    echo "Configuring KV Sentinel..."

    export KV_MONITOR_HOST="${KV_MONITOR_HOST:-kv-node1}"

    local config_file="/etc/kv-sentinel.conf"
    local template="$SENTINEL_TEMPLATE"

    if [ -n "$KV_SENTINEL_USERNAME" ] && [ -n "$KV_SENTINEL_PASSWORD" ]; then
        template="${SENTINEL_TEMPLATE}${SENTINEL_AUTH_TEMPLATE}"
    fi

    echo "$template" | envsubst >"$config_file"
    exec ${KV_TYPE}-sentinel "$config_file"
}

# Function to handle cluster node role
configure_as_cluster_node() {
    echo "Configuring KV as cluster node..."
    local config_file="/etc/kv-cluster.conf"

    echo "$CLUSTER_NODE_TEMPLATE" | envsubst >"$config_file"
    exec ${KV_TYPE}-server "$config_file"
}

# Function to handle cluster starter role
configure_as_cluster_starter() {
    echo "Starting KV Cluster initialization..."

    if [ -z "$KV_CLUSTER_NODES" ]; then
        echo "ERROR: KV_CLUSTER_NODES must be set for cluster-starter role"
        echo "Example: KV_CLUSTER_NODES='kv-node1,kv-node2,kv-node3'"
        exit 1
    fi

    IFS=',' read -ra KV_NODES <<<"$KV_CLUSTER_NODES"

    # Function to wait for KV node to be ready
    wait_for_kv() {
        local node=$1
        local max_attempts=$CLUSTER_INIT_MAX_ATTEMPTS
        local attempt=0

        echo "Waiting for $node to be ready..."

        while [ $attempt -lt $max_attempts ]; do
            if ${KV_TYPE}-cli -h "$node" -p 6379 ping 2>/dev/null | grep -q PONG; then
                echo "$node is ready!"
                return 0
            fi

            attempt=$((attempt + 1))
            sleep 1
        done

        echo "ERROR: $node is not responding after $max_attempts attempts"
        return 1
    }

    # Wait for all nodes to be ready
    for node in "${KV_NODES[@]}"; do
        if ! wait_for_kv "$node"; then
            echo "Cluster initialization failed - $node is not ready"
            exit 1
        fi
    done

    # Build node list with IPs
    NODE_LIST=""
    for node in "${KV_NODES[@]}"; do
        # Get the IP address of the node
        NODE_IP=$(getent hosts "$node" | awk '{ print $1 }')
        if [ -z "$NODE_IP" ]; then
            echo "Failed to resolve IP for $node"
            exit 1
        fi

        if [ -n "$NODE_LIST" ]; then
            NODE_LIST="$NODE_LIST "
        fi
        NODE_LIST="${NODE_LIST}${NODE_IP}:6379"
        echo "Resolved $node to $NODE_IP"
    done

    echo "Node list with IPs: $NODE_LIST"
    echo "Creating KV cluster..."

    # Create cluster with 0 replicas (hardcoded)
    ${KV_TYPE}-cli --cluster create $NODE_LIST --cluster-replicas 0 --cluster-yes

    if [ $? -eq 0 ]; then
        echo "KV cluster created successfully!"
    else
        echo "Failed to create KV cluster"
        exit 1
    fi

    echo "Verifying cluster status..."
    sleep 1

    for node in "${KV_NODES[@]}"; do
        echo "Checking node: $node"
        ${KV_TYPE}-cli -h "$node" -p 6379 cluster info | grep cluster_state
    done

    echo "Cluster nodes configuration:"
    ${KV_TYPE}-cli -h "${KV_NODES[0]}" -p 6379 cluster nodes

    echo "KV cluster initialization completed!"

    echo "Starting KV server to make health checks happy..."
    exec ${KV_TYPE}-server
}

# Main execution
case "$KV_ROLE" in
primary)
    configure_as_primary
    ;;
replica)
    configure_as_replica
    ;;
sentinel)
    configure_as_sentinel
    ;;
cluster-node)
    configure_as_cluster_node
    ;;
cluster-starter)
    configure_as_cluster_starter
    ;;
*)
    echo "Error: Unknown KV_ROLE: $KV_ROLE"
    exit 1
    ;;
esac
