#!/bin/bash
set -e pipefail

if [ "$POSTGRES_ROLE" = "primary" ]; then
    # Check if REPLICA_MAX_COUNT is a positive integer
    if ! [[ "$REPLICA_MAX_COUNT" =~ ^[1-9][0-9]*$ ]]; then
        echo "Error: REPLICA_MAX_COUNT must be a positive integer, got: $REPLICA_MAX_COUNT" >&2
        exit 1
    fi

    echo "Starting as PRIMARY..."

    # Call the original entrypoint with primary configuration
    exec /usr/local/bin/docker-entrypoint.sh postgres \
        -c listen_addresses='*' \
        -c wal_level=replica \
        -c max_wal_senders=$((REPLICA_MAX_COUNT * 2)) \
        -c max_replication_slots=$((REPLICA_MAX_COUNT * 2)) \
        -c max_connections=100 \
        -c checkpoint_completion_target=0.9 \
        -c wal_buffers=16MB \
        -c log_statement=all \
        -c log_min_duration_statement=1000

elif [ "$POSTGRES_ROLE" = "replica" ]; then
    if [ -z "${REPLICATION_SLOT}" ] || [ -z "${PRIMARY_HOST}" ]; then
        echo "Error: Required environment variables are missing" >&2
        [ -z "${REPLICATION_SLOT}" ] && echo "  - REPLICATION_SLOT is not set" >&2
        [ -z "${PRIMARY_HOST}" ] && echo "  - PRIMARY_HOST is not set" >&2
        exit 1
    fi

    # Check if REPLICATION_SLOT is a positive integer
    if ! [[ "$REPLICATION_SLOT" =~ ^[1-9][0-9]*$ ]]; then
        echo "Error: REPLICATION_SLOT must be a positive integer, got: $REPLICATION_SLOT" >&2
        exit 1
    fi

    # Check if SLOT_NUMBER is less than or equal to REPLICA_MAX_COUNT
    if [ "$REPLICATION_SLOT" -gt "$REPLICA_MAX_COUNT" ]; then
        echo "Error: Slot number ($REPLICATION_SLOT) exceeds REPLICA_MAX_COUNT ($REPLICA_MAX_COUNT)" >&2
        exit 1
    fi

    echo "Starting as REPLICA, replication slot ${REPLICATION_SLOT}..."

    # Wait for primary and run pg_basebackup
    until PGPASSWORD=$REPLICATOR_PASSWORD pg_basebackup \
        -h $PRIMARY_HOST \
        -D $PGDATA/ \
        --username=$REPLICATOR_USER \
        -v -R -X stream \
        -S "replica${REPLICATION_SLOT}_slot"; do
        echo "Waiting for connection to primary..."
        sleep 1s
    done

    echo "Backup done, starting replica..."

    # Start PostgreSQL as replica
    exec /usr/local/bin/docker-entrypoint.sh postgres \
        -c listen_addresses='*' \
        -c max_wal_senders=$((REPLICA_MAX_COUNT * 2)) \
        -c max_replication_slots=$((REPLICA_MAX_COUNT * 2)) \
        -c max_connections=100 \
        -c log_statement=all \
        -c log_min_duration_statement=1000

else
    echo "ERROR: POSTGRES_ROLE must be set to 'primary' or 'replica'"
    exit 1
fi
