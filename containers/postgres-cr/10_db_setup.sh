#!/bin/bash
set -euo pipefail

execute_sql() {
    export PGPASSWORD=$POSTGRES_PASSWORD
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" "$@"
}

if [ "$POSTGRES_ROLE" != "primary" ]; then
    echo "Skipping primary setup on a replica"
    exit 0
fi

echo "host replication all 0.0.0.0/0 scram-sha-256" >>${PGDATA}/pg_hba.conf

execute_sql <<-EOF
        CREATE USER $REPLICATOR_USER WITH REPLICATION ENCRYPTED PASSWORD '$REPLICATOR_PASSWORD';
EOF
echo "Created replication user ${REPLICATOR_USER}"

for i in $(seq 1 $REPLICA_MAX_COUNT); do
    execute_sql <<-EOF
        SELECT pg_create_physical_replication_slot('replica${i}_slot');
EOF
    echo "Created replication slot: replica${i}_slot"
done

execute_sql <<-EOF
    CREATE USER ${APP_USER} WITH 
        PASSWORD '${APP_PASSWORD}'
        CREATEDB
        CONNECTION LIMIT 100;
EOF
echo "Created application user ${APP_USER}"

execute_sql <<-EOF
    CREATE DATABASE ${APP_DATABASE} 
        OWNER ${APP_USER}
        ENCODING 'UTF8'
        LC_COLLATE 'C'
        LC_CTYPE 'C';
EOF
echo "Created database ${APP_DATABASE}"
