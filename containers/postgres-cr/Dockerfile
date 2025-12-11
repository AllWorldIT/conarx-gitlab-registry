# check=skip=SecretsUsedInArgOrEnv

ARG PG_VERSION=UNSET
FROM postgres:${PG_VERSION}-trixie

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash \
        && \
    rm -rf /var/lib/apt/lists/*

# Copy initialization script for primary
COPY 10_db_setup.sh /docker-entrypoint-initdb.d/10_db_setup.sh
RUN chmod +x /docker-entrypoint-initdb.d/10_db_setup.sh

# Copy custom entrypoint script
COPY entrypoint.sh /usr/local/bin/custom-entrypoint.sh
RUN chmod +x /usr/local/bin/custom-entrypoint.sh

# Set default environment variables
ENV POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=Maryna \
    POSTGRES_INITDB_ARGS="--auth=scram-sha-256 --encoding=UTF8 --locale=C --data-checksums" \
    REPLICATOR_USER=bozydar \
    REPLICATOR_PASSWORD=ReplicatedBozydar \
    REPLICA_MAX_COUNT=10 \
    APP_USER=boryna \
    APP_PASSWORD=Chleboslaw \
    APP_DATABASE=registry_test

# Expose PostgreSQL port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD pg_isready -U ${POSTGRES_USER}

# Use custom entrypoint
ENTRYPOINT ["/usr/local/bin/custom-entrypoint.sh"]
CMD ["postgres"]
