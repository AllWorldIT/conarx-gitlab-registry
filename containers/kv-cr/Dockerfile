# check=skip=SecretsUsedInArgOrEnv

ARG KV_TYPE=UNSET
FROM ${KV_TYPE}:bookworm 

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gettext-base \
        && \
    rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /usr/local/bin/custom-entrypoint.sh
RUN chmod +x /usr/local/bin/custom-entrypoint.sh

EXPOSE 6379

ENTRYPOINT ["/usr/local/bin/custom-entrypoint.sh"]
