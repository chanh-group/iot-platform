FROM docker.io/library/debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY load_baseline /usr/local/bin/load_baseline
ENTRYPOINT ["/usr/local/bin/load_baseline"]
