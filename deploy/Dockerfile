FROM docker.io/oven/bun:1.4.2-alpine AS web
WORKDIR /web
COPY web/package.json web/bun.lock ./
RUN bun install --frozen-lockfile
COPY web/ ./
RUN bun run --bun build

FROM docker.io/library/rust:1.98-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock build.rs build_support.rs ./
COPY src ./src
COPY migrations ./migrations
COPY --from=web /web/dist ./web/dist
RUN cargo build --release --locked

FROM docker.io/library/debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 iot \
    && useradd --uid 10001 --gid iot --create-home --shell /usr/sbin/nologin iot \
    && install -d -o iot -g iot /app/data \
    && find / -xdev -type f -perm /6000 -exec chmod a-s {} + \
    && test -z "$(find / -xdev -type f -perm /6000 -print -quit)"
WORKDIR /app
COPY --from=build --chown=root:root /src/target/release/iot-server /usr/local/bin/iot-server
RUN chmod 0755 /usr/local/bin/iot-server
USER 10001:10001
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/iot-server"]
CMD ["run", "--config", "/app/conf.toml"]
