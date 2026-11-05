# Local SQLite Compose

Build the image and prepare the bind-mounted data directory before the first start:

```sh
docker build -f deploy/Dockerfile -t iot-server:local .
deploy/prepare-data.sh
docker compose -f deploy/compose.sqlite.yml up -d
```

The server runs as UID/GID `10001`; `prepare-data.sh` creates `deploy/data` with that owner. It checks any existing directory on the host before invoking Docker, so a non-empty directory with the wrong owner is rejected without a `:Z` mount or SELinux relabel. Back up existing data and fix ownership deliberately. Do not let Compose create the initial bind directory as root.

Both published ports bind to `127.0.0.1`. MQTT is plaintext: do not expose it to the Internet without a TLS proxy. `cookie_secure = false` is for local HTTP only; enable it when serving over HTTPS.

To stop the service gracefully, run `docker compose -f deploy/compose.sqlite.yml down`.
