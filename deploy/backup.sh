#!/bin/sh
set -eu
umask 077

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "$#" -ne 1 ]; then
    echo "usage: $0 BACKUP_DIRECTORY" >&2
    exit 2
fi

data_dir=${IOT_DATA_DIR:-"$script_dir/data"}
case "$data_dir" in
    /*) ;;
    *) data_dir="$script_dir/$data_dir" ;;
esac
if [ ! -d "$data_dir" ]; then
    echo "data directory not found: $data_dir" >&2
    exit 1
fi
expected_data_dir=$(realpath -e -- "$data_dir")
backup_dir=$1
case "$backup_dir" in
    /*) ;;
    *) backup_dir="$PWD/$backup_dir" ;;
esac

if [ -e "$backup_dir" ] || [ -L "$backup_dir" ]; then
    echo "backup destination already exists: $backup_dir" >&2
    exit 1
fi
parent=$(dirname -- "$backup_dir")
mkdir -p "$parent"

compose_file=$script_dir/compose.sqlite.yml
container_ids=$(cd "$script_dir" && docker compose -f "$compose_file" ps --all --quiet iot-server)
if [ -z "$container_ids" ]; then
    echo "no iot-server container found in this Compose project; refusing backup" >&2
    exit 1
fi
for container_id in $container_ids; do
    mounted_data_dir=$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/app/data"}}{{.Source}}{{end}}{{end}}' "$container_id")
    if [ -z "$mounted_data_dir" ] || [ "$(realpath -e -- "$mounted_data_dir")" != "$expected_data_dir" ]; then
        echo "Compose container $container_id does not mount the requested data directory; refusing backup" >&2
        exit 1
    fi
done

# Compose stop sends SIGTERM and waits up to stop_grace_period before returning.
(cd "$script_dir" && docker compose -f "$compose_file" stop iot-server)
for container_id in $container_ids; do
    running=$(docker inspect --format '{{.State.Running}}' "$container_id")
    if [ "$running" != false ]; then
        echo "Compose container $container_id is still running; refusing backup" >&2
        exit 1
    fi
done

staging="$backup_dir.tmp.$$"
mkdir -m 700 "$staging"
trap 'rm -rf -- "$staging"' EXIT HUP INT TERM

# Read as root inside the runtime image; hold the app's flock-compatible lock
# through the complete copy so another server cannot restart midway through it.
docker run --rm --user 0:0 \
    --env "BACKUP_UID=$(id -u)" --env "BACKUP_GID=$(id -g)" \
    --volume "$data_dir:/data:Z" --volume "$staging:/backup:Z" \
    --entrypoint /usr/bin/flock iot-server:local \
    -n /data/.lock /bin/sh -c '
        set -eu
        data_root=${BACKUP_DATA_ROOT:-/data}
        backup_root=${BACKUP_OUTPUT_ROOT:-/backup}
        for file in iot.db iot.db-wal iot.db-shm runtime.toml; do
            if [ -f "$data_root/$file" ]; then
                cp -- "$data_root/$file" "$backup_root/$file"
                chmod 0600 "$backup_root/$file"
            fi
        done
        test -f "$backup_root/iot.db"
        chown -R "$BACKUP_UID:$BACKUP_GID" "$backup_root"
        chmod 0700 "$backup_root"
    '

set -- iot.db
for file in iot.db-wal iot.db-shm runtime.toml; do
    if [ -f "$staging/$file" ]; then set -- "$@" "$file"; fi
done
(cd "$staging" && sha256sum "$@" > SHA256SUMS)
chmod 0600 "$staging/SHA256SUMS"
mv -- "$staging" "$backup_dir"
trap - EXIT HUP INT TERM
echo "SQLite backup written to $backup_dir"
