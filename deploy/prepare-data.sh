#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
data_dir=${1:-"$script_dir/data"}

if [ -L "$data_dir" ]; then
    echo "refusing to prepare a symlink as the data directory" >&2
    exit 1
fi
mkdir -p "$data_dir"

# Reject unsafe existing data before Docker's :Z bind mount can relabel it.
owner=$(stat -c %u:%g -- "$data_dir")
if [ "$owner" != "10001:10001" ]; then
    if [ ! -r "$data_dir" ] || [ ! -x "$data_dir" ]; then
        echo "cannot inspect data directory without changing it; refusing" >&2
        exit 1
    fi
    if ! contents=$(find "$data_dir" -mindepth 1 -maxdepth 1 -print -quit); then
        echo "cannot inspect data directory; refusing" >&2
        exit 1
    fi
    if [ -n "$contents" ]; then
        echo "refusing to change ownership of non-empty /data; back it up and fix ownership manually" >&2
        exit 1
    fi
fi

docker run --rm --user 0:0 -v "$data_dir:/data:Z" --entrypoint /bin/sh iot-server:local -c '
    owner=$(stat -c %u:%g /data)
    if [ "$owner" = 10001:10001 ]; then
        chmod 0750 /data
        exit 0
    fi
    chown 10001:10001 /data
    chmod 0750 /data
'
