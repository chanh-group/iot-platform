#!/bin/sh
set -eu
umask 077

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "$#" -ne 1 ]; then
    echo "usage: $0 BACKUP_DIRECTORY" >&2
    exit 2
fi
backup_dir=$1
case "$backup_dir" in
    /*) ;;
    *) backup_dir="$PWD/$backup_dir" ;;
esac
if [ -e "$backup_dir" ] || [ -L "$backup_dir" ]; then
    echo "backup destination already exists: $backup_dir" >&2
    exit 1
fi

staging=
cleanup() {
    result=$?
    trap - EXIT HUP INT TERM
    if [ -n "$staging" ] && [ -e "$staging" ]; then rm -rf -- "$staging" || true; fi
    exit "$result"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

compose() {
    "$script_dir/compose-postgres.sh" "$@"
}
container=$(compose ps --all --quiet postgres)
if [ -z "$container" ]; then
    echo "no PostgreSQL container found in this Compose project" >&2
    exit 1
fi
running=$(docker inspect --format '{{.State.Running}}' "$container")
if [ "$running" != true ]; then
    echo "PostgreSQL container is not running" >&2
    exit 1
fi

parent=$(dirname -- "$backup_dir")
mkdir -p "$parent"
staging=$(mktemp -d "$backup_dir.tmp.XXXXXX")
chmod 0700 "$staging"
database=${IOT_DATABASE_NAME:-iot}
case "$database" in
    [a-z_]* ) ;;
    *) echo "invalid IOT_DATABASE_NAME" >&2; exit 1 ;;
esac
case "$database" in
    *[!a-z0-9_]*) echo "invalid IOT_DATABASE_NAME" >&2; exit 1 ;;
esac

docker exec --user 0:0 "$container" sh -ec '
    export PGPASSWORD="$(cat /run/secrets/postgres_password)"
    pg_dump --host=127.0.0.1 --username="$POSTGRES_USER" --format=custom --no-owner --no-acl "$1"
' sh "$database" > "$staging/postgres.dump"
docker exec --user 0:0 "$container" sh -ec '
    export PGPASSWORD="$(cat /run/secrets/postgres_password)"
    psql --host=127.0.0.1 --username="$POSTGRES_USER" --dbname="$1" --tuples-only --no-align --command="SHOW server_version"
' sh "$database" > "$staging/POSTGRES_VERSION"
printf '%s\n' "$database" > "$staging/DATABASE"
(cd "$staging" && sha256sum postgres.dump POSTGRES_VERSION DATABASE > SHA256SUMS)
chmod 0600 "$staging"/*
mv -nT -- "$staging" "$backup_dir"
if [ -e "$staging" ]; then
    echo "backup destination appeared during the operation: $backup_dir" >&2
    exit 1
fi
staging=
echo "PostgreSQL backup written to $backup_dir"
