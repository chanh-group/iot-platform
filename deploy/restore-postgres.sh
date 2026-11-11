#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "$#" -ne 2 ]; then
    echo "usage: $0 BACKUP_DIRECTORY NEW_DATABASE" >&2
    exit 2
fi
backup_dir=$1
new_database=$2
case "$new_database" in
    [a-z_]* ) ;;
    *) echo "database name must start with a lowercase letter or underscore" >&2; exit 2 ;;
esac
case "$new_database" in
    *[!a-z0-9_]*) echo "database name may contain only lowercase ASCII letters, digits and underscores" >&2; exit 2 ;;
esac
if [ ! -f "$backup_dir/postgres.dump" ] || [ ! -f "$backup_dir/SHA256SUMS" ]; then
    echo "PostgreSQL backup is incomplete" >&2
    exit 1
fi
(cd "$backup_dir" && sha256sum -c SHA256SUMS)

compose() {
    "$script_dir/compose-postgres.sh" "$@"
}
container=$(compose ps --all --quiet postgres)
if [ -z "$container" ] || [ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]; then
    echo "PostgreSQL container must be running" >&2
    exit 1
fi
version=$(docker exec --user 0:0 "$container" sh -ec '
    export PGPASSWORD="$(cat /run/secrets/postgres_password)"
    psql --host=127.0.0.1 --username="$POSTGRES_USER" --dbname=postgres --tuples-only --no-align --command="SHOW server_version"
')
backup_version=$(cat "$backup_dir/POSTGRES_VERSION")
case "$version:$backup_version" in
    18.*:18.*) ;;
    *) echo "restore requires a PostgreSQL 18 backup and target (backup=$backup_version target=$version)" >&2; exit 1 ;;
esac
existing=$(docker exec --user 0:0 "$container" sh -ec '
    export PGPASSWORD="$(cat /run/secrets/postgres_password)"
    psql --host=127.0.0.1 --username="$POSTGRES_USER" --dbname=postgres --tuples-only --no-align \
        --set=db="$1" --command="SELECT 1 FROM pg_database WHERE datname = :'\''db'\''"
' sh "$new_database")
if [ -n "$existing" ]; then
    echo "refusing to overwrite existing database: $new_database" >&2
    exit 1
fi

docker exec --user 0:0 "$container" sh -ec '
    export PGPASSWORD="$(cat /run/secrets/postgres_password)"
    createdb --host=127.0.0.1 --username="$POSTGRES_USER" "$1"
' sh "$new_database"
docker exec --user 0:0 -i "$container" sh -ec '
    export PGPASSWORD="$(cat /run/secrets/postgres_password)"
    pg_restore --host=127.0.0.1 --username="$POSTGRES_USER" --dbname="$1" --no-owner --no-acl
' sh "$new_database" < "$backup_dir/postgres.dump"
echo "PostgreSQL backup restored to new database $new_database; original database was not modified"
