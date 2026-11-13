#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
password_file=${IOT_POSTGRES_PASSWORD_FILE:-"$script_dir/secrets/postgres_password"}
if [ ! -f "$password_file" ] || [ ! -r "$password_file" ]; then
    echo "PostgreSQL password secret is not readable: $password_file" >&2
    exit 1
fi
password=$(tr -d '\r\n' < "$password_file")
case "$password" in
    ''|*[!0123456789abcdefABCDEF]*)
        echo "PostgreSQL password must be non-empty hexadecimal (generate with openssl rand -hex 32)" >&2
        exit 1
        ;;
esac

database=${IOT_DATABASE_NAME:-iot}
case "$database" in
    [a-z_]* ) ;;
    *)
        echo "IOT_DATABASE_NAME must start with a lowercase letter or underscore" >&2
        exit 1
        ;;
esac
case "$database" in
    *[!a-z0-9_]*)
        echo "IOT_DATABASE_NAME may contain only lowercase ASCII letters, digits and underscores" >&2
        exit 1
        ;;
esac

IOT_POSTGRES_PASSWORD_FILE=$(realpath -e -- "$password_file")
export IOT_POSTGRES_PASSWORD_FILE
export IOT_DATABASE_URL="postgresql://iot:${password}@postgres:5432/${database}?sslmode=disable"
export IOT_DATABASE_NAME="$database"
export IOT_POSTGRES_DATABASE=${IOT_POSTGRES_DATABASE:-$database}
export IOT_SERVER_IMAGE=${IOT_SERVER_IMAGE:-iot-server:local}
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-thingsboardrust-postgres}
exec docker compose --project-name "$COMPOSE_PROJECT_NAME" --file "$script_dir/compose.postgres.yml" "$@"
