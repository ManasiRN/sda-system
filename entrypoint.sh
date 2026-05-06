#!/bin/sh
# Run DB initialization (idempotent) then hand off to the real command.
# Set SKIP_DB_INIT=1 to bypass (used by flower and beat which don't write to the DB).
set -e

if [ "${SKIP_DB_INIT:-0}" = "0" ]; then
    python -m sda_system.db.init_db
fi

exec "$@"
