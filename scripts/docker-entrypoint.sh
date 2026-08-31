#!/bin/sh
set -eu

alembic upgrade head
python -m publishflow.seed

exec "$@"
