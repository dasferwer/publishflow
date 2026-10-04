#!/bin/sh
set -eu

# Защита охватывает миграции и seed, которые выполняются раньше pytest.
python -m publishflow.test_safety
exec "$@"
