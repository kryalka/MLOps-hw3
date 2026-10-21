#!/usr/bin/env bash
set -euo pipefail
airflow db migrate

if ! airflow users list --output json | python -c 'import json,sys; sys.exit(not any(u["username"] == "admin" for u in json.load(sys.stdin)))'; then
  airflow users create --username admin --password admin --firstname Admin \
    --lastname User --role Admin --email admin@example.com
fi
exec airflow standalone
