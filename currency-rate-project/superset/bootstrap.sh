#!/bin/bash
set -e

superset db upgrade

superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname Superset \
  --email admin@example.com \
  --password "$SUPERSET_ADMIN_PWD" || true
  
superset init

superset import-directory /app/assets/

exec /usr/bin/run-server.sh