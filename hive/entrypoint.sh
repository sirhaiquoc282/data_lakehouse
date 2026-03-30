#!/bin/bash
set -e

echo "==> Initialising Hive schema in PostgreSQL..."
/opt/hive/bin/schematool -dbType postgres -initOrUpgradeSchema  4.0.0 --verbose || true

echo "==> Starting Hive Metastore..."
exec /opt/hive/bin/hive --service metastore