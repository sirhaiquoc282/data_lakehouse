#!/bin/bash
set -e

echo "Checking schema..."
if ! schematool -dbType postgres -info > /dev/null 2>&1; then
echo "Initializing schema..."
schematool -dbType postgres -initSchema
else
echo "Schema already exists"
fi

echo "Starting Hive Metastore..."
exec hive --service metastore
