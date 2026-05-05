#!/bin/bash
set -e

echo "==> Running Ranger setup..."
cd /opt/ranger/admin
bash ./setup.sh || true

echo "==> Starting Ranger Admin..."
exec /opt/ranger/admin/ews/ranger-admin-services.sh start