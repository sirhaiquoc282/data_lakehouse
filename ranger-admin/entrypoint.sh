#!/bin/bash
set -e

echo "==> Waiting for PostgreSQL..."
until pg_isready -h "$RANGER_DB_HOST" -p "$RANGER_DB_PORT" -U "$RANGER_DB_USER" 2>/dev/null; do
  sleep 3
done

echo "==> Writing install.properties..."
cat > /opt/ranger/admin/install.properties <<PROPS
DB_FLAVOR=POSTGRES
SQL_CONNECTOR_JAR=/opt/ranger/ews/webapp/WEB-INF/lib/postgresql-42.7.3.jar
db_root_user=${RANGER_DB_USER}
db_root_password=${RANGER_DB_PASS}
db_host=${RANGER_DB_HOST}
db_name=${RANGER_DB_NAME}
db_user=${RANGER_DB_USER}
db_password=${RANGER_DB_PASS}
rangerAdmin_password=${RANGER_ADMIN_PASSWORD:-rangerR0cks!}
rangerTagsync_password=${RANGER_ADMIN_PASSWORD:-rangerR0cks!}
rangerUsersync_password=${RANGER_ADMIN_PASSWORD:-rangerR0cks!}
keyadmin_password=${RANGER_ADMIN_PASSWORD:-rangerR0cks!}
audit_store=db
policymgr_external_url=http://ranger-admin:6080
policymgr_http_enabled=true
RANGER_ADMIN_LOG_DIR=/var/log/ranger/admin
PROPS

mkdir -p /var/log/ranger/admin

echo "==> Running Ranger setup..."
cd /opt/ranger/admin
python3 setup.py || true

echo "==> Starting Ranger Admin..."
exec /opt/ranger/admin/ews/ranger-admin-services.sh start