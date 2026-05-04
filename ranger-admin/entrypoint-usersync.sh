#!/bin/bash
set -e

echo "==> Waiting for Ranger Admin at ${RANGER_ADMIN_URL}..."
until curl -sf "${RANGER_ADMIN_URL}/login.jsp" > /dev/null 2>&1; do
  echo "   Not ready, retrying in 10s..."
  sleep 10
done

echo "==> Writing usersync install.properties..."
mkdir -p /var/log/ranger/usersync

cat > /opt/ranger/usersync/install.properties <<PROPS
POLICY_MGR_URL=${RANGER_ADMIN_URL}
SYNC_SOURCE=${SYNC_SOURCE:-unix}
SYNC_INTERVAL=1
unix_user=root
unix_group=root
logdir=/var/log/ranger/usersync
PROPS

echo "==> Running usersync setup..."
cd /opt/ranger/usersync
python3 setup.py || true

echo "==> Starting Ranger UserSync..."
exec /opt/ranger/usersync/ranger-usersync-services.sh start