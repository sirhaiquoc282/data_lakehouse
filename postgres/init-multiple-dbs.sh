
set -e
set -u

function create_db() {
  local database=$1
  echo "Creating database '$database'..."
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE $database;
    GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
}

# POSTGRES_MULTIPLE_DATABASES = "metastore,ranger"
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
  echo "Multiple databases requested: $POSTGRES_MULTIPLE_DATABASES"
  for db in $(echo "$POSTGRES_MULTIPLE_DATABASES" | tr ',' ' '); do
    create_db "$db"
  done
  echo "Multiple databases created."
fi