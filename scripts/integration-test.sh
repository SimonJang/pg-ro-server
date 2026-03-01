#!/usr/bin/env bash
#
# Full integration test for postgres-mcp-server.
#
# Spins up a Docker PostgreSQL container with a bookstore database,
# builds the MCP server, connects via the MCP SDK client, exercises
# all tools (list-tables, describe-table, query), and tears down.
#
# Prerequisites: Docker
# Usage: npm run test:integration
#
set -euo pipefail

CONTAINER_NAME="postgres-mcp-integration"
DB_PORT=5435
DB_USER="testuser"
DB_PASS="testpass"
DB_NAME="bookstore"
DB_URL="postgresql://${DB_USER}:${DB_PASS}@localhost:${DB_PORT}/${DB_NAME}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cleanup() {
  echo ""
  echo "Cleaning up..."
  docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}
trap cleanup EXIT

# ── 1. Start PostgreSQL ──────────────────────────────────────────────

echo "==> Removing any existing container..."
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "==> Starting PostgreSQL container on port ${DB_PORT}..."
docker run -d --name "$CONTAINER_NAME" \
  -e POSTGRES_USER="$DB_USER" \
  -e POSTGRES_PASSWORD="$DB_PASS" \
  -e POSTGRES_DB="$DB_NAME" \
  -p "${DB_PORT}:5432" \
  postgres:17 >/dev/null

echo "==> Waiting for PostgreSQL to be ready..."
for i in $(seq 1 30); do
  if docker exec "$CONTAINER_NAME" pg_isready -U "$DB_USER" -d "$DB_NAME" >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Timed out waiting for PostgreSQL"
    exit 1
  fi
  sleep 1
done

# ── 2. Seed database ─────────────────────────────────────────────────

psql_exec() {
  docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -c "$1" >/dev/null
}

echo "==> Creating schema..."

psql_exec "CREATE TABLE authors (
  id serial PRIMARY KEY,
  name text NOT NULL,
  country text,
  born_year int
);"

psql_exec "CREATE TABLE books (
  id serial PRIMARY KEY,
  title text NOT NULL,
  author_id int REFERENCES authors(id),
  genre text,
  published_year int,
  price numeric(6,2)
);"

psql_exec "CREATE TABLE customers (
  id serial PRIMARY KEY,
  name text NOT NULL,
  email text UNIQUE,
  joined_date date DEFAULT CURRENT_DATE
);"

psql_exec "CREATE TABLE orders (
  id serial PRIMARY KEY,
  customer_id int REFERENCES customers(id),
  book_id int REFERENCES books(id),
  quantity int DEFAULT 1,
  ordered_at timestamp DEFAULT NOW()
);"

echo "==> Seeding data..."

psql_exec "INSERT INTO authors (name, country, born_year) VALUES
  ('Gabriel Garcia Marquez', 'Colombia', 1927),
  ('Haruki Murakami', 'Japan', 1949),
  ('Chimamanda Ngozi Adichie', 'Nigeria', 1977),
  ('Jorge Luis Borges', 'Argentina', 1899),
  ('Ursula K. Le Guin', 'USA', 1929);"

psql_exec "INSERT INTO books (title, author_id, genre, published_year, price) VALUES
  ('One Hundred Years of Solitude', 1, 'Magical Realism', 1967, 14.99),
  ('Love in the Time of Cholera', 1, 'Romance', 1985, 12.99),
  ('Norwegian Wood', 2, 'Literary Fiction', 1987, 13.50),
  ('Kafka on the Shore', 2, 'Magical Realism', 2002, 15.99),
  ('Half of a Yellow Sun', 3, 'Historical Fiction', 2006, 14.50),
  ('Americanah', 3, 'Literary Fiction', 2013, 16.00),
  ('Ficciones', 4, 'Short Stories', 1944, 11.99),
  ('The Aleph', 4, 'Short Stories', 1949, 10.99),
  ('The Left Hand of Darkness', 5, 'Science Fiction', 1969, 13.99),
  ('A Wizard of Earthsea', 5, 'Fantasy', 1968, 12.50);"

psql_exec "INSERT INTO customers (name, email, joined_date) VALUES
  ('Emma Wilson', 'emma@example.com', '2025-01-15'),
  ('James Chen', 'james@example.com', '2025-03-22'),
  ('Sofia Rodriguez', 'sofia@example.com', '2025-06-10'),
  ('Liam O''Brien', 'liam@example.com', '2025-09-01');"

psql_exec "INSERT INTO orders (customer_id, book_id, quantity, ordered_at) VALUES
  (1, 1, 2, '2025-02-10 14:30:00'),
  (1, 3, 1, '2025-02-10 14:30:00'),
  (2, 4, 1, '2025-04-05 09:15:00'),
  (2, 6, 3, '2025-04-05 09:15:00'),
  (3, 1, 1, '2025-07-20 16:45:00'),
  (3, 9, 2, '2025-07-20 16:45:00'),
  (4, 5, 1, '2025-09-15 11:00:00'),
  (4, 1, 1, '2025-09-15 11:00:00');"

# ── 3. Build & test ──────────────────────────────────────────────────

echo "==> Building MCP server..."
npm run build --prefix "$PROJECT_DIR" >/dev/null 2>&1

echo "==> Running MCP client tests..."
echo ""
node "$SCRIPT_DIR/integration-test.mjs" "$DB_URL" "$PROJECT_DIR/dist/index.js"
