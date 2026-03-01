# postgres-mcp-server

A read-only [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server that exposes PostgreSQL databases. It allows LLM clients (like Claude Desktop or Claude Code) to explore your database schema and run read-only SQL queries.

This project is based on the original [`@modelcontextprotocol/server-postgres`](https://github.com/modelcontextprotocol/servers-archived/tree/main/src/postgres) reference server, which has since been [archived and deprecated](https://www.npmjs.com/package/@modelcontextprotocol/server-postgres). The original server contained a [SQL injection vulnerability](https://securitylabs.datadoghq.com/articles/mcp-vulnerability-case-study-SQL-injection-in-the-postgresql-mcp-server/) that allowed attackers to bypass the read-only transaction by injecting multi-statement payloads (e.g. `COMMIT; DROP TABLE ...`). This project fixes that vulnerability by using **prepared statements** to enforce single-statement execution and **destroying connections** after each query to prevent session state poisoning.

## Features

- **Read-only by design** — all queries run inside a `READ ONLY` transaction and use prepared statements to prevent multi-statement SQL injection
- **Session isolation** — database connections are destroyed after each query to prevent session state poisoning
- **Schema exploration** — list tables, describe columns, and browse table schemas as MCP resources

## Installation

No manual install needed — use `npx` to run directly:

```bash
npx @simonja/postgres-mcp-server "postgresql://user:password@localhost:5432/mydb"
```

Or install globally:

```bash
npm install -g @simonja/postgres-mcp-server
postgres-mcp-server "postgresql://user:password@localhost:5432/mydb"
```

The server communicates over stdio, so it's designed to be launched by an MCP client rather than run directly.

## Usage

### Claude Desktop

Add to your Claude Desktop config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "postgres": {
      "command": "npx",
      "args": [
        "@simonja/postgres-mcp-server",
        "postgresql://user:password@localhost:5432/mydb"
      ]
    }
  }
}
```

### Claude Code

```bash
claude mcp add postgres -- npx @simonja/postgres-mcp-server "postgresql://user:password@localhost:5432/mydb"
```

## Tools

### `query`

Run a read-only SQL query against the database.

**Parameters:**
- `sql` (string, required) — the SQL query to execute

**Example request:**
```json
{
  "name": "query",
  "arguments": {
    "sql": "SELECT id, name, email FROM users WHERE active = true LIMIT 10"
  }
}
```

**Example response:**
```json
{
  "content": [
    {
      "type": "text",
      "text": "[{\"id\": 1, \"name\": \"Alice\", \"email\": \"alice@example.com\"}, {\"id\": 2, \"name\": \"Bob\", \"email\": \"bob@example.com\"}]"
    }
  ]
}
```

### `list-tables`

List all non-system tables in the database.

**Parameters:** none

**Example response:**
```json
{
  "content": [
    {
      "type": "text",
      "text": "[{\"table_schema\": \"public\", \"table_name\": \"users\"}, {\"table_schema\": \"public\", \"table_name\": \"orders\"}]"
    }
  ]
}
```

### `describe-table`

Get column details for a specific table.

**Parameters:**
- `table` (string, required) — table name
- `schema` (string, optional) — schema name, defaults to `"public"`

**Example request:**
```json
{
  "name": "describe-table",
  "arguments": {
    "table": "users"
  }
}
```

**Example response:**
```json
{
  "content": [
    {
      "type": "text",
      "text": "[{\"column_name\": \"id\", \"data_type\": \"integer\", \"is_nullable\": \"NO\", \"column_default\": \"nextval('users_id_seq')\"}, {\"column_name\": \"name\", \"data_type\": \"text\", \"is_nullable\": \"YES\", \"column_default\": null}]"
    }
  ]
}
```

## Resources

### `table-schema`

Each table's column schema is exposed as an MCP resource via the URI template `postgres://{table}/schema`. Clients can list and read these to discover database structure without writing SQL.

## Security

This server is read-only by design, with multiple layers of protection:

1. **Read-only transactions** — queries are wrapped in `BEGIN TRANSACTION READ ONLY`
2. **Prepared statements** — the extended query protocol is used to enforce single-statement execution, preventing SQL injection via multi-statement payloads (e.g. `COMMIT; DROP TABLE ...`)
3. **Connection destruction** — connections are destroyed after each query rather than returned to the pool, preventing session state manipulation across requests

For defense in depth, it is recommended to connect with a **least-privilege PostgreSQL role** that only has `SELECT` permissions:

```sql
CREATE ROLE mcp_reader WITH LOGIN PASSWORD 'secret';
GRANT CONNECT ON DATABASE mydb TO mcp_reader;
GRANT USAGE ON SCHEMA public TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO mcp_reader;
```

Then use this role in your connection string:

```
postgresql://mcp_reader:secret@localhost:5432/mydb
```

## Development

```bash
npm run dev     # watch mode (recompiles on change)
npm test        # run tests
npm run build   # compile TypeScript
```
