import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { execSync } from "node:child_process";

const CONTAINER_NAME = "postgres-mcp-e2e";
const DB_PORT = 5433;
const DB_USER = "testuser";
const DB_PASS = "testpass";
const DB_NAME = "testdb";
const DB_URL = `postgresql://${DB_USER}:${DB_PASS}@localhost:${DB_PORT}/${DB_NAME}`;

function docker(cmd: string) {
  execSync(`docker ${cmd}`, { stdio: "pipe" });
}

function psql(sql: string) {
  docker(`exec -i ${CONTAINER_NAME} psql -U ${DB_USER} -d ${DB_NAME} -c "${sql}"`);
}

describe("E2E: postgres-mcp", () => {
  let client: Client;
  let transport: StdioClientTransport;

  beforeAll(async () => {
    // Start PostgreSQL container
    docker(
      `run -d --name ${CONTAINER_NAME} ` +
      `-e POSTGRES_USER=${DB_USER} ` +
      `-e POSTGRES_PASSWORD=${DB_PASS} ` +
      `-e POSTGRES_DB=${DB_NAME} ` +
      `-p ${DB_PORT}:5432 postgres:17`
    );

    // Wait for PostgreSQL to be ready
    for (let i = 0; i < 30; i++) {
      try {
        docker(`exec ${CONTAINER_NAME} pg_isready -U ${DB_USER} -d ${DB_NAME}`);
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 1000));
      }
    }

    // Seed test data
    psql("CREATE TABLE users (id serial PRIMARY KEY, name text, email text)");
    psql("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com')");
    psql("CREATE TABLE orders (id serial PRIMARY KEY, user_id int REFERENCES users(id), total numeric)");
    psql("INSERT INTO orders (user_id, total) VALUES (1, 99.99), (2, 49.50)");

    // Build and connect MCP server via stdio
    execSync("npm run build", { stdio: "pipe" });

    transport = new StdioClientTransport({
      command: "node",
      args: ["dist/index.js", DB_URL],
    });

    client = new Client({ name: "e2e-test", version: "1.0.0" });
    await client.connect(transport);
  }, 60_000);

  afterAll(async () => {
    await client?.close();
    try { docker(`rm -f ${CONTAINER_NAME}`); } catch { /* ignore */ }
  });

  it("lists tables", async () => {
    const result = await client.callTool({ name: "list-tables", arguments: {} });
    const rows = JSON.parse((result.content as Array<{ text: string }>)[0].text);
    const tableNames = rows.map((r: { table_name: string }) => r.table_name).sort();
    expect(tableNames).toEqual(["orders", "users"]);
  });

  it("describes a table", async () => {
    const result = await client.callTool({ name: "describe-table", arguments: { table: "users" } });
    const rows = JSON.parse((result.content as Array<{ text: string }>)[0].text);
    const columnNames = rows.map((r: { column_name: string }) => r.column_name);
    expect(columnNames).toEqual(["id", "name", "email"]);
  });

  it("runs a read-only query", async () => {
    const result = await client.callTool({
      name: "query",
      arguments: { sql: "SELECT name, email FROM users ORDER BY id" },
    });
    expect(result.isError).toBe(false);
    const rows = JSON.parse((result.content as Array<{ text: string }>)[0].text);
    expect(rows).toEqual([
      { name: "Alice", email: "alice@example.com" },
      { name: "Bob", email: "bob@example.com" },
    ]);
  });

  it("runs a join query", async () => {
    const result = await client.callTool({
      name: "query",
      arguments: { sql: "SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id ORDER BY u.id" },
    });
    expect(result.isError).toBe(false);
    const rows = JSON.parse((result.content as Array<{ text: string }>)[0].text);
    expect(rows).toEqual([
      { name: "Alice", total: "99.99" },
      { name: "Bob", total: "49.50" },
    ]);
  });

  it("blocks write operations", async () => {
    const result = await client.callTool({
      name: "query",
      arguments: { sql: "DROP TABLE users" },
    });
    expect(result.isError).toBe(true);
    const text = (result.content as Array<{ text: string }>)[0].text;
    expect(text).toContain("read-only transaction");
  });

  it("blocks multi-statement SQL injection", async () => {
    const result = await client.callTool({
      name: "query",
      arguments: { sql: "COMMIT; DROP TABLE users" },
    });
    expect(result.isError).toBe(true);
    const text = (result.content as Array<{ text: string }>)[0].text;
    expect(text).toContain("multiple commands");
  });

  it("data is intact after attack attempts", async () => {
    const result = await client.callTool({
      name: "query",
      arguments: { sql: "SELECT count(*)::int AS count FROM users" },
    });
    const rows = JSON.parse((result.content as Array<{ text: string }>)[0].text);
    expect(rows[0].count).toBe(2);
  });
});
