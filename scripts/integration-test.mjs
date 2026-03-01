/**
 * MCP client integration test.
 *
 * Connects to a running postgres-mcp-server instance via stdio,
 * exercises every tool (list-tables, describe-table, query),
 * and asserts expected results against the bookstore seed data.
 *
 * Usage: node scripts/integration-test.mjs <db-url> <server-entry>
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const DB_URL = process.argv[2];
const SERVER_ENTRY = process.argv[3];

if (!DB_URL || !SERVER_ENTRY) {
  console.error("Usage: node integration-test.mjs <db-url> <server-entry>");
  process.exit(1);
}

let exitCode = 0;
let client;

function assert(condition, message) {
  if (condition) {
    console.log(`  PASS: ${message}`);
  } else {
    console.log(`  FAIL: ${message}`);
    exitCode = 1;
  }
}

try {
  const transport = new StdioClientTransport({
    command: "node",
    args: [SERVER_ENTRY, DB_URL],
  });

  client = new Client({ name: "integration-test", version: "1.0.0" });
  await client.connect(transport);

  console.log("Connected to MCP server");
  console.log("");

  // ── list-tables ──────────────────────────────────────────────────

  console.log("[list-tables]");
  const tables = await client.callTool({ name: "list-tables", arguments: {} });
  const tableNames = JSON.parse(tables.content[0].text)
    .map((r) => r.table_name)
    .sort();

  assert(tableNames.length === 4, `found ${tableNames.length} tables`);
  assert(
    tableNames.includes("authors") && tableNames.includes("books"),
    `tables include authors and books`
  );
  console.log("");

  // ── describe-table ───────────────────────────────────────────────

  console.log("[describe-table: books]");
  const desc = await client.callTool({
    name: "describe-table",
    arguments: { table: "books" },
  });
  const columns = JSON.parse(desc.content[0].text).map((c) => c.column_name);

  assert(columns.includes("title"), `has 'title' column`);
  assert(columns.includes("author_id"), `has 'author_id' column`);
  assert(columns.includes("price"), `has 'price' column`);
  console.log("");

  // ── query: read-only select ──────────────────────────────────────

  console.log("[query: top 3 best-selling books]");
  const result = await client.callTool({
    name: "query",
    arguments: {
      sql: `SELECT b.title, a.name AS author, SUM(o.quantity) AS total_sold, b.price
            FROM orders o
            JOIN books b ON b.id = o.book_id
            JOIN authors a ON a.id = b.author_id
            GROUP BY b.id, b.title, a.name, b.price
            ORDER BY total_sold DESC
            LIMIT 3`,
    },
  });

  assert(!result.isError, `query succeeded`);
  const rows = JSON.parse(result.content[0].text);
  assert(rows.length === 3, `returned 3 rows`);
  assert(rows[0].title === "One Hundred Years of Solitude", `#1 is correct`);
  assert(rows[0].total_sold === "4", `#1 sold 4 copies`);

  rows.forEach((r, i) => {
    console.log(
      `  ${i + 1}. "${r.title}" by ${r.author} — ${r.total_sold} sold ($${r.price})`
    );
  });
  console.log("");

  // ── query: write blocked ─────────────────────────────────────────

  console.log("[query: write operation blocked]");
  const writeResult = await client.callTool({
    name: "query",
    arguments: { sql: "DROP TABLE books" },
  });
  assert(writeResult.isError === true, `DROP TABLE rejected`);
  console.log("");

  // ── query: SQL injection blocked ─────────────────────────────────

  console.log("[query: SQL injection blocked]");
  const injectionResult = await client.callTool({
    name: "query",
    arguments: { sql: "COMMIT; DROP TABLE books" },
  });
  assert(injectionResult.isError === true, `multi-statement rejected`);
  console.log("");

  // ── query: data intact after attacks ─────────────────────────────

  console.log("[query: data intact after attack attempts]");
  const countResult = await client.callTool({
    name: "query",
    arguments: { sql: "SELECT count(*)::int AS count FROM books" },
  });
  const count = JSON.parse(countResult.content[0].text)[0].count;
  assert(count === 10, `all 10 books still present`);

} finally {
  await client?.close().catch(() => {});
}

console.log("");
if (exitCode === 0) {
  console.log("All assertions passed.");
} else {
  console.log("Some assertions FAILED.");
}
process.exit(exitCode);
