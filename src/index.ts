#!/usr/bin/env node
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { initPool, getPool, closePool } from "./db.js";
import { createServer } from "./server.js";

const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("Please provide a database URL as a command-line argument");
  process.exit(1);
}

initPool(args[0]);

const server = createServer(getPool());

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);

  const shutdown = async () => {
    await closePool();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
