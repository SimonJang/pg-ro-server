import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import pg from "pg";

const SCHEMA_PATH = "schema";

export function createServer(pool: pg.Pool): McpServer {
  const server = new McpServer({
    name: "postgres-mcp-server",
    version: "0.1.0",
  });

  // Resources: expose table schemas
  server.registerResource(
    "table-schema",
    new ResourceTemplate("postgres://{table}/schema", {
      list: async () => {
        const client = await pool.connect();
        try {
          const result = await client.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
          );
          return {
            resources: result.rows.map((row: { table_name: string }) => ({
              uri: `postgres://${encodeURIComponent(row.table_name)}/${SCHEMA_PATH}`,
              mimeType: "application/json",
              name: `"${row.table_name}" database schema`,
            })),
          };
        } finally {
          client.release();
        }
      },
    }),
    { mimeType: "application/json" },
    async (uri) => {
      const schemaComponent = uri.pathname.replace(/^\//, "");
      if (schemaComponent !== SCHEMA_PATH) {
        throw new Error("Invalid resource URI");
      }

      const tableName = decodeURIComponent(uri.hostname);
      if (!tableName) {
        throw new Error("Invalid resource URI: missing table name");
      }

      const client = await pool.connect();
      try {
        const result = await client.query(
          "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 AND table_schema = 'public'",
          [tableName]
        );
        return {
          contents: [
            {
              uri: uri.href,
              mimeType: "application/json",
              text: JSON.stringify(result.rows, null, 2),
            },
          ],
        };
      } finally {
        client.release();
      }
    }
  );

  // Tools
  server.registerTool("query", {
    description: "Run a read-only SQL query",
    inputSchema: { sql: z.string() },
  }, async ({ sql }) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN TRANSACTION READ ONLY");
      const result = await client.query({
        name: "sandboxed-statement",
        text: sql,
        values: [],
      });
      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify(result.rows, null, 2),
          },
        ],
        isError: false,
      };
    } catch (error) {
      throw error;
    } finally {
      client
        .query("ROLLBACK")
        .catch((error: unknown) => console.warn("Could not roll back transaction:", error));
      client.release(true);
    }
  });

  server.registerTool("list-tables", {
    description: "List all tables in the database",
  }, async () => {
    const result = await pool.query(`
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
      ORDER BY table_schema, table_name
    `);
    return {
      content: [
        {
          type: "text" as const,
          text: JSON.stringify(result.rows, null, 2),
        },
      ],
    };
  });

  server.registerTool("describe-table", {
    description: "Get column details for a table",
    inputSchema: { table: z.string(), schema: z.string().optional().default("public") },
  }, async ({ table, schema }) => {
    const result = await pool.query(
      `
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
      `,
      [schema, table]
    );
    return {
      content: [
        {
          type: "text" as const,
          text: JSON.stringify(result.rows, null, 2),
        },
      ],
    };
  });

  return server;
}
