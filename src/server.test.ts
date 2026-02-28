import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createServer } from "./server.js";
import pg from "pg";

function createMockPool(queryFn: (...args: unknown[]) => unknown): pg.Pool {
  const asyncQueryFn = (...args: unknown[]) => Promise.resolve(queryFn(...args));
  const mockClient = {
    query: vi.fn(asyncQueryFn),
    release: vi.fn(),
  };
  return {
    connect: vi.fn().mockResolvedValue(mockClient),
    query: vi.fn(asyncQueryFn),
  } as unknown as pg.Pool;
}

const resourceBaseUrl = new URL("postgres://localhost:5432/testdb");

describe("postgres-mcp server", () => {
  let client: Client;
  let server: ReturnType<typeof createServer>;
  let mockPool: pg.Pool;

  beforeAll(async () => {
    mockPool = createMockPool(() => ({ rows: [], rowCount: 0 }));

    server = createServer(mockPool, resourceBaseUrl);
    client = new Client({ name: "test-client", version: "1.0.0" });

    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);
    await client.connect(clientTransport);
  });

  afterAll(async () => {
    await client.close();
    await server.close();
  });

  describe("listTools", () => {
    it("returns the 3 registered tools", async () => {
      const result = await client.listTools();
      const names = result.tools.map((t) => t.name).sort();
      expect(names).toEqual(["describe-table", "list-tables", "query"]);
    });

    it("query tool has sql input schema", async () => {
      const result = await client.listTools();
      const queryTool = result.tools.find((t) => t.name === "query");
      expect(queryTool?.inputSchema.properties).toHaveProperty("sql");
    });
  });

  describe("query tool", () => {
    it("returns rows from a read-only query", async () => {
      const mockRows = [{ id: 1, name: "alice" }, { id: 2, name: "bob" }];
      const pool = createMockPool((sql: unknown) => {
        if (typeof sql === "string" && sql.includes("BEGIN")) return { rows: [] };
        return { rows: mockRows };
      });

      const srv = createServer(pool, resourceBaseUrl);
      const cli = new Client({ name: "test", version: "1.0.0" });
      const [ct, st] = InMemoryTransport.createLinkedPair();
      await srv.connect(st);
      await cli.connect(ct);

      const result = await cli.callTool({ name: "query", arguments: { sql: "SELECT * FROM users" } });
      expect(result.isError).toBe(false);
      const parsed = JSON.parse((result.content as Array<{ text: string }>)[0].text);
      expect(parsed).toEqual(mockRows);

      await cli.close();
      await srv.close();
    });

    it("wraps query in a read-only transaction using a prepared statement", async () => {
      const queries: unknown[] = [];
      const pool = createMockPool((sql: unknown) => {
        queries.push(sql);
        return { rows: [] };
      });

      const srv = createServer(pool, resourceBaseUrl);
      const cli = new Client({ name: "test", version: "1.0.0" });
      const [ct, st] = InMemoryTransport.createLinkedPair();
      await srv.connect(st);
      await cli.connect(ct);

      await cli.callTool({ name: "query", arguments: { sql: "SELECT 1" } });
      expect(queries[0]).toBe("BEGIN TRANSACTION READ ONLY");
      expect(queries[1]).toEqual({
        name: "sandboxed-statement",
        text: "SELECT 1",
        values: [],
      });
      expect(queries[2]).toBe("ROLLBACK");

      // Verify connection is destroyed (not returned to pool) for session isolation
      const mockClient = await pool.connect();
      expect(mockClient.release).toHaveBeenCalledWith(true);

      await cli.close();
      await srv.close();
    });
  });

  describe("list-tables tool", () => {
    it("returns table listing", async () => {
      const mockTables = [
        { table_schema: "public", table_name: "users" },
        { table_schema: "public", table_name: "orders" },
      ];
      const pool = createMockPool(() => ({ rows: mockTables }));

      const srv = createServer(pool, resourceBaseUrl);
      const cli = new Client({ name: "test", version: "1.0.0" });
      const [ct, st] = InMemoryTransport.createLinkedPair();
      await srv.connect(st);
      await cli.connect(ct);

      const result = await cli.callTool({ name: "list-tables", arguments: {} });
      const parsed = JSON.parse((result.content as Array<{ text: string }>)[0].text);
      expect(parsed).toEqual(mockTables);

      await cli.close();
      await srv.close();
    });
  });

  describe("describe-table tool", () => {
    it("returns column details for a table", async () => {
      const mockColumns = [
        { column_name: "id", data_type: "integer", is_nullable: "NO", column_default: null },
        { column_name: "name", data_type: "text", is_nullable: "YES", column_default: null },
      ];
      const pool = createMockPool(() => ({ rows: mockColumns }));

      const srv = createServer(pool, resourceBaseUrl);
      const cli = new Client({ name: "test", version: "1.0.0" });
      const [ct, st] = InMemoryTransport.createLinkedPair();
      await srv.connect(st);
      await cli.connect(ct);

      const result = await cli.callTool({
        name: "describe-table",
        arguments: { table: "users" },
      });
      const parsed = JSON.parse((result.content as Array<{ text: string }>)[0].text);
      expect(parsed).toEqual(mockColumns);

      await cli.close();
      await srv.close();
    });

    it("defaults schema to public", async () => {
      let capturedParams: unknown[] = [];
      const pool = createMockPool((_sql: unknown, params?: unknown) => {
        if (Array.isArray(params)) capturedParams = params;
        return { rows: [] };
      });

      const srv = createServer(pool, resourceBaseUrl);
      const cli = new Client({ name: "test", version: "1.0.0" });
      const [ct, st] = InMemoryTransport.createLinkedPair();
      await srv.connect(st);
      await cli.connect(ct);

      await cli.callTool({ name: "describe-table", arguments: { table: "users" } });
      expect(capturedParams).toEqual(["public", "users"]);

      await cli.close();
      await srv.close();
    });
  });

  describe("resources", () => {
    it("lists table schemas as resources", async () => {
      const pool = createMockPool(() => ({
        rows: [{ table_name: "users" }, { table_name: "orders" }],
      }));

      const srv = createServer(pool, resourceBaseUrl);
      const cli = new Client({ name: "test", version: "1.0.0" });
      const [ct, st] = InMemoryTransport.createLinkedPair();
      await srv.connect(st);
      await cli.connect(ct);

      const result = await cli.listResources();
      expect(result.resources).toHaveLength(2);
      expect(result.resources[0].name).toBe('"users" database schema');
      expect(result.resources[1].name).toBe('"orders" database schema');

      await cli.close();
      await srv.close();
    });

    it("reads a table schema resource", async () => {
      const mockColumns = [
        { column_name: "id", data_type: "integer" },
        { column_name: "email", data_type: "text" },
      ];
      const pool = createMockPool(() => ({ rows: mockColumns }));

      const srv = createServer(pool, resourceBaseUrl);
      const cli = new Client({ name: "test", version: "1.0.0" });
      const [ct, st] = InMemoryTransport.createLinkedPair();
      await srv.connect(st);
      await cli.connect(ct);

      const result = await cli.readResource({
        uri: "postgres://users/schema",
      });
      const content = result.contents[0];
      const text = "text" in content ? content.text : "";
      const parsed = JSON.parse(text);
      expect(parsed).toEqual(mockColumns);

      await cli.close();
      await srv.close();
    });
  });
});
