import pg from "pg";

let pool: pg.Pool | null = null;

export function initPool(databaseUrl: string): void {
  pool = new pg.Pool({ connectionString: databaseUrl });
}

export function getPool(): pg.Pool {
  if (!pool) {
    throw new Error("Database pool not initialized. Call initPool() first.");
  }
  return pool;
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
