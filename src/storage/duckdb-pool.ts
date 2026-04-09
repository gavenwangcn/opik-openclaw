import * as fs from "node:fs";
import * as path from "node:path";
import { DuckDBInstance, DuckDBConnection } from "@duckdb/node-api";

export type QueryPool = {
  query(sql: string, params?: unknown[]): Promise<[Record<string, unknown>[], null]>;
  run(sql: string, params?: unknown[]): Promise<void>;
  exec(sql: string): Promise<void>;
  close(): Promise<void>;
};

/**
 * Minimal DuckDB pool wrapper compatible with mysql2-like query shape.
 * Serializes all operations on a single connection to avoid WAL corruption.
 */
export class DuckDBPool implements QueryPool {
  private conn: DuckDBConnection | null;
  private instance: DuckDBInstance | null;
  private opQueue: Promise<unknown> = Promise.resolve();

  constructor(instance: DuckDBInstance, conn: DuckDBConnection) {
    this.instance = instance;
    this.conn = conn;
  }

  static async open(dbPath: string): Promise<DuckDBPool> {
    const dir = path.dirname(dbPath);
    if (dir && dir !== "." && !fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    const instance = await DuckDBInstance.create(dbPath);
    const conn = await DuckDBConnection.create(instance);
    return new DuckDBPool(instance, conn);
  }

  private serialize<T>(op: () => Promise<T>): Promise<T> {
    const ticket = this.opQueue.then(op, op);
    this.opQueue = ticket.then(
      () => undefined,
      () => undefined,
    );
    return ticket;
  }

  async query(sql: string, params?: unknown[]): Promise<[Record<string, unknown>[], null]> {
    return this.serialize(async () => {
      const c = this.conn;
      if (!c) throw new Error("DuckDB connection not initialized.");

      const duckSql = this.toDuckPlaceholders(sql, params);
      if (params && params.length > 0) {
        const stmt = await c.prepare(duckSql);
        this.bindParams(stmt, params);
        const result = await stmt.runAndReadAll();
        return [this.toRowObjects(result), null];
      }

      const result = await c.runAndReadAll(duckSql);
      return [this.toRowObjects(result), null];
    });
  }

  async run(sql: string, params?: unknown[]): Promise<void> {
    return this.serialize(async () => {
      const c = this.conn;
      if (!c) throw new Error("DuckDB connection not initialized.");

      const duckSql = this.toDuckPlaceholders(sql, params);
      if (params && params.length > 0) {
        const stmt = await c.prepare(duckSql);
        this.bindParams(stmt, params);
        await stmt.run();
        return;
      }
      await c.run(duckSql);
    });
  }

  async exec(sql: string): Promise<void> {
    return this.serialize(async () => {
      const c = this.conn;
      if (!c) throw new Error("DuckDB connection not initialized.");

      const statements = sql
        .split(";")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      for (const stmt of statements) {
        await c.run(stmt);
      }
    });
  }

  async close(): Promise<void> {
    return this.serialize(async () => {
      try {
        if (this.conn && typeof (this.conn as unknown as { disconnect?: () => Promise<void> }).disconnect === "function") {
          await (this.conn as unknown as { disconnect: () => Promise<void> }).disconnect();
        } else if (this.conn && typeof (this.conn as unknown as { close?: () => Promise<void> }).close === "function") {
          await (this.conn as unknown as { close: () => Promise<void> }).close();
        }
      } catch {
        // ignore
      }
      this.conn = null;
      this.instance = null;
    });
  }

  private toDuckPlaceholders(sql: string, params?: unknown[]): string {
    if (!params || params.length === 0) return sql;
    let idx = 0;
    return sql.replace(/\?/g, () => `$${++idx}`);
  }

  private bindParams(stmt: any, params: unknown[]): void {
    for (let i = 0; i < params.length; i++) {
      const paramIdx = i + 1;
      const val = params[i];
      if (val === null || val === undefined) {
        stmt.bindNull(paramIdx);
      } else if (typeof val === "number") {
        if (Number.isInteger(val)) stmt.bindInteger(paramIdx, val);
        else stmt.bindDouble(paramIdx, val);
      } else if (typeof val === "bigint") {
        if (typeof stmt.bindBigInt === "function") stmt.bindBigInt(paramIdx, val);
        else stmt.bindVarchar(paramIdx, val.toString());
      } else if (typeof val === "boolean") {
        stmt.bindBoolean(paramIdx, val);
      } else if (val instanceof Date) {
        stmt.bindVarchar(paramIdx, val.toISOString().replace("T", " ").replace("Z", ""));
      } else if (typeof val === "string") {
        stmt.bindVarchar(paramIdx, val);
      } else {
        stmt.bindVarchar(paramIdx, String(val));
      }
    }
  }

  private toRowObjects(result: any): Record<string, unknown>[] {
    const columnNames: string[] = result.columnNames();
    const rawRows: any[][] = result.getRows();
    return rawRows.map((row) => {
      const obj: Record<string, unknown> = {};
      for (let i = 0; i < columnNames.length; i++) {
        const v = row[i];
        obj[columnNames[i]] = typeof v === "bigint" ? v.toString() : v;
      }
      return obj;
    });
  }
}

