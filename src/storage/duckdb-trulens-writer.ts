import * as os from "node:os";
import * as path from "node:path";
import { DuckDBPool, type QueryPool } from "./duckdb-pool.js";

export type DuckdbConfig = {
  path?: string;
};

export type TruLensEventRow = {
  event_id: string;
  record: Record<string, unknown>;
  record_attributes: Record<string, unknown>;
  record_type: string;
  resource_attributes: Record<string, unknown>;
  start_timestamp: Date;
  timestamp: Date;
  trace: Record<string, unknown>;
};

/** Default file when `duckdbPath` / `OPIK_DUCKDB_PATH` are unset (shared with CLI `opik status`). */
export const DEFAULT_TRULENS_DUCKDB_PATH = path.join(
  os.homedir(),
  ".openclaw",
  "data",
  "opik-openclaw.trulens.duckdb",
);

const SCHEMA = `
CREATE TABLE IF NOT EXISTS trulens_events (
  event_id VARCHAR PRIMARY KEY,
  record JSON NOT NULL,
  record_attributes JSON NOT NULL,
  record_type VARCHAR NOT NULL,
  resource_attributes JSON NOT NULL,
  start_timestamp TIMESTAMP NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  trace JSON NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trulens_events_ts ON trulens_events(timestamp);
`;

// Minimal compatibility tables (openclaw-observability UI expects these).
// We treat trulens_events as source-of-truth; audit_* rows are written in parallel.
const UI_SCHEMA = `
CREATE TABLE IF NOT EXISTS audit_actions (
  id BIGINT PRIMARY KEY,
  session_id VARCHAR(64) NOT NULL,
  action_type VARCHAR(32) NOT NULL,
  action_name VARCHAR(255) NOT NULL,
  model_name VARCHAR(128) DEFAULT '',
  input_params TEXT,
  output_result TEXT,
  prompt_tokens INTEGER DEFAULT NULL,
  completion_tokens INTEGER DEFAULT NULL,
  duration_ms INTEGER DEFAULT NULL,
  user_id VARCHAR(128) DEFAULT '',
  channel_id VARCHAR(128) DEFAULT '',
  created_at TIMESTAMP DEFAULT current_timestamp
);
CREATE INDEX IF NOT EXISTS idx_act_session ON audit_actions(session_id);
CREATE INDEX IF NOT EXISTS idx_act_type ON audit_actions(action_type);
CREATE INDEX IF NOT EXISTS idx_act_created ON audit_actions(created_at);
CREATE INDEX IF NOT EXISTS idx_act_user ON audit_actions(user_id);

CREATE TABLE IF NOT EXISTS audit_sessions (
  session_id VARCHAR(64) PRIMARY KEY,
  user_id VARCHAR(128) DEFAULT '',
  model_name VARCHAR(128) DEFAULT '',
  channel_id VARCHAR(128) DEFAULT '',
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP DEFAULT NULL,
  total_actions INTEGER DEFAULT 0,
  total_tokens INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ses_user ON audit_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_ses_start ON audit_sessions(start_time);
`;

export class DuckDBTruLensWriter {
  private pool: QueryPool | null = null;
  private initialized = false;
  private initPromise: Promise<void> | null = null;
  private idCounter = 0n;
  private config: Required<DuckdbConfig>;

  constructor(config: DuckdbConfig = {}) {
    this.config = { path: config.path ?? DEFAULT_TRULENS_DUCKDB_PATH };
  }

  getDbPath(): string {
    return this.config.path;
  }

  getPool(): QueryPool | null {
    return this.pool;
  }

  async ensureReady(): Promise<void> {
    if (this.initialized) return;
    if (this.initPromise) return this.initPromise;
    this.initPromise = this.init().finally(() => {
      this.initPromise = null;
    });
    return this.initPromise;
  }

  private async init(): Promise<void> {
    this.pool = await DuckDBPool.open(this.config.path);
    await this.pool.exec(SCHEMA);
    await this.pool.exec(UI_SCHEMA);
    this.initialized = true;
  }

  nextBigintId(): bigint {
    // timestamp_ms * 10000 + counter mod 10000, avoids sequences.
    const id = BigInt(Date.now()) * 10000n + (this.idCounter++ % 10000n);
    return id;
  }

  async insertEvent(row: TruLensEventRow): Promise<void> {
    await this.ensureReady();
    if (!this.pool) throw new Error("DuckDB not initialized.");

    await this.pool.run(
      `INSERT INTO trulens_events
       (event_id, record, record_attributes, record_type, resource_attributes, start_timestamp, timestamp, trace)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        row.event_id,
        JSON.stringify(row.record),
        JSON.stringify(row.record_attributes),
        row.record_type,
        JSON.stringify(row.resource_attributes),
        row.start_timestamp,
        row.timestamp,
        JSON.stringify(row.trace),
      ],
    );
  }

  async upsertSession(params: {
    sessionId: string;
    userId?: string;
    modelName?: string;
    channelId?: string;
    startTime: Date;
    endTime?: Date;
    totalTokensDelta?: number;
  }): Promise<void> {
    await this.ensureReady();
    if (!this.pool) throw new Error("DuckDB not initialized.");

    const tokensDelta = Math.max(0, Math.floor(params.totalTokensDelta ?? 0));
    await this.pool.run(
      `INSERT INTO audit_sessions
        (session_id, user_id, model_name, channel_id, start_time, end_time, total_actions, total_tokens)
       VALUES (?, ?, ?, ?, ?, ?, 0, ?)
       ON CONFLICT(session_id) DO UPDATE SET
         user_id = CASE WHEN excluded.user_id != '' THEN excluded.user_id ELSE audit_sessions.user_id END,
         model_name = CASE WHEN excluded.model_name != '' THEN excluded.model_name ELSE audit_sessions.model_name END,
         channel_id = CASE WHEN excluded.channel_id != '' THEN excluded.channel_id ELSE audit_sessions.channel_id END,
         start_time = CASE WHEN audit_sessions.start_time < excluded.start_time THEN audit_sessions.start_time ELSE excluded.start_time END,
         end_time = CASE WHEN excluded.end_time IS NULL THEN audit_sessions.end_time ELSE excluded.end_time END,
         total_tokens = audit_sessions.total_tokens + ?`,
      [
        params.sessionId,
        params.userId ?? "",
        params.modelName ?? "",
        params.channelId ?? "",
        params.startTime,
        params.endTime ?? null,
        tokensDelta,
        tokensDelta,
      ],
    );
  }

  async insertAction(params: {
    sessionId: string;
    actionType: string;
    actionName: string;
    modelName?: string;
    inputParams?: string;
    outputResult?: string;
    promptTokens?: number;
    completionTokens?: number;
    durationMs?: number;
    userId?: string;
    channelId?: string;
    createdAt: Date;
  }): Promise<void> {
    await this.ensureReady();
    if (!this.pool) throw new Error("DuckDB not initialized.");

    const id = this.nextBigintId();
    await this.pool.run(
      `INSERT INTO audit_actions
       (id, session_id, action_type, action_name, model_name, input_params, output_result,
        prompt_tokens, completion_tokens, duration_ms, user_id, channel_id, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        id,
        params.sessionId,
        params.actionType,
        params.actionName,
        params.modelName ?? "",
        params.inputParams ?? null,
        params.outputResult ?? null,
        params.promptTokens ?? null,
        params.completionTokens ?? null,
        params.durationMs ?? null,
        params.userId ?? "",
        params.channelId ?? "",
        params.createdAt,
      ],
    );

    // Update aggregate counts cheaply.
    const tokenTotal = Math.max(
      0,
      Math.floor((params.promptTokens ?? 0) + (params.completionTokens ?? 0)),
    );
    await this.pool.run(
      `UPDATE audit_sessions
       SET total_actions = total_actions + 1,
           total_tokens = total_tokens + ?
       WHERE session_id = ?`,
      [tokenTotal, params.sessionId],
    );
  }

  async checkpoint(): Promise<void> {
    await this.ensureReady();
    if (!this.pool) return;
    // DuckDB CHECKPOINT merges WAL into main file.
    await this.pool.exec("CHECKPOINT");
  }

  async close(): Promise<void> {
    if (!this.pool) return;
    await this.pool.close();
    this.pool = null;
    this.initialized = false;
  }
}

