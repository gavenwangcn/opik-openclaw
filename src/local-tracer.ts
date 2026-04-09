import { DuckDBTruLensWriter, type TruLensEventRow } from "./storage/duckdb-trulens-writer.js";

type TraceCreateParams = {
  name: string;
  threadId: string; // sessionKey
  input?: unknown;
  metadata?: Record<string, unknown>;
  tags?: string[];
};

type SpanCreateParams = {
  name: string;
  type?: "llm" | "tool" | "subagent" | "trace";
  model?: string;
  provider?: string;
  input?: unknown;
};

const RECORD_KIND = "SPAN_KIND_TRULENS";
const RECORD_TYPE = "SPAN";

function safeJson(value: unknown): unknown {
  if (value === undefined) return undefined;
  try {
    JSON.stringify(value);
    return value;
  } catch {
    return String(value);
  }
}

function spanTypeFrom(params: { isRoot?: boolean; type?: SpanCreateParams["type"] }): string {
  if (params.isRoot) return "record_root";
  if (params.type === "llm") return "generation";
  if (params.type === "tool") return "tool";
  if (params.type === "subagent") return "agent";
  return "unknown";
}

export class LocalSpan {
  private ended = false;
  private eventId: string;
  private spanId: string;
  private parentSpanId: string | null;
  private traceId: string;
  private recordId: string;
  private name: string;
  private start: Date;
  private attrs: Record<string, unknown>;
  private resourceAttrs: Record<string, unknown>;
  private writer: DuckDBTruLensWriter;

  constructor(params: {
    writer: DuckDBTruLensWriter;
    eventId: string;
    traceId: string;
    spanId: string;
    parentSpanId: string | null;
    recordId: string;
    name: string;
    spanType: string;
    resourceAttributes: Record<string, unknown>;
    initialAttributes?: Record<string, unknown>;
    start: Date;
  }) {
    this.writer = params.writer;
    this.eventId = params.eventId;
    this.traceId = params.traceId;
    this.spanId = params.spanId;
    this.parentSpanId = params.parentSpanId;
    this.recordId = params.recordId;
    this.name = params.name;
    this.start = params.start;
    this.resourceAttrs = params.resourceAttributes;
    this.attrs = {
      "ai.observability.span_type": params.spanType,
      "ai.observability.record_id": this.recordId,
      ...(params.initialAttributes ?? {}),
    };
  }

  update(payload: Record<string, unknown>): void {
    if (this.ended) return;
    // Store payload in a stable, TruLens-friendly namespace.
    // Keep it compact: safeJson + avoid circular references.
    this.attrs["ai.observability.call.function"] = this.name;
    if ("input" in payload) this.attrs["ai.observability.call.input"] = safeJson(payload.input);
    if ("output" in payload) this.attrs["ai.observability.call.return"] = safeJson(payload.output);
    if ("usage" in payload) this.attrs["ai.observability.usage"] = safeJson(payload.usage);
    if ("model" in payload) this.attrs["ai.observability.model"] = safeJson(payload.model);
    if ("provider" in payload) this.attrs["ai.observability.provider"] = safeJson(payload.provider);
    this.attrs["ai.observability.payload"] = safeJson(payload);
  }

  async end(): Promise<void> {
    if (this.ended) return;
    this.ended = true;
    const end = new Date();
    const row: TruLensEventRow = {
      event_id: this.eventId,
      record: {
        name: this.name,
        kind: RECORD_KIND,
      },
      record_attributes: this.attrs,
      record_type: RECORD_TYPE,
      resource_attributes: this.resourceAttrs,
      start_timestamp: this.start,
      timestamp: end,
      trace: {
        trace_id: this.traceId,
        span_id: this.spanId,
        parent_id: this.parentSpanId,
      },
    };
    await this.writer.insertEvent(row);

    // --- UI compatibility write-through (openclaw-observability style) ---
    const spanType = String(this.attrs["ai.observability.span_type"] ?? "");
    const actionType =
      spanType === "generation"
        ? "llm"
        : spanType === "tool"
          ? "tool"
          : spanType === "agent"
            ? "subagent"
            : spanType === "record_root"
              ? "trace"
              : "action";

    const usage = this.attrs["ai.observability.usage"] as any;
    const promptTokens =
      usage && typeof usage === "object" && "input" in usage ? Number((usage as any).input) : undefined;
    const completionTokens =
      usage && typeof usage === "object" && "output" in usage ? Number((usage as any).output) : undefined;

    const inputParams =
      this.attrs["ai.observability.call.input"] !== undefined
        ? JSON.stringify(this.attrs["ai.observability.call.input"])
        : undefined;
    const outputResult =
      this.attrs["ai.observability.call.return"] !== undefined
        ? JSON.stringify(this.attrs["ai.observability.call.return"])
        : undefined;

    await this.writer.upsertSession({
      sessionId: this.recordId,
      modelName: typeof this.attrs["ai.observability.model"] === "string" ? (this.attrs["ai.observability.model"] as string) : undefined,
      channelId:
        typeof this.attrs["ai.observability.payload"] === "object" &&
        this.attrs["ai.observability.payload"] !== null &&
        "channelId" in (this.attrs["ai.observability.payload"] as any)
          ? String((this.attrs["ai.observability.payload"] as any).channelId)
          : undefined,
      startTime: this.start,
      endTime: spanType === "record_root" ? end : undefined,
      totalTokensDelta:
        (Number.isFinite(promptTokens as number) ? (promptTokens as number) : 0) +
        (Number.isFinite(completionTokens as number) ? (completionTokens as number) : 0),
    });

    await this.writer.insertAction({
      sessionId: this.recordId,
      actionType,
      actionName: this.name,
      modelName:
        typeof this.attrs["ai.observability.model"] === "string"
          ? (this.attrs["ai.observability.model"] as string)
          : "",
      inputParams,
      outputResult,
      promptTokens: Number.isFinite(promptTokens as number) ? (promptTokens as number) : undefined,
      completionTokens: Number.isFinite(completionTokens as number) ? (completionTokens as number) : undefined,
      durationMs: end.getTime() - this.start.getTime(),
      createdAt: end,
    });
  }
}

export class LocalTrace {
  private writer: DuckDBTruLensWriter;
  private traceId: string;
  private recordId: string;
  private threadId: string;
  private name: string;
  private rootSpan: LocalSpan;
  private spanSeq = 0;
  private resourceAttributes: Record<string, unknown>;

  constructor(params: {
    writer: DuckDBTruLensWriter;
    traceId: string;
    threadId: string;
    name: string;
    input?: unknown;
    metadata?: Record<string, unknown>;
    tags?: string[];
  }) {
    this.writer = params.writer;
    this.traceId = params.traceId;
    this.threadId = params.threadId;
    this.recordId = params.threadId;
    this.name = params.name;
    this.resourceAttributes = {
      "ai.observability.app_name": "opik-openclaw",
      "ai.observability.app_version": "local",
    };

    const start = new Date();
    const rootEventId = `evt_${this.traceId}_root`;
    const rootSpanId = `span_${this.traceId}_0`;
    this.rootSpan = new LocalSpan({
      writer: this.writer,
      eventId: rootEventId,
      traceId: this.traceId,
      spanId: rootSpanId,
      parentSpanId: null,
      recordId: this.recordId,
      name: this.name,
      spanType: spanTypeFrom({ isRoot: true }),
      resourceAttributes: this.resourceAttributes,
      initialAttributes: {
        "ai.observability.trace.input": safeJson(params.input),
        "ai.observability.trace.metadata": safeJson(params.metadata ?? {}),
        "ai.observability.trace.tags": safeJson(params.tags ?? []),
      },
      start,
    });
  }

  span(params: SpanCreateParams): LocalSpan {
    const start = new Date();
    const seq = ++this.spanSeq;
    const eventId = `evt_${this.traceId}_${seq}`;
    const spanId = `span_${this.traceId}_${seq}`;
    const parentSpanId = `span_${this.traceId}_0`;
    const span = new LocalSpan({
      writer: this.writer,
      eventId,
      traceId: this.traceId,
      spanId,
      parentSpanId,
      recordId: this.recordId,
      name: params.name,
      spanType: spanTypeFrom({ type: params.type }),
      resourceAttributes: this.resourceAttributes,
      initialAttributes: {
        ...(params.model ? { "ai.observability.model": params.model } : {}),
        ...(params.provider ? { "ai.observability.provider": params.provider } : {}),
        ...(params.input !== undefined ? { "ai.observability.call.input": safeJson(params.input) } : {}),
      },
      start,
    });
    return span;
  }

  update(payload: Record<string, unknown>): void {
    this.rootSpan.update(payload);
  }

  async end(): Promise<void> {
    await this.rootSpan.end();
  }
}

export class LocalTracer {
  private writer: DuckDBTruLensWriter;
  private traceCounter = 0;

  constructor(writer: DuckDBTruLensWriter) {
    this.writer = writer;
  }

  trace(params: TraceCreateParams): LocalTrace {
    const id = `tr_${Date.now().toString(36)}_${(this.traceCounter++).toString(36)}`;
    return new LocalTrace({
      writer: this.writer,
      traceId: id,
      threadId: params.threadId,
      name: params.name,
      input: params.input,
      metadata: params.metadata,
      tags: params.tags,
    });
  }

  async flush(): Promise<void> {
    await this.writer.checkpoint();
  }
}

