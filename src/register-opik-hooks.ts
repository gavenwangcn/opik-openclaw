/**
 * Opik exporter typed-hook registration (OpenClaw `api.on` → global hook runner).
 *
 * Called synchronously from `createOpikService()` during the plugin `register()` phase,
 * matching bundled plugins that register services + hooks in the same turn (see
 * `diagnostics-otel`, `definePluginEntry` pattern in `index.ts`).
 */

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import type { Opik, Span, Trace } from "opik";
import { registerLlmHooks } from "./service/hooks/llm.js";
import { registerSubagentHooks } from "./service/hooks/subagent.js";
import { registerToolHooks } from "./service/hooks/tool.js";
import { logOpikHookEnter } from "./service/hook-enter-log.js";
import { instrumentOpenClawPluginApi } from "./service/instrument-plugin-api.js";
import { OPIK_INSTRUMENTED_HOOK_REGISTRATION_SITE } from "./service/opik-instrumented-hook-registration-coverage.js";
import {
  OPIK_INSTRUMENTED_TYPED_HOOK_NAMES,
  OPIK_INSTRUMENTED_TYPED_HOOK_NAME_SET,
} from "./service/opik-instrumented-hook-names.js";
import { sanitizeStringForOpik, sanitizeValueForOpik } from "./service/payload-sanitizer.js";
import { mergeDefinedConfig, formatError } from "./service/helpers.js";
import { parseOpikPluginConfig, type ActiveTrace, type OpikPluginConfig } from "./types.js";

type ServiceLogger = {
  info: (message: string) => void;
  warn: (message: string) => void;
};

export type OpikExporterHookInstall = {
  api: OpenClawPluginApi;
  pluginConfig: OpikPluginConfig;
  log: ServiceLogger;
  hookInstallFlags: { instrumentPluginApiApplied: boolean };
  getClient: () => Opik | null;
  activeTraces: Map<string, ActiveTrace>;
  sessionByAgentId: Map<string, string>;
  getLastActiveSessionKey: () => string | undefined;
  getHookProjectName: () => string;
  getHookTags: () => string[];
  getToolResultPersistSanitizeEnabled: () => boolean;
  rememberSessionCorrelation: (sessionKey: string, agentId?: unknown) => void;
  forgetSessionCorrelation: (sessionKey: string) => void;
  closeActiveTrace: (active: ActiveTrace, reason: string) => void;
  applyContextMeta: (active: ActiveTrace, ctx: Record<string, unknown>) => void;
  resolveSessionSpanContainer: (
    sessionKey: string,
  ) => { sessionKey: string; active: ActiveTrace; parent: Trace | Span } | undefined;
  resolveSubagentSpanContainer: (params: {
    requesterSessionKey?: string;
    childSessionKey?: string;
    targetSessionKey?: string;
  }) => { sessionKey: string; active: ActiveTrace; parent: Trace | Span } | undefined;
  getSubagentSpanHost: (
    sessionKey: string,
  ) => { hostSessionKey: string; active: ActiveTrace; span: Span } | undefined;
  rememberSubagentSpanHost: (
    sessionKey: string,
    hostSessionKey: string,
    active: ActiveTrace,
    span: Span,
  ) => void;
  forgetSubagentSpanHost: (sessionKey: string) => void;
  warnMissingAfterToolSessionKey: (fallbackMode: string) => void;
  nextSpanSeq: () => number;
  safeSpanUpdate: (span: Span, payload: Record<string, unknown>, reason: string) => void;
  safeSpanEnd: (span: Span, reason: string) => void;
  scheduleMediaAttachmentUploads: (params: {
    entityType: "trace" | "span";
    entity: unknown;
    projectName: string;
    reason: string;
    payloads: unknown[];
  }) => void;
  scheduleTraceFinalize: (sessionKey: string, traceRef: Trace) => void;
};

/**
 * Registers all Opik exporter `api.on(...)` handlers. Must run during plugin `register()`.
 */
export function registerOpikExporterHooks(ctx: OpikExporterHookInstall): void {
  void OPIK_INSTRUMENTED_HOOK_REGISTRATION_SITE;

  const opikInstrumentedHookNamesSeen = new Set<string>();
  {
    const underlyingOn = ctx.api.on.bind(ctx.api);
    ctx.api.on = ((hookName, handler, opts) => {
      if (OPIK_INSTRUMENTED_TYPED_HOOK_NAME_SET.has(String(hookName))) {
        opikInstrumentedHookNamesSeen.add(String(hookName));
      }
      underlyingOn(hookName, handler as never, opts);
    }) as OpenClawPluginApi["on"];
  }

  const registerTimeOpikConfig = mergeDefinedConfig(
    parseOpikPluginConfig(ctx.api.pluginConfig),
    ctx.pluginConfig,
  );
  const instrumentDisabledByEnv =
    process.env.OPIK_DEBUG_INSTRUMENT_PLUGIN_API === "0" ||
    process.env.OPIK_DEBUG_INSTRUMENT_PLUGIN_API === "false";
  if (registerTimeOpikConfig.debugInstrumentPluginApi !== false && !instrumentDisabledByEnv) {
    instrumentOpenClawPluginApi(ctx.api, { info: (message: string) => ctx.log.info(message) });
    ctx.hookInstallFlags.instrumentPluginApiApplied = true;
  }

  registerLlmHooks({
    api: ctx.api,
    getClient: ctx.getClient,
    activeTraces: ctx.activeTraces,
    getProjectName: ctx.getHookProjectName,
    getTags: ctx.getHookTags,
    rememberSessionCorrelation: ctx.rememberSessionCorrelation,
    closeActiveTrace: ctx.closeActiveTrace,
    forgetSessionCorrelation: ctx.forgetSessionCorrelation,
    applyContextMeta: ctx.applyContextMeta,
    safeSpanUpdate: ctx.safeSpanUpdate,
    safeSpanEnd: ctx.safeSpanEnd,
    scheduleMediaAttachmentUploads: ctx.scheduleMediaAttachmentUploads,
    warn: (message) => ctx.log.warn(message),
    info: (message) => ctx.log.info(message),
    formatError,
  });

  registerToolHooks({
    api: ctx.api,
    getClient: ctx.getClient,
    activeTraces: ctx.activeTraces,
    sessionByAgentId: ctx.sessionByAgentId,
    getLastActiveSessionKey: ctx.getLastActiveSessionKey,
    rememberSessionCorrelation: ctx.rememberSessionCorrelation,
    resolveSessionSpanContainer: ctx.resolveSessionSpanContainer,
    warnMissingAfterToolSessionKey: ctx.warnMissingAfterToolSessionKey,
    nextSpanSeq: ctx.nextSpanSeq,
    safeSpanUpdate: ctx.safeSpanUpdate,
    safeSpanEnd: ctx.safeSpanEnd,
    scheduleMediaAttachmentUploads: ctx.scheduleMediaAttachmentUploads,
    getProjectName: ctx.getHookProjectName,
    warn: (message) => ctx.log.warn(message),
    info: (message) => ctx.log.info(message),
    formatError,
  });

  registerSubagentHooks({
    api: ctx.api,
    getClient: ctx.getClient,
    rememberSessionCorrelation: ctx.rememberSessionCorrelation,
    resolveSubagentSpanContainer: ctx.resolveSubagentSpanContainer,
    getSubagentSpanHost: ctx.getSubagentSpanHost,
    rememberSubagentSpanHost: ctx.rememberSubagentSpanHost,
    forgetSubagentSpanHost: ctx.forgetSubagentSpanHost,
    safeSpanUpdate: ctx.safeSpanUpdate,
    safeSpanEnd: ctx.safeSpanEnd,
    warn: (message) => ctx.log.warn(message),
    info: (message) => ctx.log.info(message),
    formatError,
  });

  ctx.api.on("tool_result_persist", (event) => {
    logOpikHookEnter(ctx.log.info, "tool_result_persist");
    if (!ctx.getToolResultPersistSanitizeEnabled()) {
      return;
    }
    try {
      const eventObj = event as Record<string, unknown>;
      const message = eventObj.message;
      if (!message || typeof message !== "object") return;

      const sanitizedMessage = sanitizeValueForOpik(message);
      if (sanitizedMessage !== message) {
        return { message: sanitizedMessage };
      }
    } catch (err) {
      ctx.log.warn(`opik: tool_result_persist failed: ${formatError(err)}`);
    }
  });

  ctx.api.on("agent_end", (event, agentCtx) => {
    logOpikHookEnter(ctx.log.info, "agent_end");
    const sessionKey = agentCtx.sessionKey;
    if (!sessionKey) {
      ctx.log.info("opik: event=agent_end phase=skip reason=no_session_key");
      return;
    }
    ctx.rememberSessionCorrelation(sessionKey, agentCtx.agentId);

    const active = ctx.activeTraces.get(sessionKey);
    if (!active) {
      ctx.log.info(
        `opik: event=agent_end phase=skip reason=no_active_trace sessionKey=${sessionKey} (no prior llm_input; nothing to export for this thread)`,
      );
      return;
    }

    ctx.applyContextMeta(active, agentCtx as Record<string, unknown>);
    for (const [toolKey, toolSpan] of active.toolSpans) {
      ctx.safeSpanEnd(toolSpan, `agent_end orphan tool sessionKey=${sessionKey} toolKey=${toolKey}`);
    }
    active.toolSpans.clear();

    for (const [subagentKey, subagentSpan] of active.subagentSpans) {
      ctx.safeSpanEnd(
        subagentSpan,
        `agent_end orphan subagent sessionKey=${sessionKey} subagentKey=${subagentKey}`,
      );
    }
    active.subagentSpans.clear();

    active.agentEnd = {
      success: event.success,
      error: typeof event.error === "string" ? sanitizeStringForOpik(event.error) : event.error,
      durationMs: event.durationMs,
      messages: (sanitizeValueForOpik(
        ((event as Record<string, unknown>).messages as unknown[]) ?? [],
      ) as unknown[]) ?? [],
    };

    ctx.scheduleMediaAttachmentUploads({
      entityType: "trace",
      entity: active.trace,
      projectName: ctx.getHookProjectName(),
      reason: `agent_end sessionKey=${sessionKey}`,
      payloads: [
        event.error,
        ((event as Record<string, unknown>).messages as unknown[] | undefined)?.at(-1),
      ],
    });

    const traceRef = active.trace;
    ctx.log.info(
      `opik: event=agent_end phase=ok sessionKey=${sessionKey} success=${event.success} durationMs=${event.durationMs ?? "n/a"} finalize=deferred_macrotask`,
    );
    ctx.scheduleTraceFinalize(sessionKey, traceRef);
  });

  for (const name of OPIK_INSTRUMENTED_TYPED_HOOK_NAMES) {
    if (!opikInstrumentedHookNamesSeen.has(name)) {
      throw new Error(
        `opik-openclaw: missing api.on registration for instrumented hook "${name}". ` +
          `Each name in OPIK_INSTRUMENTED_TYPED_HOOK_NAMES must be registered.`,
      );
    }
  }
}
