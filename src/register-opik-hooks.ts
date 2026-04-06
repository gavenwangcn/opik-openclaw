/**
 * Opik exporter typed-hook registration (OpenClaw `api.on` → global hook runner).
 *
 * Called from `createOpikTraceExporter().registerHookHandlers(api)` during plugin `register()`,
 * aligned with bundled plugins (parse config, wire hooks, then `registerService`).
 *
 * Hook handlers log via `api.logger` so messages use the active gateway logger (same pattern as
 * `memory-lancedb-pro`).
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

export type OpikTraceHookBinding = {
  instanceId: string;
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

/** @deprecated Use `OpikTraceHookBinding`. */
export type OpikExporterHookInstall = OpikTraceHookBinding & {
  api: OpenClawPluginApi;
  pluginConfig: OpikPluginConfig;
  info: (message: string) => void;
  warn: (message: string) => void;
};

/**
 * Registers all Opik exporter `api.on(...)` handlers. Must run during plugin `register()`.
 */
export function registerOpikTraceHooks(
  api: OpenClawPluginApi,
  pluginConfig: OpikPluginConfig,
  binding: OpikTraceHookBinding,
): void {
  void OPIK_INSTRUMENTED_HOOK_REGISTRATION_SITE;

  const prefix = `opik[#${binding.instanceId}]:`;
  const info = (message: string) => api.logger.info(`${prefix} ${message}`);
  const warn = (message: string) => api.logger.warn(`${prefix} ${message}`);

  const opikInstrumentedHookNamesSeen = new Set<string>();
  {
    const underlyingOn = api.on.bind(api);
    api.on = ((hookName, handler, opts) => {
      if (OPIK_INSTRUMENTED_TYPED_HOOK_NAME_SET.has(String(hookName))) {
        opikInstrumentedHookNamesSeen.add(String(hookName));
      }
      underlyingOn(hookName, handler as never, opts);
    }) as OpenClawPluginApi["on"];
  }

  const registerTimeOpikConfig = mergeDefinedConfig(
    parseOpikPluginConfig(api.pluginConfig),
    pluginConfig,
  );
  const instrumentDisabledByEnv =
    process.env.OPIK_DEBUG_INSTRUMENT_PLUGIN_API === "0" ||
    process.env.OPIK_DEBUG_INSTRUMENT_PLUGIN_API === "false";
  if (registerTimeOpikConfig.debugInstrumentPluginApi !== false && !instrumentDisabledByEnv) {
    instrumentOpenClawPluginApi(api, { info });
    binding.hookInstallFlags.instrumentPluginApiApplied = true;
  }

  registerLlmHooks({
    api,
    getClient: binding.getClient,
    activeTraces: binding.activeTraces,
    getProjectName: binding.getHookProjectName,
    getTags: binding.getHookTags,
    rememberSessionCorrelation: binding.rememberSessionCorrelation,
    closeActiveTrace: binding.closeActiveTrace,
    forgetSessionCorrelation: binding.forgetSessionCorrelation,
    applyContextMeta: binding.applyContextMeta,
    safeSpanUpdate: binding.safeSpanUpdate,
    safeSpanEnd: binding.safeSpanEnd,
    scheduleMediaAttachmentUploads: binding.scheduleMediaAttachmentUploads,
    warn,
    info,
    formatError,
  });

  registerToolHooks({
    api,
    getClient: binding.getClient,
    activeTraces: binding.activeTraces,
    sessionByAgentId: binding.sessionByAgentId,
    getLastActiveSessionKey: binding.getLastActiveSessionKey,
    rememberSessionCorrelation: binding.rememberSessionCorrelation,
    resolveSessionSpanContainer: binding.resolveSessionSpanContainer,
    warnMissingAfterToolSessionKey: binding.warnMissingAfterToolSessionKey,
    nextSpanSeq: binding.nextSpanSeq,
    safeSpanUpdate: binding.safeSpanUpdate,
    safeSpanEnd: binding.safeSpanEnd,
    scheduleMediaAttachmentUploads: binding.scheduleMediaAttachmentUploads,
    getProjectName: binding.getHookProjectName,
    warn,
    info,
    formatError,
  });

  registerSubagentHooks({
    api,
    getClient: binding.getClient,
    rememberSessionCorrelation: binding.rememberSessionCorrelation,
    resolveSubagentSpanContainer: binding.resolveSubagentSpanContainer,
    getSubagentSpanHost: binding.getSubagentSpanHost,
    rememberSubagentSpanHost: binding.rememberSubagentSpanHost,
    forgetSubagentSpanHost: binding.forgetSubagentSpanHost,
    safeSpanUpdate: binding.safeSpanUpdate,
    safeSpanEnd: binding.safeSpanEnd,
    warn,
    info,
    formatError,
  });

  api.on("tool_result_persist", (event) => {
    logOpikHookEnter(info, "tool_result_persist");
    if (!binding.getToolResultPersistSanitizeEnabled()) {
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
      warn(`opik: tool_result_persist failed: ${formatError(err)}`);
    }
  });

  api.on("agent_end", (event, agentCtx) => {
    logOpikHookEnter(info, "agent_end");
    const sessionKey = agentCtx.sessionKey;
    if (!sessionKey) {
      info("opik: event=agent_end phase=skip reason=no_session_key");
      return;
    }
    binding.rememberSessionCorrelation(sessionKey, agentCtx.agentId);

    const active = binding.activeTraces.get(sessionKey);
    if (!active) {
      info(
        `opik: event=agent_end phase=skip reason=no_active_trace sessionKey=${sessionKey} (no prior llm_input; nothing to export for this thread)`,
      );
      return;
    }

    binding.applyContextMeta(active, agentCtx as Record<string, unknown>);
    for (const [toolKey, toolSpan] of active.toolSpans) {
      binding.safeSpanEnd(toolSpan, `agent_end orphan tool sessionKey=${sessionKey} toolKey=${toolKey}`);
    }
    active.toolSpans.clear();

    for (const [subagentKey, subagentSpan] of active.subagentSpans) {
      binding.safeSpanEnd(
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

    binding.scheduleMediaAttachmentUploads({
      entityType: "trace",
      entity: active.trace,
      projectName: binding.getHookProjectName(),
      reason: `agent_end sessionKey=${sessionKey}`,
      payloads: [
        event.error,
        ((event as Record<string, unknown>).messages as unknown[] | undefined)?.at(-1),
      ],
    });

    const traceRef = active.trace;
    info(
      `opik: event=agent_end phase=ok sessionKey=${sessionKey} success=${event.success} durationMs=${event.durationMs ?? "n/a"} finalize=deferred_macrotask`,
    );
    binding.scheduleTraceFinalize(sessionKey, traceRef);
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

/** @deprecated Use `registerOpikTraceHooks`. */
export function registerOpikExporterHooks(ctx: OpikExporterHookInstall): void {
  registerOpikTraceHooks(ctx.api, ctx.pluginConfig, ctx);
}
