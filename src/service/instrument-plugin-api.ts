import type { OpenClawPluginApi } from "openclaw/plugin-sdk";

/** Events relevant to Opik tracing — log register + FIRED when instrumentation is on. */
const INSTRUMENT_EVENTS = new Set([
  "llm_input",
  "llm_output",
  "agent_end",
  "before_tool_call",
  "after_tool_call",
  "subagent_spawning",
  "subagent_spawned",
  "subagent_delivery_target",
  "subagent_ended",
]);

/**
 * Wraps `api.on` so each registered handler logs when OpenClaw actually invokes it.
 * Use when `opik: event=...` never appears — if FIRED never shows, the host is not emitting hooks to this API.
 */
export function instrumentOpenClawPluginApi(
  api: OpenClawPluginApi,
  log: { info: (message: string) => void },
): void {
  const originalOn = api.on.bind(api);
  api.on = ((event: string, handler: (event: unknown, ctx: unknown) => unknown) => {
    if (INSTRUMENT_EVENTS.has(event)) {
      log.info(`opik: [instrument] register listener event=${event}`);
    }
    originalOn(event, (ev: unknown, ctx: unknown) => {
      if (INSTRUMENT_EVENTS.has(event)) {
        const ctxObj = ctx && typeof ctx === "object" ? (ctx as Record<string, unknown>) : undefined;
        const sessionKey =
          ctxObj && typeof ctxObj.sessionKey === "string" ? ctxObj.sessionKey : "n/a";
        log.info(`opik: [instrument] FIRED event=${event} sessionKey=${sessionKey}`);
      }
      return handler(ev, ctx);
    });
  }) as OpenClawPluginApi["on"];
}
