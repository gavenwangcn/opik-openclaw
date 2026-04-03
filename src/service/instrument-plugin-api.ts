import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { OPIK_INSTRUMENTED_TYPED_HOOK_NAME_SET } from "./opik-instrumented-hook-names.js";

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
    if (OPIK_INSTRUMENTED_TYPED_HOOK_NAME_SET.has(event)) {
      log.info(`opik: [instrument] register listener event=${event}`);
    }
    originalOn(event, (ev: unknown, ctx: unknown) => {
      if (OPIK_INSTRUMENTED_TYPED_HOOK_NAME_SET.has(event)) {
        const ctxObj = ctx && typeof ctx === "object" ? (ctx as Record<string, unknown>) : undefined;
        const sessionKey =
          ctxObj && typeof ctxObj.sessionKey === "string" ? ctxObj.sessionKey : "n/a";
        log.info(`opik: [instrument] FIRED event=${event} sessionKey=${sessionKey}`);
      }
      return handler(ev, ctx);
    });
  }) as OpenClawPluginApi["on"];
}
