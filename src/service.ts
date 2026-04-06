import type { OpenClawPluginApi, OpenClawPluginService } from "openclaw/plugin-sdk";
import { createOpikTraceExporter } from "./opik-trace-exporter.js";
import type { OpikPluginConfig } from "./types.js";

/**
 * One-shot factory for tests: builds trace runtime + registers hooks on `api`.
 * Production entry uses `createOpikTraceExporter` + `registerHookHandlers` from `index.ts`.
 */
export function createOpikService(
  api: OpenClawPluginApi,
  pluginConfig: OpikPluginConfig = {},
): OpenClawPluginService {
  const { service, registerHookHandlers } = createOpikTraceExporter(pluginConfig);
  registerHookHandlers(api);
  return service;
}

export { createOpikTraceExporter } from "./opik-trace-exporter.js";
