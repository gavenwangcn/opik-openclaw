import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { registerOpikCli } from "./src/configure.js";
import { createOpikTraceExporter } from "./src/opik-trace-exporter.js";
import { parseOpikPluginConfig } from "./src/types.js";

const opikOpenClawPlugin = {
  id: "opik-openclaw",
  name: "OpenClaw trace store",
  description: "Store LLM traces/spans in local DuckDB (TruLens-compatible + observability UI tables)",

  register(api: OpenClawPluginApi) {
    const pluginConfig = parseOpikPluginConfig(api.pluginConfig);
    const { service, registerHookHandlers } = createOpikTraceExporter(pluginConfig);
    registerHookHandlers(api);
    api.registerService(service);
    api.registerCli(
      ({ program }) =>
        registerOpikCli({
          program,
          loadConfig: api.runtime.config.loadConfig,
          writeConfigFile: api.runtime.config.writeConfigFile,
        }),
      { commands: ["opik"] },
    );
  },
};

export default opikOpenClawPlugin;
