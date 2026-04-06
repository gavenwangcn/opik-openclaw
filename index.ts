import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { disableLogger } from "opik";
import { registerOpikCli } from "./src/configure.js";
import { createOpikTraceExporter } from "./src/opik-trace-exporter.js";
import { parseOpikPluginConfig } from "./src/types.js";

// Suppress Opik SDK tslog console output
disableLogger();

const opikOpenClawPlugin = {
  id: "opik-openclaw",
  name: "Opik",
  description: "Export LLM traces and spans to Opik for observability",

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
