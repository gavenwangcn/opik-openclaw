import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { definePluginEntry, emptyPluginConfigSchema } from "openclaw/plugin-sdk/plugin-entry";
import { disableLogger } from "opik";
import { registerOpikCli } from "./src/configure.js";
import { createOpikService } from "./src/service.js";
import { parseOpikPluginConfig } from "./src/types.js";

// Suppress Opik SDK tslog console output
disableLogger();

export default definePluginEntry({
  id: "opik-openclaw",
  name: "Opik",
  description: "Export LLM traces and spans to Opik for observability",
  configSchema: emptyPluginConfigSchema(),
  register(api: OpenClawPluginApi) {
    const pluginConfig = parseOpikPluginConfig(api.pluginConfig);
    // Service construction registers typed hooks synchronously via `api.on` (see `register-opik-hooks.ts`).
    api.registerService(createOpikService(api, pluginConfig));
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
});
