import * as p from "@clack/prompts";
import type { OpenClawConfig } from "openclaw/plugin-sdk";
import type { OpikPluginConfig } from "./types.js";
import { DEFAULT_TRULENS_DUCKDB_PATH } from "./storage/duckdb-trulens-writer.js";

type ConfigDeps = {
  loadConfig: () => OpenClawConfig;
  writeConfigFile: (cfg: OpenClawConfig) => Promise<void>;
};

type RegisterOpikCliParams = {
  program: any;
} & ConfigDeps;

const OPIK_PLUGIN_ID = "opik-openclaw";

function asObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

export function getOpikPluginEntry(cfg: OpenClawConfig): {
  enabled?: boolean;
  config: Record<string, unknown>;
} {
  const root = asObject(cfg);
  const plugins = asObject(root.plugins);
  const entries = asObject(plugins.entries);
  const entry = asObject(entries[OPIK_PLUGIN_ID]);
  const config = asObject(entry.config);
  return {
    enabled: typeof entry.enabled === "boolean" ? entry.enabled : undefined,
    config,
  };
}

export function setOpikPluginEntry(
  cfg: OpenClawConfig,
  config: OpikPluginConfig,
  enabled = true,
): OpenClawConfig {
  const root = asObject(cfg);
  const plugins = asObject(root.plugins);
  const entries = asObject(plugins.entries);
  const existingEntry = asObject(entries[OPIK_PLUGIN_ID]);
  const nextEntries = {
    ...entries,
    [OPIK_PLUGIN_ID]: {
      ...existingEntry,
      enabled,
      config: {
        ...asObject(existingEntry.config),
        ...config,
      },
    },
  };
  return {
    ...root,
    plugins: {
      ...plugins,
      entries: nextEntries,
    },
  } as OpenClawConfig;
}

/** Effective DuckDB path (config → env → default), for status display. */
export function resolveEffectiveDuckdbPath(opik: Record<string, unknown>): string {
  const fromCfg =
    typeof opik.duckdbPath === "string" && opik.duckdbPath.trim().length > 0
      ? opik.duckdbPath.trim()
      : undefined;
  const fromEnv =
    typeof process.env.OPIK_DUCKDB_PATH === "string" && process.env.OPIK_DUCKDB_PATH.trim().length > 0
      ? process.env.OPIK_DUCKDB_PATH.trim()
      : undefined;
  return fromCfg ?? fromEnv ?? DEFAULT_TRULENS_DUCKDB_PATH;
}

async function runOpikConfigure(deps: ConfigDeps): Promise<void> {
  p.intro("OpenClaw trace store (local DuckDB)");

  const cfg = deps.loadConfig();
  const existingEntry = getOpikPluginEntry(cfg);
  const existing = existingEntry.config as OpikPluginConfig;

  const suggested =
    (typeof existing.duckdbPath === "string" && existing.duckdbPath.trim()) ||
    (typeof process.env.OPIK_DUCKDB_PATH === "string" && process.env.OPIK_DUCKDB_PATH.trim()) ||
    DEFAULT_TRULENS_DUCKDB_PATH;

  const pathInput = await p.text({
    message: "Path to DuckDB file for traces (empty = use default below):",
    placeholder: DEFAULT_TRULENS_DUCKDB_PATH,
    initialValue: suggested,
  });

  if (p.isCancel(pathInput)) {
    p.cancel("Setup cancelled.");
    return;
  }

  const trimmed = String(pathInput as string).trim();
  const next: OpikPluginConfig = {
    ...existing,
    enabled: true,
  };

  if (trimmed.length === 0 || trimmed === DEFAULT_TRULENS_DUCKDB_PATH) {
    delete (next as Record<string, unknown>).duckdbPath;
  } else {
    next.duckdbPath = trimmed;
  }

  const nextCfg = setOpikPluginEntry(cfg, next, true);
  await deps.writeConfigFile(nextCfg);

  const preview = resolveEffectiveDuckdbPath(next as Record<string, unknown>);
  p.note(`DuckDB file (effective): ${preview}`, "Trace store configuration saved");
  p.outro("Restart the gateway to apply changes.");
}

export function showOpikStatus(deps: ConfigDeps): void {
  const cfg = deps.loadConfig();
  const entry = getOpikPluginEntry(cfg);
  const opik = entry.config;

  if (entry.enabled === undefined && Object.keys(opik).length === 0) {
    console.log("Trace store is not configured. Run: openclaw opik configure");
    return;
  }

  const enabled = entry.enabled !== false && opik.enabled !== false;
  const effective = resolveEffectiveDuckdbPath(opik);
  const lines = [
    `  Enabled:        ${enabled ? "yes" : "no"}`,
    `  DuckDB file:    ${effective}`,
  ];

  const cfgPath =
    typeof opik.duckdbPath === "string" && opik.duckdbPath.trim()
      ? opik.duckdbPath.trim()
      : undefined;
  if (cfgPath && cfgPath !== effective) {
    lines.push(`  Config path:    ${cfgPath}`);
  }

  const envPath =
    typeof process.env.OPIK_DUCKDB_PATH === "string" && process.env.OPIK_DUCKDB_PATH.trim()
      ? process.env.OPIK_DUCKDB_PATH.trim()
      : undefined;
  if (envPath && envPath !== cfgPath && envPath === effective) {
    lines.push(`  (from OPIK_DUCKDB_PATH)`);
  }

  const tags = opik.tags as string[] | undefined;
  if (tags?.length) {
    lines.push(`  Tags:           ${tags.join(", ")}`);
  }

  console.log("OpenClaw trace store (opik-openclaw):\n");
  console.log(lines.join("\n"));
}

export function registerOpikCli(params: RegisterOpikCliParams): void {
  const { program, loadConfig, writeConfigFile } = params;
  const deps: ConfigDeps = { loadConfig, writeConfigFile };

  const root = program.command("opik").description("Local DuckDB trace storage (TruLens-compatible)");

  root
    .command("configure")
    .description("Interactive setup for the local trace database")
    .action(async () => {
      await runOpikConfigure(deps);
    });

  root
    .command("status")
    .description("Show trace store paths and enabled flag")
    .action(() => {
      showOpikStatus(deps);
    });
}
