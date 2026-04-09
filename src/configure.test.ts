import { Command } from "commander";
import { describe, expect, test, vi } from "vitest";
import {
  getOpikPluginEntry,
  registerOpikCli,
  resolveEffectiveDuckdbPath,
  setOpikPluginEntry,
  showOpikStatus,
} from "./configure.js";
import { DEFAULT_TRULENS_DUCKDB_PATH } from "./storage/duckdb-trulens-writer.js";

describe("configure helpers", () => {
  test("setOpikPluginEntry writes plugins.entries.opik-openclaw", () => {
    const next = setOpikPluginEntry(
      {} as any,
      {
        enabled: true,
        duckdbPath: "/tmp/traces.duckdb",
        tags: ["tag-a", "tag-b"],
      },
      true,
    ) as any;

    expect(next.plugins.entries["opik-openclaw"].enabled).toBe(true);
    expect(next.plugins.entries["opik-openclaw"].config).toEqual({
      enabled: true,
      duckdbPath: "/tmp/traces.duckdb",
      tags: ["tag-a", "tag-b"],
    });
  });

  test("getOpikPluginEntry reads canonical plugin-scoped config", () => {
    const parsed = getOpikPluginEntry({
      plugins: {
        entries: {
          "opik-openclaw": {
            enabled: false,
            config: {
              duckdbPath: "/x/y.duckdb",
            },
          },
        },
      },
    } as any);

    expect(parsed.enabled).toBe(false);
    expect(parsed.config.duckdbPath).toBe("/x/y.duckdb");
  });

  test("resolveEffectiveDuckdbPath prefers config over env over default", () => {
    const prev = process.env.OPIK_DUCKDB_PATH;
    try {
      process.env.OPIK_DUCKDB_PATH = "/env/path.duckdb";
      expect(resolveEffectiveDuckdbPath({ duckdbPath: "/cfg.duckdb" })).toBe("/cfg.duckdb");
      expect(resolveEffectiveDuckdbPath({})).toBe("/env/path.duckdb");
      delete process.env.OPIK_DUCKDB_PATH;
      expect(resolveEffectiveDuckdbPath({})).toBe(DEFAULT_TRULENS_DUCKDB_PATH);
    } finally {
      if (prev === undefined) delete process.env.OPIK_DUCKDB_PATH;
      else process.env.OPIK_DUCKDB_PATH = prev;
    }
  });
});

describe("opik status command", () => {
  test("reads plugin entry and shows DuckDB path", async () => {
    const program = new Command();
    const loadConfig = () =>
      ({
        plugins: {
          entries: {
            "opik-openclaw": {
              enabled: true,
              config: {
                enabled: true,
                duckdbPath: "/data/traces.duckdb",
                tags: ["prod"],
              },
            },
          },
        },
      }) as any;

    registerOpikCli({
      program,
      loadConfig,
      writeConfigFile: async () => undefined,
    });

    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    showOpikStatus({
      loadConfig,
      writeConfigFile: async () => undefined,
    });
    const output = logSpy.mock.calls.map((call) => call.join(" ")).join("\n");
    logSpy.mockRestore();
    expect(output).toContain("Enabled:        yes");
    expect(output).toContain("/data/traces.duckdb");
    expect(output).toContain("Tags:           prod");
  });

  test("status command is registered under openclaw opik", () => {
    const program = new Command();
    registerOpikCli({
      program,
      loadConfig: () =>
        ({
          plugins: {
            entries: {
              "opik-openclaw": {
                enabled: true,
                config: {
                  enabled: true,
                  duckdbPath: "/x.duckdb",
                },
              },
            },
          },
        }) as any,
      writeConfigFile: async () => undefined,
    });
    const opikCommand = program.commands.find((cmd) => cmd.name() === "opik");
    expect(opikCommand).toBeDefined();
    expect(opikCommand?.commands.map((cmd) => cmd.name())).toContain("status");
  });
});
