import type { OpikInstrumentedHookName } from "./opik-instrumented-hook-names.js";

/**
 * Compile-time guard: every instrumented hook must map to a registration site.
 * If you add a name to OPIK_INSTRUMENTED_TYPED_HOOK_NAMES, add a row here or TypeScript errors.
 */
export const OPIK_INSTRUMENTED_HOOK_REGISTRATION_SITE = {
  llm_input: "llm",
  llm_output: "llm",
  before_tool_call: "tool",
  after_tool_call: "tool",
  subagent_spawning: "subagent",
  subagent_spawned: "subagent",
  subagent_delivery_target: "subagent",
  subagent_ended: "subagent",
  agent_end: "agent_end",
} satisfies Record<OpikInstrumentedHookName, "llm" | "tool" | "subagent" | "agent_end">;
