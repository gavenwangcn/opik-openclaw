/**
 * Typed hook names that Opik instruments for register/FIRED diagnostics.
 * Keep in sync with actual `api.on(...)` registrations in createOpikService.
 */
export const OPIK_INSTRUMENTED_TYPED_HOOK_NAMES = [
  "llm_input",
  "llm_output",
  "agent_end",
  "before_tool_call",
  "after_tool_call",
  "subagent_spawning",
  "subagent_spawned",
  "subagent_delivery_target",
  "subagent_ended",
] as const;

export type OpikInstrumentedHookName = (typeof OPIK_INSTRUMENTED_TYPED_HOOK_NAMES)[number];

export const OPIK_INSTRUMENTED_TYPED_HOOK_NAME_SET: ReadonlySet<string> = new Set(
  OPIK_INSTRUMENTED_TYPED_HOOK_NAMES,
);
