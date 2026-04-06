import type { Opik } from "opik";

export type SharedOpikRuntimeConfig = {
  projectName: string;
  tags: string[];
  toolResultPersistSanitizeEnabled: boolean;
  /** Normalized, no trailing slashes. */
  attachmentBaseUrl: string;
};

type SharedOpikRuntimeState = {
  client: Opik | null;
  config: SharedOpikRuntimeConfig | null;
  lastUpdatedAtMs: number;
  lastUpdatedByInstanceId: string | null;
};

const sharedKey = Symbol.for("opik-openclaw.shared-runtime-state");

function createInitialState(): SharedOpikRuntimeState {
  return {
    client: null,
    config: null,
    lastUpdatedAtMs: 0,
    lastUpdatedByInstanceId: null,
  };
}

export function getSharedOpikRuntimeState(): SharedOpikRuntimeState {
  const g = globalThis as unknown as Record<PropertyKey, unknown>;
  const existing = g[sharedKey];
  if (existing && typeof existing === "object") {
    return existing as SharedOpikRuntimeState;
  }
  const next = createInitialState();
  g[sharedKey] = next as unknown;
  return next;
}

export function setSharedOpikClient(params: {
  instanceId: string;
  client: Opik;
}): void {
  const state = getSharedOpikRuntimeState();
  state.client = params.client;
  state.lastUpdatedAtMs = Date.now();
  state.lastUpdatedByInstanceId = params.instanceId;
}

export function setSharedOpikRuntimeConfig(params: {
  instanceId: string;
  config: SharedOpikRuntimeConfig;
}): void {
  const state = getSharedOpikRuntimeState();
  state.config = params.config;
  state.lastUpdatedAtMs = Date.now();
  state.lastUpdatedByInstanceId = params.instanceId;
}

