/**
 * Emit as the first statement in each Opik typed hook handler so logs show the
 * handler ran before getClient/session checks or other early exits.
 */
export function logOpikHookEnter(info: (message: string) => void, event: string): void {
  info(`opik: hook_enter event=${event}`);
}
