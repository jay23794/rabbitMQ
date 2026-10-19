// RULE: core/ must NEVER import from events/.
// core/ is the transport layer (connections, channels, logging). events/ builds
// on top of it; importing the other way creates a cycle and couples the
// transport to specific message types.

// Throw this from a handler when retrying can't help (bad input, 404, invalid
// file). The message skips the retry queue and goes straight to the DLQ.
// Any other error is treated as transient and retried.
export class PermanentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PermanentError";
  }
}
