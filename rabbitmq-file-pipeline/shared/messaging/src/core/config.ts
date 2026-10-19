// RULE: core/ must NEVER import from events/.
// core/ is the transport layer (connections, channels, logging). events/ builds
// on top of it; importing the other way creates a cycle and couples the
// transport to specific message types.

// Shared dead-letter exchange for every queue. Each queue dead-letters with
// its own DLQ name as the routing key, so failures still land in separate DLQs.
export const DLX_EXCHANGE = "dlx.exchange";

// Message header carrying the attempt number (1 on first delivery).
export const ATTEMPT_HEADER = "x-attempt";

// Total tries per message, including the first one.
export const DEFAULT_MAX_ATTEMPTS = 3;

// Wait between tries. Stored on each <queue>.retry queue as x-message-ttl, so
// changing it requires deleting existing .retry queues first.
export const DEFAULT_RETRY_DELAY_MS = 15_000;
