// RULE: core/ must NEVER import from events/.
// core/ is the transport layer (connections, channels, logging). events/ builds
// on top of it; importing the other way creates a cycle and couples the
// transport to specific message types.

export interface EventDefinition<T> {
  exchange: string;
  routingKey: string;
  // Phantom field: never set at runtime, only carries T so publish/subscribe
  // can infer the payload type from the event definition.
  __type?: T;
}

export function defineEvent<T>(def: { exchange: string; routingKey: string }): EventDefinition<T> {
  return { exchange: def.exchange, routingKey: def.routingKey };
}

// Every message on the wire is wrapped in an Envelope so consumers get the same
// metadata (ids, origin, time) regardless of the event type.
export interface Envelope<T> {
  messageId: string; // unique per message; use it to de-duplicate redeliveries
  type: string; // the event's routing key, e.g. "file.uploaded"
  source: string; // serviceName of the publisher
  timestamp: string; // ISO 8601
  correlationId: string; // shared by every message in one flow, for tracing
  data: T;
}
