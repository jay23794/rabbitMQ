// RULE: core/ must NEVER import from events/.
// core/ is the transport layer (connections, channels, logging). events/ builds
// on top of it; importing the other way creates a cycle and couples the
// transport to specific message types.

export interface Logger {
  info(message: string, ...meta: unknown[]): void;
  warn(message: string, ...meta: unknown[]): void;
  error(message: string, ...meta: unknown[]): void;
}

export const consoleLogger: Logger = {
  info: (message, ...meta) => console.log(message, ...meta),
  warn: (message, ...meta) => console.warn(message, ...meta),
  error: (message, ...meta) => console.error(message, ...meta),
};
