// RULE: core/ must NEVER import from events/.
// core/ is the transport layer (connections, channels, logging). events/ builds
// on top of it; importing the other way creates a cycle and couples the
// transport to specific message types.

import amqp, { AmqpConnectionManager } from "amqp-connection-manager";
import { consoleLogger, Logger } from "./logger";

export interface CreateConnectionOptions {
  url: string;
  serviceName: string; // prefixes every log line so multi-service logs are readable
  logger?: Logger;
}

export function createConnection({ url, serviceName, logger = consoleLogger }: CreateConnectionOptions): AmqpConnectionManager {
  const tag = `[${serviceName}][rabbit]`;

  // amqp-connection-manager wraps amqplib: it keeps retrying the initial
  // connect and transparently reconnects (re-running channel setup) after
  // the broker drops, so callers never hold a dead connection.
  const connection = amqp.connect([url], {
    // Short label shown in the RabbitMQ management UI's Connections tab.
    connectionOptions: { clientProperties: { connection_name: serviceName } },
  });

  // "connect" fires on the first connection AND every successful reconnect,
  // so track whether we've connected before to tell them apart.
  let hasConnected = false;

  connection.on("connect", ({ url: connectedUrl }) => {
    if (hasConnected) {
      logger.info(`${tag} reconnected to ${redact(connectedUrl)}`);
    } else {
      hasConnected = true;
      logger.info(`${tag} connected to ${redact(connectedUrl)}`);
    }
  });

  connection.on("disconnect", ({ err }) => {
    logger.warn(`${tag} disconnected${err ? `: ${describe(err)}` : ""} — will retry`);
  });

  connection.on("connectFailed", ({ err, url: failedUrl }) => {
    logger.error(`${tag} connect to ${redact(failedUrl)} failed: ${describe(err)}`);
  });

  connection.on("blocked", ({ reason }) => {
    logger.warn(`${tag} blocked by broker: ${reason}`);
  });

  connection.on("unblocked", () => {
    logger.info(`${tag} unblocked by broker`);
  });

  return connection;
}

// Strips credentials from an AMQP URL before it reaches the logs.
function redact(url: unknown): string {
  const raw = typeof url === "string" ? url : String((url as { hostname?: string })?.hostname ?? url);
  return raw.replace(/\/\/[^@/]*@/, "//***@");
}

// Some socket errors (e.g. ECONNREFUSED surfaced as an AggregateError) have an
// empty message, so fall back to the error code.
function describe(err: Error): string {
  return err.message || (err as NodeJS.ErrnoException).code || err.name;
}
