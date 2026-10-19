// RULE: core/ must NEVER import from events/.
// core/ is the transport layer (connections, channels, logging). events/ builds
// on top of it; importing the other way creates a cycle and couples the
// transport to specific message types.

import { ChannelWrapper } from "amqp-connection-manager";
import { ConfirmChannel } from "amqplib";
import { v4 as uuidv4 } from "uuid";
import { createConnection, CreateConnectionOptions } from "./connection";
import { Envelope, EventDefinition } from "./event";
import { consoleLogger } from "./logger";

export interface PublishOptions {
  correlationId?: string; // pass the incoming message's id to keep a flow traceable
}

export interface MessageBus {
  publish<T>(event: EventDefinition<T>, data: T, opts?: PublishOptions): Promise<Envelope<T>>;
  subscribe(): never;
  close(): Promise<void>;
}

export function createMessageBus({ url, serviceName, logger = consoleLogger }: CreateConnectionOptions): MessageBus {
  const connection = createConnection({ url, serviceName, logger });

  // amqp-connection-manager channels are confirm channels by default: publish()
  // resolves only after the broker acks the message, and rejects on a nack.
  // Messages published while disconnected are buffered and sent on reconnect.
  const channel: ChannelWrapper = connection.createChannel({ name: `${serviceName}-publisher` });

  // One assert per exchange per process. addSetup also re-runs the assert on
  // every reconnect, so the exchange exists even if the broker was wiped.
  const assertedExchanges = new Map<string, Promise<void>>();

  function ensureExchange(exchange: string): Promise<void> {
    let pending = assertedExchanges.get(exchange);
    if (!pending) {
      pending = channel.addSetup(async (ch: ConfirmChannel) => {
        await ch.assertExchange(exchange, "topic", { durable: true });
      });
      // Don't cache a failure; let the next publish try again.
      pending.catch(() => assertedExchanges.delete(exchange));
      assertedExchanges.set(exchange, pending);
    }
    return pending;
  }

  async function publish<T>(event: EventDefinition<T>, data: T, opts: PublishOptions = {}): Promise<Envelope<T>> {
    const exchange = `${event.exchange}.exchange`;
    await ensureExchange(exchange);

    const messageId = uuidv4();
    const envelope: Envelope<T> = {
      messageId,
      type: event.routingKey,
      source: serviceName,
      timestamp: new Date().toISOString(),
      // A message with no parent starts a new flow, so it correlates to itself.
      correlationId: opts.correlationId ?? messageId,
      data,
    };

    // Persistent + durable exchange/queue = survives a broker restart.
    // Envelope fields are mirrored into AMQP properties so they show up in the
    // management UI and can be read without parsing the body.
    await channel.publish(exchange, event.routingKey, Buffer.from(JSON.stringify(envelope)), {
      persistent: true,
      contentType: "application/json",
      messageId,
      correlationId: envelope.correlationId,
      type: envelope.type,
      appId: serviceName,
      timestamp: Math.floor(Date.now() / 1000),
    });

    logger.info(`[${serviceName}] published ${event.routingKey} ${messageId}`);
    return envelope;
  }

  function subscribe(): never {
    // TODO: subscribe<T>(event, queue, handler) — assert queue, bind to
    // `${event.exchange}.exchange` with event.routingKey, parse Envelope<T>,
    // ack on success / nack to DLQ on failure.
    throw new Error("subscribe() is not implemented yet");
  }

  async function close(): Promise<void> {
    await channel.close();
    await connection.close();
  }

  return { publish, subscribe, close };
}
