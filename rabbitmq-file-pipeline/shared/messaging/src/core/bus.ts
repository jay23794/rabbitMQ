// RULE: core/ must NEVER import from events/.
// core/ is the transport layer (connections, channels, logging). events/ builds
// on top of it; importing the other way creates a cycle and couples the
// transport to specific message types.

import { ChannelWrapper } from "amqp-connection-manager";
import { ConfirmChannel, ConsumeMessage } from "amqplib";
import { v4 as uuidv4 } from "uuid";
import { createConnection, CreateConnectionOptions } from "./connection";
import { Envelope, EventDefinition } from "./event";
import { consoleLogger } from "./logger";

export interface PublishOptions {
  correlationId?: string; // pass the incoming message's id to keep a flow traceable
}

export interface SubscribeOptions {
  prefetch?: number; // max unacked messages this consumer holds at once (default 1)
}

export type EventHandler<T> = (data: T, ctx: { envelope: Envelope<T> }) => Promise<void>;

export interface MessageBus {
  publish<T>(event: EventDefinition<T>, data: T, opts?: PublishOptions): Promise<Envelope<T>>;
  subscribe<T>(event: EventDefinition<T>, handler: EventHandler<T>, opts?: SubscribeOptions): Promise<void>;
  close(): Promise<void>;
}

const DLX_EXCHANGE = "dlx.exchange";

export function createMessageBus({ url, serviceName, logger = consoleLogger }: CreateConnectionOptions): MessageBus {
  const connection = createConnection({ url, serviceName, logger });

  // amqp-connection-manager channels are confirm channels by default: publish()
  // resolves only after the broker acks the message, and rejects on a nack.
  // Messages published while disconnected are buffered and sent on reconnect.
  const channel: ChannelWrapper = connection.createChannel({ name: `${serviceName}-publisher` });

  // One assert per exchange per process. addSetup also re-runs the assert on
  // every reconnect, so the exchange exists even if the broker was wiped.
  const assertedExchanges = new Map<string, Promise<void>>();

  // One channel per subscription (own prefetch, own setup), closed in close().
  const subscriberChannels: ChannelWrapper[] = [];
  
   
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

  // Queue name is per SERVICE, not per instance:
  //  - different services get different queues bound to the same routing key,
  //    so each service receives its own copy (fan-out between services);
  //  - instances of the same service share one queue, so RabbitMQ hands each
  //    message to only one of them (load-balancing between instances).
  async function subscribe<T>(event: EventDefinition<T>, handler: EventHandler<T>, opts: SubscribeOptions = {}): Promise<void> {
    const exchange = `${event.exchange}.exchange`;
    const queue = `${serviceName}.${event.routingKey}`;
    const dlq = `${queue}.dlq`;

    const subChannel = connection.createChannel({
      name: `${serviceName}-sub-${event.routingKey}`,
      // Runs on first connect AND after every reconnect: re-declares the
      // topology and re-registers the consumer on the fresh channel.
      setup: async (ch: ConfirmChannel) => {
        await ch.assertExchange(exchange, "topic", { durable: true });
        await ch.assertExchange(DLX_EXCHANGE, "direct", { durable: true });

        // Dead-letter queue: where failed messages are parked for inspection.
        await ch.assertQueue(dlq, { durable: true });
        await ch.bindQueue(dlq, DLX_EXCHANGE, dlq);

        // Main queue: a nack(requeue=false) re-routes the message through
        // dlx.exchange with routing key = dlq, landing it in the DLQ above.
        await ch.assertQueue(queue, {
          durable: true,
          arguments: {
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": dlq,
          },
        });
        await ch.bindQueue(queue, exchange, event.routingKey);

        await ch.prefetch(opts.prefetch ?? 1);

        await ch.consume(queue, (msg) => void handleMessage(ch, msg));
      },
    });
    subscriberChannels.push(subChannel);

    async function handleMessage(ch: ConfirmChannel, msg: ConsumeMessage | null): Promise<void> {
      // null = the broker cancelled the consumer (e.g. queue deleted).
      if (!msg) {
        logger.warn(`[${serviceName}] consumer for ${queue} was cancelled by the broker`);
        return;
      }

      const messageId = msg.properties.messageId ?? "unknown";
      try {
        // Parsing is inside the try so malformed messages also go to the DLQ
        // instead of crashing the consumer.
        const envelope = JSON.parse(msg.content.toString()) as Envelope<T>;
        await handler(envelope.data, { envelope });
        ch.ack(msg);
      } catch (err) {
        logger.error(
          `[${serviceName}] handler failed for ${event.routingKey} ${messageId} — sending to ${dlq}:`,
          err,
        );
        // requeue=false: don't retry forever; dead-letter it instead.
        ch.nack(msg, false, false);
      }
    }

    // Resolves once the topology exists and the consumer is registered.
    await subChannel.waitForConnect();
    logger.info(`[${serviceName}] subscribed ${queue} <- ${exchange} (${event.routingKey})`);
  }

  async function close(): Promise<void> {
    await Promise.all(subscriberChannels.map((ch) => ch.close()));
    await channel.close();
    await connection.close();
  }

  return { publish, subscribe, close };
}
