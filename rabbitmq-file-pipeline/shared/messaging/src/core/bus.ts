// RULE: core/ must NEVER import from events/.
// core/ is the transport layer (connections, channels, logging). events/ builds
// on top of it; importing the other way creates a cycle and couples the
// transport to specific message types.

import { ChannelWrapper } from "amqp-connection-manager";
import { ConfirmChannel, ConsumeMessage } from "amqplib";
import { v4 as uuidv4 } from "uuid";
import { ATTEMPT_HEADER, DEFAULT_MAX_ATTEMPTS, DEFAULT_RETRY_DELAY_MS, DLX_EXCHANGE } from "./config";
import { createConnection, CreateConnectionOptions } from "./connection";
import { PermanentError } from "./errors";
import { Envelope, EventDefinition } from "./event";
import { consoleLogger } from "./logger";

export interface PublishOptions {
  correlationId?: string; // pass the incoming message's id to keep a flow traceable
}

export interface SubscribeOptions {
  prefetch?: number; // max unacked messages this consumer holds at once (default 1)
  maxAttempts?: number; // total tries incl. the first one (default 3)
  retryDelayMs?: number; // wait between tries (default 15000)
}

export interface HandlerContext<T> {
  envelope: Envelope<T>;
  attempt: number; // 1 on first delivery, 2 on first retry, ...
  maxAttempts: number;
  // true when a failure now goes to the DLQ instead of being retried. Use it to
  // report a failure once (e.g. publish "failed") rather than on every try.
  isLastAttempt: boolean;
}

export type EventHandler<T> = (data: T, ctx: HandlerContext<T>) => Promise<void>;

export interface MessageBus {
  publish<T>(event: EventDefinition<T>, data: T, opts?: PublishOptions): Promise<Envelope<T>>;
  subscribe<T>(event: EventDefinition<T>, handler: EventHandler<T>, opts?: SubscribeOptions): Promise<void>;
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
  //
  // Failure handling:
  //   transient error, attempts left -> <queue>.retry, waits retryDelayMs, back to <queue>
  //   last attempt or PermanentError -> <queue>.dlq (kept until handled by hand)
  // No TTL on the main queue: jobs wait as long as the consumer is down.
  async function subscribe<T>(event: EventDefinition<T>, handler: EventHandler<T>, opts: SubscribeOptions = {}): Promise<void> {
    const exchange = `${event.exchange}.exchange`;
    const queue = `${serviceName}.${event.routingKey}`;
    const dlq = `${queue}.dlq`;
    const retryQueue = `${queue}.retry`;
    const maxAttempts = opts.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    const retryDelayMs = opts.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;

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

        // Retry queue: nobody consumes it. Messages sit for retryDelayMs, then
        // expire and are dead-lettered via the default exchange ("") straight
        // back into THIS service's queue only, so other services never see the
        // retry.
        await ch.assertQueue(retryQueue, {
          durable: true,
          arguments: {
            "x-message-ttl": retryDelayMs,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": queue,
          },
        });

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
      const attempt = Number(msg.properties.headers?.[ATTEMPT_HEADER] ?? 1);
      const isLastAttempt = attempt >= maxAttempts;

      let envelope: Envelope<T>;
      try {
        envelope = JSON.parse(msg.content.toString()) as Envelope<T>;
      } catch (err) {
        // Malformed body will never parse, so retrying is pointless.
        logger.error(`[${serviceName}] invalid JSON in ${event.routingKey} ${messageId} — sending to ${dlq}:`, err);
        ch.nack(msg, false, false);
        return;
      }

      try {
        await handler(envelope.data, { envelope, attempt, maxAttempts, isLastAttempt });
        ch.ack(msg);
      } catch (err) {
        if (err instanceof PermanentError || isLastAttempt) {
          const why = err instanceof PermanentError ? "permanent error" : `attempt ${attempt}/${maxAttempts}`;
          logger.error(`[${serviceName}] ${event.routingKey} ${messageId} failed (${why}) — sending to ${dlq}:`, err);
          // requeue=false: dead-letter it via the main queue's DLX settings.
          ch.nack(msg, false, false);
          return;
        }

        logger.warn(
          `[${serviceName}] ${event.routingKey} ${messageId} failed (attempt ${attempt}/${maxAttempts}), ` +
            `retrying in ${retryDelayMs / 1000}s: ${err instanceof Error ? err.message : String(err)}`,
        );
        try {
          // Copy into the retry queue with the next attempt number, wait for
          // the broker to confirm, and only then ack the original. A crash in
          // between can cause a duplicate, never a lost message.
          await sendToQueueConfirmed(ch, retryQueue, msg.content, {
            ...msg.properties,
            headers: { ...msg.properties.headers, [ATTEMPT_HEADER]: attempt + 1 },
          });
          ch.ack(msg);
        } catch (retryErr) {
          // Couldn't schedule the retry: put it back on the queue rather than lose it.
          logger.error(`[${serviceName}] could not schedule retry for ${messageId}, requeueing:`, retryErr);
          ch.nack(msg, false, true);
        }
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

// Raw ConfirmChannel.sendToQueue takes a callback; wrap it so the caller can
// await the broker's confirm.
function sendToQueueConfirmed(
  ch: ConfirmChannel,
  queue: string,
  content: Buffer,
  options: Parameters<ConfirmChannel["sendToQueue"]>[2],
): Promise<void> {
  return new Promise((resolve, reject) => {
    ch.sendToQueue(queue, content, options, (err) => (err ? reject(err) : resolve()));
  });
}
