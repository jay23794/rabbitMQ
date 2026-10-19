import amqp, { Channel, ChannelModel, Options } from "amqplib";
import { EXCHANGE, QUEUE, ROUTING_KEY, setupTopology } from "./topology";

export interface FileUploadedMessage {
  fileId: string;
  fileName: string;
  mimeType: string;
  signedUrl: string;
  uploadedAt: string; // ISO 8601 timestamp
  simulateFailure?: boolean; // test only: tells the consumer to fail on purpose
}

export interface PublishOptions {
  ttlMs?: number; // per-message TTL; expired messages are dead-lettered
}

let connection: ChannelModel | null = null;
let channel: Channel | null = null;

export async function connectRabbit(url: string): Promise<void> {
  // Connection = one long-lived TCP socket to the broker. Expensive to open,
  // so the service creates it once at startup and reuses it.
  connection = await amqp.connect(url);

  // Channel = lightweight virtual connection multiplexed over the TCP socket.
  // All publishing/consuming happens on a channel, not the connection itself.
  channel = await connection.createChannel();

  // Declares exchanges, queues and bindings (see topology.ts) so published
  // messages are routable even if the file-processor hasn't started yet.
  await setupTopology(channel);

  connection.on("error", (err) => console.error("[rabbit] connection error:", err));
  connection.on("close", () => console.warn("[rabbit] connection closed"));

  console.log(`[rabbit] connected, topology ready (${EXCHANGE} -> ${QUEUE})`);
}

export function publishFileUploaded(msg: FileUploadedMessage, options: PublishOptions = {}): boolean {
  if (!channel) {
    throw new Error("RabbitMQ channel not initialised — call connectRabbit() first");
  }

  // expiration must be a string of milliseconds (AMQP spec).
  const extraOptions: Options.Publish = options.ttlMs !== undefined ? { expiration: String(options.ttlMs) } : {};

  // Persistent message = written to disk by the broker, so it survives a
  // restart too. Needs BOTH a durable queue and persistent: true to be safe.
  // Publishing to the exchange (not the queue) lets bindings decide routing.
  const ok = channel.publish(EXCHANGE, ROUTING_KEY, Buffer.from(JSON.stringify(msg)), {
    persistent: true,
    contentType: "application/json",
    ...extraOptions,
  });

  console.log(
    `[rabbit] published ${msg.fileId} -> exchange="${EXCHANGE}" routingKey="${ROUTING_KEY}"` +
      (options.ttlMs !== undefined ? ` ttl=${options.ttlMs}ms` : "") +
      (msg.simulateFailure ? " simulateFailure=true" : ""),
  );

  return ok;
}

export async function closeRabbit(): Promise<void> {
  await channel?.close();
  await connection?.close();
}
