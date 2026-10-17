import amqp, { Channel, ChannelModel } from "amqplib";

export const QUEUE = "file.uploaded";

export interface FileUploadedMessage {
  fileId: string;
  fileName: string;
  mimeType: string;
  signedUrl: string;
  uploadedAt: string; // ISO 8601 timestamp
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

  // Durable queue = the queue definition survives a broker restart.
  // (assertQueue is idempotent: creates it if missing, no-op if it exists.)
  await channel.assertQueue(QUEUE, { durable: true });

  connection.on("error", (err) => console.error("[rabbit] connection error:", err));
  connection.on("close", () => console.warn("[rabbit] connection closed"));

  console.log(`[rabbit] connected, queue "${QUEUE}" ready`);
}

export function publishFileUploaded(msg: FileUploadedMessage): boolean {
  if (!channel) {
    throw new Error("RabbitMQ channel not initialised — call connectRabbit() first");
  }

  // Persistent message = written to disk by the broker, so it survives a
  // restart too. Needs BOTH a durable queue and persistent: true to be safe.
  return channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(msg)), {
    persistent: true,
    contentType: "application/json",
  });
}

export async function closeRabbit(): Promise<void> {
  await channel?.close();
  await connection?.close();
}
