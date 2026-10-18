import amqp, { Channel, ChannelModel, ConsumeMessage } from "amqplib";

// Must match the orchestrator's queue name and message shape exactly.
export const QUEUE = "file.uploaded.test";

export interface FileUploadedMessage {
  fileId: string;
  fileName: string;
  mimeType: string;
  signedUrl: string;
  uploadedAt: string; // ISO 8601 timestamp
}

let connection: ChannelModel | null = null;
let channel: Channel | null = null;

export async function startConsumer(url: string): Promise<void> {
  connection = await amqp.connect(url);
  channel = await connection.createChannel();

  // Same declaration as the producer. Whichever service starts first creates
  // the queue; the options must match or RabbitMQ rejects the call.
  //await channel.assertQueue(QUEUE, { durable: true });

  // prefetch(1) = RabbitMQ sends this consumer at most 1 unacknowledged
  // message at a time. The next one arrives only after we ack/nack the
  // current one, so a slow file never piles up work in this process.
  await channel.prefetch(1);

  connection.on("error", (err) => console.error("[rabbit] connection error:", err));
  connection.on("close", () => console.warn("[rabbit] connection closed"));

  const ch = channel;

  // noAck: false = manual acknowledgement. RabbitMQ keeps the message
  // (marked "unacked") until we explicitly ack or nack it. If this process
  // dies mid-processing, the message goes back to the queue.
  await ch.consume(
    QUEUE,
    (msg: ConsumeMessage | null) => {
      if (!msg) return; // consumer was cancelled by the broker

      try {
        const payload = JSON.parse(msg.content.toString()) as FileUploadedMessage;
        console.log("[consumer] received:", payload);

        // ack = "done, delete it". RabbitMQ removes the message from the queue.
        ch.ack(msg);
      } catch (err) {
        console.error("[consumer] failed to process message:", err);

        // nack(msg, allUpTo=false, requeue=false) = reject this one message
        // and DON'T put it back. Otherwise a bad message (e.g. invalid JSON)
        // would be redelivered forever. With no dead-letter exchange set up,
        // it is discarded.
        ch.nack(msg, false, false);
      }
    },
    { noAck: false },
  );

  console.log(`[consumer] waiting for messages on "${QUEUE}"`);
}

export async function stopConsumer(): Promise<void> {
  await channel?.close();
  await connection?.close();
}
