import { Channel } from "amqplib";

// Keep this file identical in orchestrator and file-processor.

export const EXCHANGE = "files.exchange"; // topic exchange producers publish to
export const ROUTING_KEY = "file.uploaded"; // key used when a file upload is published
export const QUEUE = "file.processing.q"; // work queue the file-processor consumes
export const DLX = "files.dlx"; // dead-letter exchange for failed/dropped messages
export const DLQ = "file.processing.dlq"; // queue where dead-lettered messages end up
export const DLQ_ROUTING_KEY = "file.failed"; // key dead-lettered messages are routed with

// Both services call setupTopology() on startup. Declarations are idempotent
// (no-op if it already exists with the same options), so whichever service
// starts first builds everything. This matters most for the producer: publishing
// to an exchange with no bound queue is not an error — RabbitMQ silently drops
// the message as unroutable. Declaring the queue + binding from the producer
// side too guarantees messages have somewhere to go even if the consumer has
// never started.
//
// Options must match across both services, or RabbitMQ rejects the second
// declaration with 406 PRECONDITION_FAILED. Changing arguments on an existing
// queue requires deleting it first.
export async function setupTopology(channel: Channel): Promise<void> {
  // 1. Dead-letter exchange (direct): receives rejected/expired/overflowed
  //    messages from QUEUE. Declared first so it exists before QUEUE points at it.
  await channel.assertExchange(DLX, "direct", { durable: true });

  // 2. Dead-letter queue: durable storage for failed messages so they can be
  //    inspected or replayed instead of being lost.
  await channel.assertQueue(DLQ, { durable: true });

  // 3. Route messages arriving at DLX with key "file.failed" into the DLQ.
  await channel.bindQueue(DLQ, DLX, DLQ_ROUTING_KEY);

  // 4. Main exchange (topic): producers publish here instead of straight to a
  //    queue, so more queues can subscribe to "file.*" patterns later.
  await channel.assertExchange(EXCHANGE, "topic", { durable: true });

  // 5. Main work queue (classic, durable).
  await channel.assertQueue(QUEUE, {
    durable: true,
    arguments: {
      // nack(requeue=false), expired, or overflowed messages go to DLX...
      "x-dead-letter-exchange": DLX,
      // ...with this routing key, which the DLQ binding (step 3) matches.
      "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
      // Cap at 10 ready messages. Default overflow "drop-head" removes the
      // OLDEST message when full, and since a DLX is set it is dead-lettered
      // to the DLQ rather than discarded.
      "x-max-length": 10,
    },
  });

  // 6. Deliver messages published to EXCHANGE with key "file.uploaded" into QUEUE.
  await channel.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY);
}
