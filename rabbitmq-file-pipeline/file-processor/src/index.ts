import "dotenv/config";
import express from "express";
import { createMessageBus } from "@app/messaging";
import { registerHandlers } from "./consumer";
import { closeMongo, connectMongo, filesCollection } from "./db";

const PORT = Number(process.env.PORT ?? 4000);
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";
const MONGO_URL = process.env.MONGO_URL ?? "mongodb://localhost:27017/file-pipeline";

const app = express();

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

// Latest file records first. Failed messages themselves are in the
// file-processor.file.uploaded.dlq queue (RabbitMQ UI).
app.get("/files", async (_req, res) => {
  const files = await filesCollection().find().sort({ updatedAt: -1 }).limit(100).toArray();
  res.json(files);
});

async function main(): Promise<void> {
  // Mongo first: handlers write to it as soon as messages start arriving.
  await connectMongo(MONGO_URL);

  const bus = createMessageBus({ url: RABBITMQ_URL, serviceName: "file-processor" });

  // Start consuming before listening, so a running server always means
  // the consumer is attached to the queue.
  await registerHandlers(bus);

  const server = app.listen(PORT, () => {
    console.log(`[file-processor] listening on http://localhost:${PORT}`);
  });

  const shutdown = async (): Promise<void> => {
    server.close();
    await bus.close();
    await closeMongo();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("[file-processor] failed to start:", err);
  process.exit(1);
});
