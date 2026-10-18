import "dotenv/config";
import express from "express";
import { startConsumer, stopConsumer } from "./consumer";

const PORT = Number(process.env.PORT ?? 4000);
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const app = express();

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

async function main(): Promise<void> {
  // Start consuming before listening, so a running server always means
  // the consumer is attached to the queue.
  await startConsumer(RABBITMQ_URL);

  const server = app.listen(PORT, () => {
    console.log(`[file-processor] listening on http://localhost:${PORT}`);
  });

  const shutdown = async (): Promise<void> => {
    server.close();
    await stopConsumer();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("[file-processor] failed to start:", err);
  process.exit(1);
});
