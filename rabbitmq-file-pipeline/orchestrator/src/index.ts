import "dotenv/config";
import { randomUUID } from "crypto";
import express from "express";
import { closeRabbit, connectRabbit, FileUploadedMessage, publishFileUploaded, QUEUE } from "./rabbit";

const PORT = Number(process.env.PORT ?? 3000);
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const app = express();
app.use(express.json());

// Publishes a fake FileUploadedMessage so the queue can be tested end to end
// before real uploads exist.
app.post("/test-publish", (_req, res) => {
  console.log("********");
  
  const fileId = randomUUID();
  const msg: FileUploadedMessage = {
    fileId,
    fileName: "sample.pdf",
    mimeType: "application/pdf",
    signedUrl: `https://example.com/fake-signed-url/${fileId}?token=dummy`,
    uploadedAt: new Date().toISOString(),
  };

  publishFileUploaded(msg);

  // 202 Accepted: the job is queued, not processed yet.
  res.status(202).json({ queued: true, queue: QUEUE, message: msg });
});

async function main(): Promise<void> {
  // Connect to RabbitMQ first so the HTTP server never accepts requests
  // it can't publish.
  await connectRabbit(RABBITMQ_URL);

  const server = app.listen(PORT, () => {
    console.log(`[orchestrator] listening on http://localhost:${PORT}`);
  });

  const shutdown = async (): Promise<void> => {
    server.close();
    await closeRabbit();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("[orchestrator] failed to start:", err);
  process.exit(1);
});
