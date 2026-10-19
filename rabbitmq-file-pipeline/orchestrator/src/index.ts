import "dotenv/config";
import { randomUUID } from "crypto";
import express from "express";
import { closeRabbit, connectRabbit, FileUploadedMessage, publishFileUploaded } from "./rabbit";
import { EXCHANGE, ROUTING_KEY } from "./topology";

const PORT = Number(process.env.PORT ?? 3000);
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const app = express();
app.use(express.json());

// Publishes fake FileUploadedMessages so the pipeline can be tested end to end
// before real uploads exist. Query params:
//   ?fail=true  -> simulateFailure: true (consumer nacks -> DLQ)
//   ?ttl=5000   -> per-message TTL in ms (expires -> DLQ if not consumed)
//   ?count=15   -> publish N messages (queue max-length is 10 -> oldest go to DLQ)
app.post("/test-publish", (req, res) => {
  console.log("********");

  const fail = req.query.fail === "true";
  const ttlMs = req.query.ttl !== undefined ? Number(req.query.ttl) : undefined;
  const count = req.query.count !== undefined ? Number(req.query.count) : 1;

  if (ttlMs !== undefined && (!Number.isInteger(ttlMs) || ttlMs < 0)) {
    res.status(400).json({ error: "ttl must be a non-negative integer (ms)" });
    return;
  }
  if (!Number.isInteger(count) || count < 1 || count > 1000) {
    res.status(400).json({ error: "count must be an integer between 1 and 1000" });
    return;
  }

  const messages: FileUploadedMessage[] = [];
  for (let i = 0; i < count; i++) {
    const fileId = randomUUID();
    const msg: FileUploadedMessage = {
      fileId,
      fileName: `sample-${i + 1}.pdf`,
      mimeType: "application/pdf",
      signedUrl: `https://example.com/fake-signed-url/${fileId}?token=dummy`,
      uploadedAt: new Date().toISOString(),
      ...(fail && { simulateFailure: true }),
    };
    publishFileUploaded(msg, { ttlMs });
    messages.push(msg);
  }

  // 202 Accepted: the jobs are queued, not processed yet.
  res.status(202).json({
    queued: messages.length,
    exchange: EXCHANGE,
    routingKey: ROUTING_KEY,
    ttlMs: ttlMs ?? null,
    messages,
  });
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
