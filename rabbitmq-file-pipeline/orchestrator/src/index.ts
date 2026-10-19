import "dotenv/config";
import { randomUUID } from "crypto";
import express from "express";
import { createMessageBus, FileProcessed, FileUploaded, FileUploadedData } from "@app/messaging";

const PORT = Number(process.env.PORT ?? 3000);
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

// One bus per process: owns the connection, reconnects on its own and
// re-declares exchanges/queues after a reconnect.
const bus = createMessageBus({ url: RABBITMQ_URL, serviceName: "orchestrator" });

const app = express();
app.use(express.json());

// Publishes fake FileUploaded events so the pipeline can be tested end to end
// before real uploads exist. Query params:
//   ?fail=true  -> simulateFailure: true (consumer throws -> DLQ)
//   ?count=15   -> publish N messages
app.post("/test-publish", async (req, res) => {
  const fail = req.query.fail === "true";
  const count = req.query.count !== undefined ? Number(req.query.count) : 1;

  if (!Number.isInteger(count) || count < 1 || count > 1000) {
    res.status(400).json({ error: "count must be an integer between 1 and 1000" });
    return;
  }

  const messages: FileUploadedData[] = [];
  for (let i = 0; i < count; i++) {
    const fileId = randomUUID();
    const data: FileUploadedData = {
      fileId,
      fileName: `sample-${i + 1}.pdf`,
      mimeType: "application/pdf",
      signedUrl: `https://example.com/fake-signed-url/${fileId}?token=dummy`,
      uploadedAt: new Date().toISOString(),
      ...(fail && { simulateFailure: true }),
    };
    // Resolves once the broker has confirmed it stored the message.
    await bus.publish(FileUploaded, data);
    messages.push(data);
  }

  // 202 Accepted: the jobs are queued, not processed yet.
  res.status(202).json({
    queued: messages.length,
    exchange: `${FileUploaded.exchange}.exchange`,
    routingKey: FileUploaded.routingKey,
    messages,
  });
});

async function main(): Promise<void> {
  console.log("MAIN");
  
  // Subscribe before listening so results for jobs published from this
  // process are never missed.
  await bus.subscribe(FileProcessed, async (data) => {
    if (data.status === "success") {
      console.log(`✅ file ${data.fileId} processed (${data.size} bytes)`);
    } else {
      console.log(`❌ ${data.fileId} failed: ${data.error}`);
    }
  });

  const server = app.listen(PORT, () => {
    console.log(`[orchestrator] listening on http://localhost:${PORT}`);
  });

  const shutdown = async (): Promise<void> => {
    server.close();
    await bus.close();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("[orchestrator] failed to start:", err);
  process.exit(1);
});
