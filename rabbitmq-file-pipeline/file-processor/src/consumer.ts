import { FileProcessed, FileUploaded, FileUploadedData, MessageBus, PermanentError } from "@app/messaging";
import { filesCollection } from "./db";

export async function registerHandlers(bus: MessageBus): Promise<void> {
  // Creates/binds file-processor.file.uploaded (+ .retry, .dlq). Throwing from
  // the handler retries the message (3 tries, 15s apart), then dead-letters it.
  // Throwing PermanentError skips the retries.
  await bus.subscribe(FileUploaded, async (data, { envelope, attempt, maxAttempts, isLastAttempt }) => {
    // Results belong to the same flow as the upload that caused them.
    const correlationId = envelope.correlationId;
    console.log(`[consumer] received ${data.fileId} (${data.fileName}) attempt ${attempt}/${maxAttempts}`);

    try {
      if (data.simulateFailure) {
        throw new PermanentError("simulated");
      }
      const size = await downloadAndSave(data);
      await bus.publish(FileProcessed, { fileId: data.fileId, status: "success", size }, { correlationId });
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      const final = err instanceof PermanentError || isLastAttempt;

      // Report the failure once, when no retry will follow; otherwise the
      // orchestrator would log ❌ for every attempt.
      if (final) {
        await markFailed(data, error);
        await bus.publish(FileProcessed, { fileId: data.fileId, status: "failed", error }, { correlationId });
      } else {
        await markRetrying(data, error);
      }
      throw err; // bus decides: retry queue or DLQ
    }
  });
}

async function downloadAndSave(data: FileUploadedData): Promise<number> {
  const files = filesCollection();
  const now = new Date();

  // Upsert keyed on fileId, so a redelivered message updates the same record
  // instead of creating a duplicate.
  await files.updateOne(
    { _id: data.fileId },
    {
      $set: { fileName: data.fileName, mimeType: data.mimeType, status: "processing", updatedAt: now },
      $setOnInsert: { createdAt: now },
    },
    { upsert: true },
  );

  const res = await fetch(data.signedUrl);
  if (!res.ok) {
    // 4xx (missing file, expired/invalid signed URL) won't fix itself;
    // 5xx may be a temporary storage outage, so let it retry.
    const message = `download failed: HTTP ${res.status}`;
    throw res.status >= 400 && res.status < 500 ? new PermanentError(message) : new Error(message);
  }
  // Buffers the whole file in memory; fine for now, switch to streaming to
  // disk for the 100 MB limit (plan.md Phase 4).
  const sizeBytes = (await res.arrayBuffer()).byteLength;

  await files.updateOne(
    { _id: data.fileId },
    { $set: { sizeBytes, status: "downloaded", updatedAt: new Date() }, $unset: { error: "" } },
  );

  console.log(`[consumer] saved ${data.fileId} (${sizeBytes} bytes)`);
  return sizeBytes;
}

async function markFailed(data: FileUploadedData, error: string): Promise<void> {
  const now = new Date();
  await filesCollection().updateOne(
    { _id: data.fileId },
    {
      $set: { fileName: data.fileName, mimeType: data.mimeType, status: "failed", error, updatedAt: now },
      $setOnInsert: { createdAt: now },
    },
    { upsert: true },
  );
}

async function markRetrying(data: FileUploadedData, error: string): Promise<void> {
  await filesCollection().updateOne(
    { _id: data.fileId },
    { $set: { status: "retrying", error, updatedAt: new Date() } },
  );
}
