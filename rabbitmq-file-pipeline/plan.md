# File Pipeline Plan — OCR + Vector Ingest

Upload → queue → split into page batches → OCR → embed → vector DB, using RabbitMQ
for job distribution and MongoDB for job tracking.

---

## 1. Constraints

| Limit | Value |
|---|---|
| Max file size | **100 MB** |
| Max pages per file | **~1,000** |
| Max files per upload | **4** |
| Max total per upload | **400 MB / ~4,000 pages** |
| File types | PDF (scanned/image-based), images (JPG / PNG / TIFF) |

The orchestrator enforces these limits at upload time (reject with `413` / `400`).

**Assumption:** sizing below is for **one full upload (4 files) at a time**. If several
users upload at once, jobs queue up and finish later. See §9 for scaling.

---

## 2. Architecture

```
client
  │ POST /upload (≤ 4 files, ≤ 100 MB each)
  ▼
orchestrator ──▶ object storage (signed URL)
  │ 1 msg per file
  ▼
files.exchange (topic) ──file.uploaded──▶ file.processing.q
                                              │ prefetch 1
                                              ▼
                                      splitter (file-processor)
                                      • download → count pages
                                      • create parent record in Mongo
                                      • publish N batch msgs → ack
                                              │
files.exchange (topic) ──ocr.batch──▶ ocr.batch.q
                                              │ prefetch = OCR_CONCURRENCY
                                              ▼
                                      OCR workers (file-processor)
                                      • download file to temp disk
                                      • page by page: render → OCR → save text
                                      • chunk → embed → upsert vectors
                                      • mark batch done in Mongo → ack
                                              │
                                              ▼
                                      Mongo parent: 20/20 → status "indexed"

failures (nack / delivery limit) ──▶ files.dlx ──▶ DLQs
```

### Key ideas
- **Fan-out / fan-in:** 1 file = 1 parent record (Mongo) + N child jobs (RabbitMQ messages).
- **Queues are fixed infrastructure**, shared by all files. No queue per file or per job.
- **Messages carry IDs and page ranges only**, never file content.

---

## 3. RabbitMQ topology

Declared in `src/topology.ts` (identical copy in both services) by `setupTopology()`,
called on startup by **both** services so whichever starts first creates everything.

| Kind | Name | Type / args | Purpose |
|---|---|---|---|
| Exchange | `files.exchange` | topic, durable | All producers publish here |
| Exchange | `files.dlx` | direct, durable | Dead-letter exchange |
| Queue | `file.processing.q` | durable, DLX → `files.dlx` / `file.failed` | 1 msg per file (splitter input) |
| Queue | `ocr.batch.q` | durable, DLX → `files.dlx` / `ocr.failed` | 1 msg per page batch |
| Queue | `file.processing.dlq` | durable | Failed file/split jobs |
| Queue | `ocr.batch.dlq` | durable | Failed batch jobs |

| Binding | Routing key |
|---|---|
| `files.exchange` → `file.processing.q` | `file.uploaded` |
| `files.exchange` → `ocr.batch.q` | `ocr.batch` |
| `files.dlx` → `file.processing.dlq` | `file.failed` |
| `files.dlx` → `ocr.batch.dlq` | `ocr.failed` |

### Production changes vs. the current test topology
- [ ] **Remove `x-max-length: 10`.** One upload creates 80 batch messages; a cap of 10 would dead-letter real work.
- [ ] **Switch work queues to quorum queues** with `x-delivery-limit: 3`, so a message that keeps
      crashing the worker goes to the DLQ instead of looping forever.
      (Changing queue type/args requires deleting and recreating the queue.)
- [ ] **No TTL on real jobs.** TTL is only for testing the DLQ (`/test-publish?ttl=`).
- [ ] Add the `ocr.batch.q` / `ocr.batch.dlq` declarations and bindings.

---

## 4. Messages

```ts
// file.processing.q — routing key "file.uploaded" (1 per file)
interface FileUploadedMessage {
  fileId: string;
  fileName: string;
  mimeType: string;
  signedUrl: string;
  uploadedAt: string;        // ISO 8601
  simulateFailure?: boolean; // test only
}

// ocr.batch.q — routing key "ocr.batch" (N per file)
interface OcrBatchMessage {
  fileId: string;
  batchIndex: number;        // 0-based
  startPage: number;         // inclusive, 1-based
  endPage: number;           // inclusive
  signedUrl: string;
  mimeType: string;
}
```

All messages: `persistent: true`, `contentType: "application/json"`.

---

## 5. MongoDB records (parent job tracking)

```js
// collection: files — the PARENT
{
  _id: "<fileId>",
  uploadId: "<uploadId>",          // groups the ≤ 4 files of one upload
  fileName: "contract.pdf",
  mimeType: "application/pdf",
  sizeBytes: 104857600,
  totalPages: 1000,
  batchSize: 50,
  totalBatches: 20,
  doneBatches: [0, 1, 2],          // $addToSet → safe on redelivery
  failedBatches: [],
  status: "queued" | "splitting" | "processing" | "indexed" | "partial" | "failed",
  createdAt, updatedAt
}

// collection: pages (optional, for debugging / re-embedding)
{ fileId, page, text, ocrConfidence, createdAt }
```

**Rules**
- Mark a batch done with `$addToSet: { doneBatches: batchIndex }`, never `$inc`. A redelivered
  batch must not count twice.
- The worker that makes `doneBatches.length == totalBatches` sets `status: "indexed"`
  (use a conditional update so only one worker does it).
- If any batch reaches the DLQ, set the file to `"partial"` and keep the failed batch index for retry.

---

## 6. Processing rules

### Batching
- **Batch size: 50 pages** (configurable via `OCR_BATCH_SIZE`, sensible range 25–100).
- 1,000-page file → **20 batches**. 4 files → **80 batches**.
- Images (JPG/PNG/TIFF) → 1 image = 1 batch (no split).
- Each batch takes ~1.5–2 min, well under RabbitMQ's 30-minute consumer ack timeout.

### Inside one batch
- Download the file to **temp disk with streams**, never into a Buffer.
- Process pages **one by one** (render → OCR → save → delete image).
  Parallelism comes from `prefetch`, not from inside the batch.
- Run OCR in a **child process** (Tesseract CLI) so the Node event loop and RabbitMQ heartbeats keep working.
- Chunk text → embed in **batches of ~100 chunks per API call**, retry `429` with backoff.
- Upsert vectors with **deterministic IDs** `fileId:page:chunkIndex`, so redelivery overwrites instead of duplicating.
- Delete temp files in `finally`.

### Ack / nack
| Outcome | Action |
|---|---|
| Success | update Mongo → `ack` |
| Known bad input (corrupt PDF, invalid JSON) | `nack(msg, false, false)` → DLQ immediately |
| Transient error (network, 5xx, rate limit after retries) | `nack(msg, false, true)` → requeue; quorum `x-delivery-limit` stops endless loops |
| Worker crash / OOM | RabbitMQ requeues unacked msgs automatically |

### Prefetch (per channel, one channel per queue)
| Queue | prefetch | Why |
|---|---|---|
| `file.processing.q` (splitter) | **1** | Split takes seconds |
| `ocr.batch.q` (OCR) | **`OCR_CONCURRENCY`** (default 6) | CPU-bound; ≈ cores − 2 |

```
total parallel OCR = OCR_CONCURRENCY × 1 page per batch × worker processes ≈ cores − 2
```

---

## 7. Sizing (single server)

### Estimates (to be validated, see §10)
| Item | Value |
|---|---|
| OCR (Tesseract, CPU, 300 DPI) | ~2 s / page |
| Peak RAM per running batch | ~0.5–1 GB |
| 1 batch (50 pages) | ~1.5–2 min |
| Embedding tokens per upload | ~2M → ~$0.04 (`text-embedding-3-small`) |
| Vectors per upload | ~8,000 × 1536 dims ≈ 50 MB |

### Recommended server
| | Minimum | **Recommended** |
|---|---|---|
| CPU / RAM | 4 vCPU / 8 GB | **8 vCPU / 16 GB** |
| `OCR_CONCURRENCY` | 3 | **6** |
| Disk | 20 GB SSD | 50 GB SSD |
| 1 file (1,000 pages) | ~13 min | **~7–8 min** |
| Full upload (4 files, 400 MB) | ~45 min | **~25–30 min** |

RabbitMQ, MongoDB, orchestrator and file-processor can all run on this one server.

### How `OCR_CONCURRENCY` is chosen
```
CPU limit = cores − 2                              → 8 − 2 = 6
RAM limit = (RAM − ~4 GB reserved) ÷ peak per job  → 12 ÷ 1 = 12
workers   = min(CPU limit, RAM limit, API limit)   → 6
```

---

## 8. Configuration (`.env`)

```
# orchestrator
PORT=3004
RABBITMQ_URL=amqp://guest:guest@localhost:5672
MAX_FILE_SIZE_MB=100
MAX_FILES_PER_UPLOAD=4

# file-processor
PORT=4000
RABBITMQ_URL=amqp://guest:guest@localhost:5672
MONGO_URL=mongodb://localhost:27017/file-pipeline
OCR_BATCH_SIZE=50
OCR_CONCURRENCY=6
TMP_DIR=/tmp/file-pipeline
```

---

## 9. Scaling beyond one upload

- More uploads at once → batches queue up in FIFO order; the file uploaded first finishes first.
- Time needed ≈ `(total batches × 2 min) ÷ total OCR workers`.
  Example: 50 files = 1,000 batches → ~5.5 h on 1 server; ~1 h needs ~34 workers.
- Scale out by running **the same file-processor on more servers**. RabbitMQ distributes
  automatically, with no code change.
- Alternative: cloud OCR (Textract / Document AI), ~$1.50 per 1,000 pages (~$6 per full upload).
- Monitor `ocr.batch.q` in the management UI: **ready** should fall steadily, **unacked** ≈ total workers × prefetch.

---

## 10. Roadmap

### Done
- [x] `docker-compose.yml`: RabbitMQ 3-management (MongoDB to run outside Docker)
- [x] orchestrator: `setupTopology`, publish to `files.exchange`, `/test-publish?fail&ttl&count`
- [x] `topology.ts`: exchange, DLX, DLQ, main queue (test settings)

### Phase 1: finish the DLQ flow
- [ ] file-processor: call `setupTopology`, consume `file.processing.q` (currently still on `file.uploaded.test` with `assertQueue` commented out)
- [ ] file-processor: `simulateFailure` → `nack(msg, false, false)` → DLQ
- [ ] Test: normal, `?fail=true`, `?ttl=5000` (no consumer), `?count=15`
- [ ] Delete old queues `file.uploaded`, `file.uploaded.test`

### Phase 2: production topology
- [ ] Remove `x-max-length`, switch to quorum queues + `x-delivery-limit: 3`
- [ ] Add `ocr.batch.q`, `ocr.batch.dlq`, bindings
- [ ] Move shared constants/types into one shared package (no duplicated `topology.ts`)

### Phase 3: storage + upload
- [ ] Object storage (Supabase / S3) + signed URLs
- [ ] `POST /upload` with limits (4 files, 100 MB each)
- [ ] MongoDB connection + `files` collection

### Phase 4: splitter
- [ ] Download (stream to disk) → page count (`pdfinfo`)
- [ ] Create parent record, publish `OcrBatchMessage`s, ack

### Phase 5: OCR worker
- [ ] Separate channel, `prefetch(OCR_CONCURRENCY)`
- [ ] `pdftoppm` per page → `tesseract` child process → save text
- [ ] Mark batch done (`$addToSet`), finalize parent status

### Phase 6: vector ingest
- [ ] Chunking strategy (size / overlap)
- [ ] Embedding provider + batching + 429 backoff
- [ ] Vector DB upsert with deterministic IDs

### Phase 7: validate + harden
- [ ] Benchmark 1 real 1,000-page file: s/page, peak RAM, then tune `OCR_CONCURRENCY` / batch size
- [ ] Graceful shutdown (stop consuming, finish in-flight batches)
- [ ] DLQ retry tool (move/republish failed batches)
- [ ] Progress endpoint `GET /files/:id` → `doneBatches / totalBatches`

---

## 11. Open decisions

| Decision | Options | Leaning |
|---|---|---|
| OCR engine | Tesseract (CPU) / PaddleOCR (GPU) / Textract / Document AI | Tesseract to start |
| Text-layer shortcut | Skip OCR for born-digital PDFs (`pdftotext`) | Yes, check per page |
| Embedding provider | OpenAI / Voyage / local model | API (cheap at this size) |
| Vector DB | MongoDB Atlas Vector Search / Qdrant / pgvector | Atlas (already using Mongo) |
| Object storage | Supabase / S3 | TBD |
| MongoDB hosting | Local install / Atlas | TBD |
