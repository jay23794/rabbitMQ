// events/ may import from core/; core/ must never import from events/.

import { defineEvent } from "../core/event";

export interface FileUploadedData {
  fileId: string;
  fileName: string;
  mimeType: string;
  signedUrl: string;
  uploadedAt: string; // ISO 8601 timestamp
  simulateFailure?: boolean; // test only: tells the consumer to fail on purpose
}

export interface FileProcessedData {
  fileId: string;
  status: "success" | "failed";
  size?: number; // bytes; set when status is "success"
  error?: string; // set when status is "failed"
}

export const FileUploaded = defineEvent<FileUploadedData>({
  exchange: "file",
  routingKey: "file.uploaded",
});

export const FileProcessed = defineEvent<FileProcessedData>({
  exchange: "file",
  routingKey: "file.processed",
});
