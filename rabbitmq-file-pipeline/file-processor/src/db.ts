import { Collection, MongoClient } from "mongodb";

// Parent record per file (see plan.md §5). Only the fields this phase fills
// in; page/batch fields get added with the splitter.
export interface FileRecord {
  _id: string; // fileId
  fileName: string;
  mimeType: string;
  sizeBytes?: number;
  status: "processing" | "retrying" | "downloaded" | "failed";
  error?: string;
  createdAt: Date;
  updatedAt: Date;
}

let client: MongoClient | null = null;
let files: Collection<FileRecord> | null = null;

export async function connectMongo(url: string): Promise<void> {
  client = new MongoClient(url);
  await client.connect();
  // Database name comes from the URL path, e.g. mongodb://host/file-pipeline
  files = client.db().collection<FileRecord>("files");
  console.log(`[mongo] connected to ${client.db().databaseName}`);
}

export function filesCollection(): Collection<FileRecord> {
  if (!files) {
    throw new Error("Mongo not initialised — call connectMongo() first");
  }
  return files;
}

export async function closeMongo(): Promise<void> {
  await client?.close();
}
