import { createHash, randomUUID } from "node:crypto";
import {
  MongoClient,
  MongoServerError,
  type Collection,
  type Document,
} from "mongodb";
import { receiverConfig } from "./config";

export interface StoredReceipt {
  delivery_id: string;
  receipt_id: string;
  received_at: string;
  duplicate: boolean;
}

interface ReceiptDocument extends Document {
  delivery_id: string;
  receipt_id: string;
  payload_sha256: string;
  received_at: string;
  event: Document;
}

export class IdempotencyConflictError extends Error {
  constructor(deliveryId: string) {
    super(`delivery_id '${deliveryId}' was already used with a different event payload`);
    this.name = "IdempotencyConflictError";
  }
}

export class ReceiptStore {
  private readonly client: MongoClient;
  private collection!: Collection<ReceiptDocument>;
  private connected = false;

  constructor() {
    this.client = new MongoClient(receiverConfig.mongodbUri, {
      appName: "field-receiver",
      retryWrites: true,
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 5000,
    });
  }

  async connect(): Promise<void> {
    await this.client.connect();
    const database = this.client.db(receiverConfig.database);
    this.collection = database.collection<ReceiptDocument>(receiverConfig.collection, {
      writeConcern: { w: "majority", j: true, wtimeoutMS: 5000 },
    });
    await this.collection.createIndex({ delivery_id: 1 }, { unique: true, name: "uniq_delivery_id" });
    await database.command({ ping: 1 });
    this.connected = true;
  }

  isConnected(): boolean {
    return this.connected;
  }

  async saveReceipt(deliveryId: string, event: Record<string, unknown>): Promise<StoredReceipt> {
    if (!this.connected) throw new Error("Receipt store is not connected");

    const payloadHash = createHash("sha256").update(canonicalJson(event)).digest("hex");
    const now = new Date().toISOString();
    const document: ReceiptDocument = {
      delivery_id: deliveryId,
      receipt_id: randomUUID(),
      payload_sha256: payloadHash,
      received_at: now,
      event: event as Document,
    };

    try {
      await this.collection.insertOne(document, {
        writeConcern: { w: "majority", j: true, wtimeoutMS: 5000 },
      });
      return {
        delivery_id: document.delivery_id,
        receipt_id: document.receipt_id,
        received_at: document.received_at,
        duplicate: false,
      };
    } catch (error) {
      if (!(error instanceof MongoServerError) || error.code !== 11000) throw error;

      const existing = await this.collection.findOne({ delivery_id: deliveryId });
      if (!existing) throw new Error("Duplicate receipt key reported, but existing receipt could not be read");
      if (existing.payload_sha256 !== payloadHash) throw new IdempotencyConflictError(deliveryId);

      return {
        delivery_id: existing.delivery_id,
        receipt_id: existing.receipt_id,
        received_at: existing.received_at,
        duplicate: true,
      };
    }
  }

  async close(): Promise<void> {
    this.connected = false;
    await this.client.close();
  }
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(sortRecursively(value));
}

function sortRecursively(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortRecursively);
  if (typeof value !== "object" || value === null) return value;
  const record = value as Record<string, unknown>;
  const sorted: Record<string, unknown> = {};
  for (const key of Object.keys(record).sort()) sorted[key] = sortRecursively(record[key]);
  return sorted;
}
