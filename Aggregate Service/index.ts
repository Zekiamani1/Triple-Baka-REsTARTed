import {
  logError,
  logInfo,
  recordGrpcRequest,
  recordIngestEvent,
  recordMongoOp,
  SCOPE_NAME,
  shutdownTelemetry,
  SpanKind,
  SpanStatusCode,
  trace,
} from "./telemetry";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { MongoClient } from "mongodb";
import { kafka, mongo, service } from "./config";
import {
  connectKafka,
  disconnectKafka,
  publishHazard,
  runIngestConsumer,
} from "./kafkabridge";

const packageDefinition = protoLoader.loadSync(path.resolve("proto/hazard/hazard.proto"), {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: ["proto"],
});

const healthPackageDefinition = protoLoader.loadSync(
  path.resolve("proto/grpc/health/v1/health.proto"),
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  },
);

const proto = grpc.loadPackageDefinition(packageDefinition) as any;
const healthProto = grpc.loadPackageDefinition(healthPackageDefinition) as any;

const mongoClient = new MongoClient(mongo.uri);

let collection: any;

const hazardPackage = proto.hazard;
const healthPackage = healthProto.grpc.health.v1;

function protoToHazard(protoHazard: any) {
    return {
        hazard_id: protoHazard.hazard_id,
        source: protoHazard.source,
        source_ref_id: protoHazard.source_ref_id,
        hazard_type: protoHazard.hazard_type,
        severity: protoHazard.severity,
        area_name: protoHazard.area_name,
        latitude: protoHazard.latitude,
        longitude: protoHazard.longitude,
        occurred_at: toDate(protoHazard.occurred_at),
        ingested_at: toDate(protoHazard.ingested_at),
        attributes: protoHazard.attributes,
    }
}

function withoutDeliveryState(document: any): Record<string, unknown> {
    const { _id, published, published_at, ...hazardEvent } = document;
    return hazardEvent;
}

async function trackMongoOp<T = any>(operation: string, fn: () => Promise<T>): Promise<T> {
    const start = performance.now();
    try {
        const result = await fn();
        recordMongoOp(operation, "success", performance.now() - start);
        return result;
    } catch (error) {
        recordMongoOp(operation, "error", performance.now() - start);
        throw error;
    }
}

async function ingestHazard(hazardEvent: any): Promise<void> {
    const start = performance.now();
    const source = String(hazardEvent?.source ?? "UNKNOWN");
    const hazardType = String(hazardEvent?.hazard_type ?? "UNKNOWN");
    const severity = String(hazardEvent?.severity ?? "UNKNOWN");

    await trace.getTracer(SCOPE_NAME).startActiveSpan(
        "AggregateService.ingestHazard",
        {
            kind: SpanKind.INTERNAL,
            attributes: {
                "hazard.id": String(hazardEvent?.hazard_id ?? ""),
                "hazard.source": source,
                "hazard.source_ref_id": String(hazardEvent?.source_ref_id ?? ""),
                "hazard.type": hazardType,
                "hazard.severity": severity,
            },
        },
        async (span) => {
            try {
                hazardEvent = {
                    ...hazardEvent,
                    occurred_at: toDate(hazardEvent.occurred_at),
                    ingested_at: toDate(hazardEvent.ingested_at),
                    attributes: toStruct(hazardEvent.attributes),
                };
                const identity = {
                    source: hazardEvent.source,
                    source_ref_id: hazardEvent.source_ref_id,
                };

                let eventToPublish = hazardEvent;

                try {
                    await trackMongoOp("insertOne", () =>
                        collection.insertOne({ ...hazardEvent, published: false }),
                    );
                } catch (error: any) {
                    if (error?.code !== 11000) {
                        throw error;
                    }

                    const existingEvent = await trackMongoOp("findOne", () =>
                        collection.findOne(identity),
                    );
                    if (existingEvent?.published) {
                        logInfo("duplicate hazard event ignored", {
                            source: hazardEvent.source,
                            source_ref_id: hazardEvent.source_ref_id,
                        });
                        span.setAttribute("hazard.duplicate", true);
                        recordIngestEvent(source, hazardType, severity, "duplicate", performance.now() - start);
                        span.setStatus({ code: SpanStatusCode.OK });
                        return;
                    }

                    if (!existingEvent) {
                        throw error;
                    }

                    eventToPublish = withoutDeliveryState(existingEvent);
                }

                await publishHazard(eventToPublish);
                await trackMongoOp("updateOne", () =>
                    collection.updateOne(identity, {
                        $set: { published: true, published_at: new Date() },
                    }),
                );

                recordIngestEvent(source, hazardType, severity, "processed", performance.now() - start);
                span.setStatus({ code: SpanStatusCode.OK });

                logInfo("HazardEvent processed", {
                    source: eventToPublish.source,
                    source_ref_id: eventToPublish.source_ref_id,
                    hazard_id: eventToPublish.hazard_id,
                });
            } catch (error: any) {
                recordIngestEvent(source, hazardType, severity, "error", performance.now() - start);
                logError("failed to ingest hazard event", {
                    source,
                    hazard_id: hazardEvent?.hazard_id,
                    err: error instanceof Error ? error.message : String(error),
                });
                span.recordException(error);
                span.setStatus({
                    code: SpanStatusCode.ERROR,
                    message: error instanceof Error ? error.message : String(error),
                });
                throw error;
            } finally {
                span.end();
            }
        },
    );
}

async function aggregateEvent(call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) {
    const start = performance.now();
    await trace.getTracer(SCOPE_NAME).startActiveSpan(
        "AggregateService.AggregateEvent",
        {
            kind: SpanKind.SERVER,
            attributes: {
                "rpc.system": "grpc",
                "rpc.service": "hazard.Aggregate",
                "rpc.method": "AggregateEvent",
            },
        },
        async (span) => {
            try {
                const hazardEvent = protoToHazard(call.request);
                span.setAttributes({
                    "hazard.id": String(hazardEvent.hazard_id ?? ""),
                    "hazard.source": String(hazardEvent.source ?? ""),
                    "hazard.type": String(hazardEvent.hazard_type ?? ""),
                    "hazard.severity": String(hazardEvent.severity ?? ""),
                });

                await ingestHazard(hazardEvent);

                const durationMs = performance.now() - start;
                recordGrpcRequest("AggregateEvent", "OK", durationMs);
                span.setStatus({ code: SpanStatusCode.OK });
                callback(null, {});
            } catch (error: any) {
                const durationMs = performance.now() - start;
                recordGrpcRequest("AggregateEvent", "INTERNAL", durationMs);
                logError("AggregateEvent RPC failed", {
                    err: error instanceof Error ? error.message : String(error),
                });
                span.recordException(error);
                span.setStatus({
                    code: SpanStatusCode.ERROR,
                    message: error instanceof Error ? error.message : String(error),
                });
                callback({ code: grpc.status.INTERNAL });
            } finally {
                span.end();
            }
        },
    );
}

async function listHazards(call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) {
    const start = performance.now();
    await trace.getTracer(SCOPE_NAME).startActiveSpan(
        "AggregateService.ListHazards",
        {
            kind: SpanKind.SERVER,
            attributes: {
                "rpc.system": "grpc",
                "rpc.service": "hazard.Aggregate",
                "rpc.method": "ListHazards",
            },
        },
        async (span) => {
            try {
                const req = call.request;
                const filter: Record<string, unknown> = {};

                if (req.source && req.source.trim() !== "SOURCE_UNSPECIFIED") {
                    filter.source = req.source;
                }

                if (req.hazard_type && req.hazard_type.trim() !== "HAZARD_TYPE_UNSPECIFIED") {
                    filter.hazard_type = req.hazard_type;
                }

                if (req.severity && req.severity.trim() !== "SEVERITY_UNSPECIFIED") {
                    filter.severity = req.severity;
                }

                const limit = req.limit && req.limit > 0 ? (req.limit > 100 ? 100 : req.limit) : 10;
                const offset = req.offset && req.offset >= 0 ? req.offset : 0;

                span.setAttributes({
                    "query.filter.source": String(filter.source ?? "ALL"),
                    "query.filter.hazard_type": String(filter.hazard_type ?? "ALL"),
                    "query.filter.severity": String(filter.severity ?? "ALL"),
                    "query.limit": limit,
                    "query.offset": offset,
                });

                const [hazards, total] = await Promise.all([
                    trackMongoOp("find", () =>
                        collection.find(filter, {
                            projection: { _id: 0, published: 0, published_at: 0 }
                        })
                        .sort({ ingested_at: -1 })
                        .skip(offset)
                        .limit(limit)
                        .toArray()
                    ),
                    trackMongoOp("countDocuments", () =>
                        collection.countDocuments(filter)
                    ),
                ]);

                span.setAttributes({
                    "query.result_count": hazards.length,
                    "query.total_count": total,
                });

                const durationMs = performance.now() - start;
                recordGrpcRequest("ListHazards", "OK", durationMs);
                span.setStatus({ code: SpanStatusCode.OK });

                callback(null, {
                    hazards: hazards.map((h: any) => ({
                        ...h,
                        occurred_at: toProtoTimestamp(h.occurred_at),
                        ingested_at: toProtoTimestamp(h.ingested_at),
                        attributes: toStruct(h.attributes),
                    })),
                    total,
                });
            } catch (error: any) {
                const durationMs = performance.now() - start;
                recordGrpcRequest("ListHazards", "INTERNAL", durationMs);
                logError("ListHazards RPC failed", {
                    err: error instanceof Error ? error.message : String(error),
                });
                span.recordException(error);
                span.setStatus({
                    code: SpanStatusCode.ERROR,
                    message: error instanceof Error ? error.message : String(error),
                });
                callback({ code: grpc.status.INTERNAL });
            } finally {
                span.end();
            }
        },
    );
}

async function getHazard(call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) {
    const start = performance.now();
    await trace.getTracer(SCOPE_NAME).startActiveSpan(
        "AggregateService.GetHazard",
        {
            kind: SpanKind.SERVER,
            attributes: {
                "rpc.system": "grpc",
                "rpc.service": "hazard.Aggregate",
                "rpc.method": "GetHazard",
            },
        },
        async (span) => {
            try {
                const hazardId = call.request.hazard_id;
                span.setAttribute("hazard.id", String(hazardId ?? ""));

                if (!hazardId) {
                    const durationMs = performance.now() - start;
                    recordGrpcRequest("GetHazard", "INVALID_ARGUMENT", durationMs);
                    span.setStatus({ code: SpanStatusCode.ERROR, message: "hazard_id is required" });
                    callback({ code: grpc.status.INVALID_ARGUMENT, message: "hazard_id is required" });
                    return;
                }

                const hazardEvent = await trackMongoOp("findOne", () =>
                    collection.findOne(
                        { hazard_id: hazardId },
                        { projection: { _id: 0, published: 0, published_at: 0 } },
                    ),
                );

                if (!hazardEvent) {
                    const durationMs = performance.now() - start;
                    recordGrpcRequest("GetHazard", "NOT_FOUND", durationMs);
                    span.setStatus({ code: SpanStatusCode.ERROR, message: "not found" });
                    callback({
                        code: grpc.status.NOT_FOUND,
                        message: `Hazard event with hazard_id ${hazardId} not found`,
                    });
                    return;
                }

                const durationMs = performance.now() - start;
                recordGrpcRequest("GetHazard", "OK", durationMs);
                span.setStatus({ code: SpanStatusCode.OK });

                callback(null, {
                    ...hazardEvent,
                    occurred_at: toProtoTimestamp(hazardEvent.occurred_at),
                    ingested_at: toProtoTimestamp(hazardEvent.ingested_at),
                    attributes: toStruct(hazardEvent.attributes),
                });
            } catch (error: any) {
                const durationMs = performance.now() - start;
                recordGrpcRequest("GetHazard", "INTERNAL", durationMs);
                logError("GetHazard RPC failed", {
                    err: error instanceof Error ? error.message : String(error),
                });
                span.recordException(error);
                span.setStatus({
                    code: SpanStatusCode.ERROR,
                    message: error instanceof Error ? error.message : String(error),
                });
                callback({ code: grpc.status.INTERNAL });
            } finally {
                span.end();
            }
        },
    );
}


async function main () {
    await mongoClient.connect();

    const db = mongoClient.db(mongo.dbName);
    collection = db.collection(mongo.collection);
    await collection.createIndex({ source: 1, source_ref_id: 1 }, { unique: true });
    await collection.createIndex({ hazard_id: 1 });

    await connectKafka();
    void runIngestConsumer(ingestHazard).catch((error) => {
        logError("Kafka ingestion consumer failed", {
            err: error instanceof Error ? error.message : String(error),
        });
        process.exit(1);
    });

    const server = new grpc.Server();

    server.addService(hazardPackage.Aggregate.service, {
        aggregateEvent,
        getHazard,
        listHazards,
    } );
    server.addService(healthPackage.Health.service, {
        Check: (_call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) => {
            callback(null, { status: "SERVING" });
        },
        Watch: (call: grpc.ServerWritableStream<any, any>) => {
            call.write({ status: "SERVING" });
        },
    });

    server.bindAsync(
    `0.0.0.0:${service.port}`,
    grpc.ServerCredentials.createInsecure(),
        (error, port) => {
            if (error) {
                logError("failed to bind gRPC server", { err: error.message });
                return;
            }

            logInfo("Aggregate Service started", {
                grpc_port: port,
                kafka_in_topic: kafka.inTopic,
                kafka_out_topic: kafka.outTopic,
            });
        }
    );
}

async function shutdown() {
  await disconnectKafka();
  await mongoClient.close();
  await shutdownTelemetry();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
main().catch(async (error) => {
  logError("Aggregate Service failed to start", {
    err: error instanceof Error ? error.message : String(error),
  });
  await mongoClient.close().catch(() => undefined);
  await shutdownTelemetry();
  process.exit(1);
});

// Accepts a Date, ISO string, epoch millis, or an already-proto {seconds, nanos}
// object, and returns a native JS Date — the one shape we store in Mongo.
function toDate(value: unknown): Date {
  if (value instanceof Date) return value;
  if (value && typeof value === "object" && "seconds" in (value as any)) {
    const v = value as { seconds: number | string; nanos?: number | string };
    return new Date(Number(v.seconds) * 1000 + Number(v.nanos ?? 0) / 1e6);
  }
  const date = new Date(value as string | number);
  if (isNaN(date.getTime())) {
    throw new Error(`Invalid timestamp value: ${JSON.stringify(value)}`);
  }
  return date;
}

// Converts a stored Date back into the {seconds, nanos} shape proto-loader
// needs to serialize a google.protobuf.Timestamp field on the way out.
function toProtoTimestamp(date: Date): { seconds: number; nanos: number } {
  const ms = date.getTime();
  return { seconds: Math.floor(ms / 1000), nanos: (ms % 1000) * 1e6 };
}

// plain JS -> protobuf Struct (for responses)
function toProtoValue(v: unknown): any {
  if (v === null || v === undefined) return { nullValue: "NULL_VALUE" };
  if (v instanceof Date) return { stringValue: v.toISOString() };
  if (typeof v === "number") return { numberValue: v };
  if (typeof v === "string") return { stringValue: v };
  if (typeof v === "boolean") return { boolValue: v };
  if (Array.isArray(v)) return { listValue: { values: v.map(toProtoValue) } };
  if (typeof v === "object") return { structValue: toStruct(v as Record<string, unknown>) };
  return { stringValue: String(v) };
}

function toStruct(obj: Record<string, unknown> | undefined): any {
  return {
    fields: Object.fromEntries(
      Object.entries(obj ?? {}).map(([k, v]) => [k, toProtoValue(v)]),
    ),
  };
}

// protobuf Struct -> plain JS (for requests)
function fromProtoValue(v: any): unknown {
  switch (v?.kind) {
    case "numberValue": return v.numberValue;
    case "stringValue": return v.stringValue;
    case "boolValue": return v.boolValue;
    case "structValue": return fromStruct(v.structValue);
    case "listValue": return (v.listValue?.values ?? []).map(fromProtoValue);
    default: return null;
  }
}

function fromStruct(s: any): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(s?.fields ?? {}).map(([k, v]) => [k, fromProtoValue(v)]),
  );
}
