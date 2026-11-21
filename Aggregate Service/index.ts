import {
  initTelemetry,
  logError,
  logInfo,
  recordGrpcRequest,
  recordIngestEvent,
  recordMongoOp,
  SCOPE_NAME,
  SpanKind,
  SpanStatusCode,
  trace,
} from "./telemetry";

const shutdownTelemetry = initTelemetry();
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { hostname } from "node:os";
import { createHash } from "node:crypto";
import { MongoClient } from "mongodb";
import { kafka, mongo, service } from "./config";
import { normalizeAttributes, toStruct } from "./attributes";
import {
  connectKafka,
  disconnectKafka,
  ensureProducerConnected,
  isKafkaReady,
  publishHazard,
  runIngestConsumer,
  stopIngestConsumer,
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

const MONGO_QUERY_MAX_TIME_MS = 6000;
const MONGO_HEALTH_INTERVAL_MS = 5000;
const MONGO_HEALTH_PING_TIMEOUT_MS = 3000;
const MONGO_HEALTH_FAILURE_THRESHOLD = 3;

const mongoClient = new MongoClient(mongo.uri, {
    retryWrites: true,
    writeConcern: { w: "majority", journal: true, wtimeoutMS: 10000 },
    readConcern: { level: "local" },
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 8000,
});

let collection: any;
const server = new grpc.Server();
let ready = false;
let stopping = false;
let mongoHealthy = false;
let mongoConsecutiveFailures = 0;
let mongoHealthTimer: NodeJS.Timeout | null = null;
const healthWatchers = new Set<grpc.ServerWritableStream<any, any>>();

function startMongoHealthMonitor(db: any): void {
    mongoHealthy = true;
    mongoConsecutiveFailures = 0;
    let inFlight = false;
    mongoHealthTimer = setInterval(async () => {
        if (inFlight || stopping) return;
        inFlight = true;
        try {
            await db.command({ ping: 1 }, { maxTimeMS: MONGO_HEALTH_PING_TIMEOUT_MS });
            mongoHealthy = true;
            mongoConsecutiveFailures = 0;
        } catch {
            mongoConsecutiveFailures += 1;
            if (mongoConsecutiveFailures >= MONGO_HEALTH_FAILURE_THRESHOLD) {
                mongoHealthy = false;
            }
        } finally {
            inFlight = false;
        }
    }, MONGO_HEALTH_INTERVAL_MS);
    mongoHealthTimer.unref();
}

function healthStatus(): string {
    const kafkaOk = service.mode === "api" || isKafkaReady();
    return ready && !stopping && mongoHealthy && kafkaOk ? "SERVING" : "NOT_SERVING";
}

function currentHealthStatus(): string {
    return healthStatus();
}

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
    const { _id, partition_key, published, published_at, ...hazardEvent } = document;
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
                    attributes: normalizeAttributes(hazardEvent.attributes),
                };
                if (typeof hazardEvent.source !== "string" || !hazardEvent.source.trim() ||
                    typeof hazardEvent.source_ref_id !== "string" || !hazardEvent.source_ref_id.trim()) {
                    throw new Error("source and source_ref_id must be non-empty strings");
                }
                const identity = {
                    // Matches the pre-split hash ranges configured by init-cluster.js.
                    partition_key: createHash("sha256")
                        .update(JSON.stringify([hazardEvent.source, hazardEvent.source_ref_id]))
                        .digest("hex"),
                    source: hazardEvent.source,
                    source_ref_id: hazardEvent.source_ref_id,
                };

                let eventToPublish = hazardEvent;

                try {
                    await trackMongoOp("insertOne", () =>
                        collection.insertOne({ ...hazardEvent, ...identity, published: false }),
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

                await ensureProducerConnected();
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

const LIST_CACHE_TTL_MS = 2000;
const LIST_CACHE_MAX_ENTRIES = 500;
const listHazardsCache = new Map<string, { expiresAt: number; payload: { hazards: any[]; total: number } }>();
const listHazardsInFlight = new Map<string, Promise<{ hazards: any[]; total: number }>>();

async function fetchListHazardsPayload(
    filter: Record<string, unknown>,
    limit: number,
    offset: number,
    span: any,
): Promise<{ hazards: any[]; total: number }> {
    const isUnfiltered = Object.keys(filter).length === 0;
    const [hazards, total] = await Promise.all([
        trackMongoOp("find", () =>
            collection.find(filter, {
                projection: { _id: 0, partition_key: 0, published: 0, published_at: 0 }
            })
            .sort({ ingested_at: -1, source: 1, source_ref_id: 1 })
            .skip(offset)
            .limit(limit)
            .maxTimeMS(MONGO_QUERY_MAX_TIME_MS)
            .toArray()
        ),
        trackMongoOp(isUnfiltered ? "estimatedDocumentCount" : "countDocuments", () =>
            isUnfiltered
                ? collection.estimatedDocumentCount({ maxTimeMS: MONGO_QUERY_MAX_TIME_MS })
                : collection.countDocuments(filter, { maxTimeMS: MONGO_QUERY_MAX_TIME_MS })
        ),
    ]);

    span.setAttributes({
        "query.result_count": hazards.length,
        "query.total_count": total,
    });

    return {
        hazards: hazards.map((h: any) => ({
            ...h,
            occurred_at: toProtoTimestamp(h.occurred_at),
            ingested_at: toProtoTimestamp(h.ingested_at),
            attributes: toStruct(normalizeAttributes(h.attributes)),
        })),
        total,
    };
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

                const cacheKey = `${filter.source ?? ""}|${filter.hazard_type ?? ""}|${filter.severity ?? ""}|${limit}|${offset}`;
                const now = Date.now();
                const cached = listHazardsCache.get(cacheKey);
                let payload: { hazards: any[]; total: number };

                if (cached && cached.expiresAt > now) {
                    span.setAttribute("query.cache_hit", true);
                    payload = cached.payload;
                } else {
                    span.setAttribute("query.cache_hit", false);
                    let pending = listHazardsInFlight.get(cacheKey);
                    if (!pending) {
                        pending = fetchListHazardsPayload(filter, limit, offset, span)
                            .then((res) => {
                                if (listHazardsCache.size >= LIST_CACHE_MAX_ENTRIES) {
                                    listHazardsCache.clear();
                                }
                                listHazardsCache.set(cacheKey, {
                                    expiresAt: Date.now() + LIST_CACHE_TTL_MS,
                                    payload: res,
                                });
                                return res;
                            })
                            .finally(() => {
                                listHazardsInFlight.delete(cacheKey);
                            });
                        listHazardsInFlight.set(cacheKey, pending);
                    }
                    payload = await pending;
                }

                const durationMs = performance.now() - start;
                recordGrpcRequest("ListHazards", "OK", durationMs);
                span.setStatus({ code: SpanStatusCode.OK });

                callback(null, payload);
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
                        {
                            projection: { _id: 0, partition_key: 0, published: 0, published_at: 0 },
                            maxTimeMS: MONGO_QUERY_MAX_TIME_MS,
                        },
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
                    attributes: toStruct(normalizeAttributes(hazardEvent.attributes)),
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
    await collection.createIndex({ partition_key: 1, source: 1, source_ref_id: 1 }, { unique: true });
    await collection.createIndex({ hazard_id: 1 });
    startMongoHealthMonitor(db);

    const runIngester = service.mode === "ingester" || service.mode === "all";
    if (runIngester) {
        await connectKafka();
        void runIngestConsumer(ingestHazard).catch((error) => {
            logError("Kafka ingestion consumer failed", {
                err: error instanceof Error ? error.message : String(error),
            });
            process.exit(1);
        });
    }

    server.addService(hazardPackage.Aggregate.service, {
        aggregateEvent,
        getHazard,
        listHazards,
    } );
    server.addService(healthPackage.Health.service, {
        Check: (call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) => {
            if (call.request.service && call.request.service !== "hazard.Aggregate") {
                callback({ code: grpc.status.NOT_FOUND, message: "Unknown service" });
                return;
            }
            callback(null, { status: currentHealthStatus() });
        },
        Watch: (call: grpc.ServerWritableStream<any, any>) => {
            if (call.request.service && call.request.service !== "hazard.Aggregate") {
                call.write({ status: "SERVICE_UNKNOWN" });
                call.end();
                return;
            }
            healthWatchers.add(call);
            let previous = "";
            const publish = () => {
                const status = currentHealthStatus();
                if (healthWatchers.has(call) && status !== previous) {
                    previous = status;
                    call.write({ status });
                }
            };
            publish();
            const timer = setInterval(() => { publish(); }, 2000);
            const cleanup = () => {
                clearInterval(timer);
                healthWatchers.delete(call);
            };
            call.on("cancelled", cleanup);
            call.on("close", cleanup);
        },
    });

    server.bindAsync(
    `0.0.0.0:${service.port}`,
    grpc.ServerCredentials.createInsecure(),
        (error, port) => {
            if (error) {
                logError("failed to bind gRPC server", { err: error.message });
                void shutdown(1);
                return;
            }

            ready = true;
            logInfo("Aggregate Service started", {
                instance_id: hostname(),
                mode: service.mode,
                grpc_port: port,
                kafka_in_topic: runIngester ? kafka.inTopic : undefined,
                kafka_out_topic: kafka.outTopic,
            });
        }
    );
}

async function shutdown(exitCode = 0) {
  if (stopping) return;
  stopping = true;
  ready = false;
  if (mongoHealthTimer) {
    clearInterval(mongoHealthTimer);
    mongoHealthTimer = null;
  }
  logInfo("Aggregate Service draining", { instance_id: hostname() });
  for (const call of healthWatchers) {
    call.write({ status: "NOT_SERVING" });
    call.end();
  }
  healthWatchers.clear();
  const deadline = setTimeout(() => {
    logError("Aggregate shutdown deadline exceeded", { instance_id: hostname() });
    server.forceShutdown();
    process.exit(1);
  }, 35000);
  deadline.unref();
  try {
    // Allow Envoy's active health check to remove this instance first.
    await new Promise(resolve => setTimeout(resolve, 5000));
    await Promise.all([
      stopIngestConsumer(),
      new Promise<void>((resolve, reject) => {
        server.tryShutdown(error => error ? reject(error) : resolve());
      }),
    ]);
    // Producer and storage stay available until in-flight work has finished.
    await disconnectKafka();
    await mongoClient.close();
    await shutdownTelemetry();
    clearTimeout(deadline);
    process.exit(exitCode);
  } catch (error) {
    logError("Aggregate shutdown failed", { err: String(error) });
    server.forceShutdown();
    process.exit(1);
  }
}

process.on("SIGINT", () => { void shutdown(); });
process.on("SIGTERM", () => { void shutdown(); });
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
