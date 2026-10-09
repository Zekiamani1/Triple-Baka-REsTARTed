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

async function ingestHazard(hazardEvent: any): Promise<void> {
    hazardEvent = {
        ...hazardEvent,
        occurred_at: toDate(hazardEvent.occurred_at),
        ingested_at: toDate(hazardEvent.ingested_at),
    };
    const identity = {
        source: hazardEvent.source,
        source_ref_id: hazardEvent.source_ref_id,
    };

    let eventToPublish = hazardEvent;

    try {
        await collection.insertOne({ ...hazardEvent, published: false });
    } catch (error: any) {
        if (error?.code !== 11000) {
            throw error;
        }

        const existingEvent = await collection.findOne(identity);
        if (existingEvent?.published) {
            console.log(`Duplicate hazard event with source_ref_id ${hazardEvent.source_ref_id} ignored`);
            return;
        }

        if (!existingEvent) {
            throw error;
        }

        eventToPublish = withoutDeliveryState(existingEvent);
    }

    await publishHazard(eventToPublish);
    await collection.updateOne(identity, {
        $set: { published: true, published_at: new Date() },
    });

    console.log("HazardEvent processed", {
        source: eventToPublish.source,
        source_ref_id: eventToPublish.source_ref_id,
        hazard_id: eventToPublish.hazard_id,
    });
}

async function aggregateEvent (call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) {
    try {
        console.log(call.request);
        
        const hazardEvent = protoToHazard(call.request);

        await ingestHazard(hazardEvent);

        callback(null, {})
    }
    catch (error) {
        console.error(error);
        callback({code: grpc.status.INTERNAL});
    }
}

async function listHazards(call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) {
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

        const [hazards, total] = await Promise.all([
            collection.find(filter, {
                projection: { _id: 0, published: 0, published_at: 0 }
            })
            .sort({ ingested_at: -1 })
            .skip(offset)
            .limit(limit)
            .toArray(),
            collection.countDocuments(filter),
        ]);

        callback(null, {
            hazards: hazards.map((h: any) => ({
                ...h,
                occurred_at: toProtoTimestamp(h.occurred_at),
                ingested_at: toProtoTimestamp(h.ingested_at),
            })), total });
    } catch (error) {
        console.error(error);
        callback({code: grpc.status.INTERNAL});
    }
}

async function getHazard(call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) {
    try {
        const hazardId = call.request.hazard_id;

        if (!hazardId) {
            callback({code: grpc.status.INVALID_ARGUMENT, message: "hazard_id is required"});

            return;
        }

        const hazardEvent = await collection.findOne(
            { hazard_id: hazardId },
            { projection: { _id: 0, published: 0, published_at: 0 } },
        );

        if (!hazardEvent) {
            callback({code: grpc.status.NOT_FOUND, message: `Hazard event with hazard_id ${hazardId} not found`});
            return;
        }

        callback(null, {
            ...hazardEvent,
            occurred_at: toProtoTimestamp(hazardEvent.occurred_at),
            ingested_at: toProtoTimestamp(hazardEvent.ingested_at),
        });
    } catch (error) {
        console.error(error);
        callback({code: grpc.status.INTERNAL});
    }
}


async function main () {
    await mongoClient.connect();

    const db = mongoClient.db(mongo.dbName);
    collection = db.collection(mongo.collection);
    await collection.createIndex({ source: 1, source_ref_id: 1 }, { unique: true });
    await collection.createIndex({ hazard_id: 1 });

    await connectKafka();
    void runIngestConsumer(ingestHazard).catch((error) => {
        console.error("Kafka ingestion consumer failed", error);
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
            console.error(error);
            return;
            }

            console.log(`gRPC listening on ${port}`);
            console.log(`Consuming HazardEvents from Kafka topic ${kafka.inTopic}`);
            console.log(`Publishing HazardEvents to Kafka topic ${kafka.outTopic}`);
        }
    );
}

async function shutdown() {
  await disconnectKafka();
  await mongoClient.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
main().catch(async (error) => {
  console.error("Aggregate Service failed to start", error);
  await mongoClient.close().catch(() => undefined);
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
