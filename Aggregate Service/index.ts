import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { MongoClient } from "mongodb";
import { kafka, mongo, service } from "./config";
import {
  connectKafka,
  disconnectKafka,
  publishHazard,
} from "./kafkabridge";

const packageDefinition = protoLoader.loadSync(path.resolve("proto/hazard.proto"), {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const mongoClient = new MongoClient(mongo.uri);

let collection: any;

const hazardPackage = proto.hazard;

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
        occurred_at: protoHazard.occurred_at,
        ingested_at: protoHazard.ingested_at,
        attributes: protoHazard.attributes,
    }
}

async function aggregateEvent (call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) {
    try {
        console.log(call.request);
        
        const hazardEvent = protoToHazard(call.request);
    
        // MongoDB adds _id to the object passed to insertOne. Store a copy so
        // the canonical Kafka event stays independent of persistence details.
        await collection.insertOne({ ...hazardEvent });

        await publishHazard(hazardEvent);

        callback(null, {})
    }
    catch (error) {
        console.error(error);

        callback({code: grpc.status.INTERNAL});
    }
}


async function main () {
    await mongoClient.connect();
    await connectKafka();

    const db = mongoClient.db(mongo.dbName);
    collection = db.collection(mongo.collection);

    const server = new grpc.Server();

    server.addService(hazardPackage.Aggregate.service, {aggregateEvent} );

    server.bindAsync(
    `0.0.0.0:${service.port}`,
    grpc.ServerCredentials.createInsecure(),
        (error, port) => {
            if (error) {
            console.error(error);
            return;
            }

            console.log(`gRPC listening on ${port}`);
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
