import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { MongoClient } from "mongodb";
import { hazardEvent } from "./types"

const packageDefinition = protoLoader.loadSync(path.resolve("proto/hazard.proto"), {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const mongoClient = new MongoClient(
  "mongodb://mongo-aggregate:27017"
);

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
    
        const result = await collection.insertOne(hazardEvent);

        callback(null, {})
    }
    catch (error) {
        console.error(error);

        callback({code: grpc.status.INTERNAL});
    }
}


async function main () {
    await mongoClient.connect();

    const db = mongoClient.db("bnpb");
    collection = db.collection("hazard_events");

    const server = new grpc.Server();

    server.addService(hazardPackage.Aggregate.service, {aggregateEvent} );

    server.bindAsync(
    "0.0.0.0:50051",
    grpc.ServerCredentials.createInsecure(),
        (error, port) => {
            if (error) {
            console.error(error);
            return;
            }

            console.log(`gRPC listening on ${port}`);
        }
    );
}

main();