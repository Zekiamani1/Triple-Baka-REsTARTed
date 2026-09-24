import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";

const definition = protoLoader.loadSync(
  path.resolve("proto/hazard.proto"),
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  }
);

const proto =
  grpc.loadPackageDefinition(definition) as any;

const client =
  new proto.hazard.Aggregate(
    "localhost:50051",
    grpc.credentials.createInsecure()
  );

const now =
  Math.floor(Date.now() / 1000);

const hazard = {
  hazard_id: "hazard-test-001",
  source: "BMKG",
  source_ref_id: "bmkg-test-001",
  hazard_type: "SEISMIC",
  severity: "SIAGA",
  area_name: "Bandung",
  latitude: -6.9175,
  longitude: 107.6191,
  occurred_at: {
    seconds: now,
    nanos: 0,
  },
  ingested_at: {
    seconds: now,
    nanos: 0,
  },
  attributes: {},
};

client.aggregateEvent(
  hazard,

  (error: any, response: any) => {
    if (error) {
      console.error(error);
      return;
    }

    console.log("RPC SUCCESS");
    console.log(response);
  }
);