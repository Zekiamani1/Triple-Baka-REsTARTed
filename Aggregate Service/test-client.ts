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

const sourceRefId = `bmkg-test-${Date.now()}`;

const firstHazard = {
  hazard_id: `hazard-first-${Date.now()}`,
  source: "BMKG",
  source_ref_id: sourceRefId,
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

const duplicateHazard = {
  ...firstHazard,
  hazard_id: `hazard-duplicate-${Date.now()}`,
};

function aggregateEvent(hazard: typeof firstHazard): Promise<void> {
  return new Promise((resolve, reject) => {
    client.aggregateEvent(hazard, (error: grpc.ServiceError | null) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}

function getHazard(hazardId: string): Promise<any> {
  return new Promise((resolve, reject) => {
    client.getHazard(
      { hazard_id: hazardId },
      (error: grpc.ServiceError | null, response: any) => {
        if (error) {
          reject(error);
          return;
        }

        resolve(response);
      },
    );
  });
}

function listHazards(): Promise<any> {
  return new Promise((resolve, reject) => {
    client.listHazards(
      { source: "BMKG", limit: 100, offset: 0 },
      (error: grpc.ServiceError | null, response: any) => {
        if (error) {
          reject(error);
          return;
        }

        resolve(response);
      },
    );
  });
}

async function main(): Promise<void> {
  console.log("Sending the original event...");
  await aggregateEvent(firstHazard);

  console.log("Sending the same source event with a different hazard_id...");
  await aggregateEvent(duplicateHazard);

  const storedHazard = await getHazard(firstHazard.hazard_id);
  console.log("Stored event:", storedHazard);

  try {
    await getHazard(duplicateHazard.hazard_id);
    throw new Error("Idempotency failed: the duplicate hazard_id was stored");
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error)) {
      throw error;
    }

    if ((error as grpc.ServiceError).code !== grpc.status.NOT_FOUND) {
      throw error;
    }
  }

  const response = await listHazards();
  console.log(`ListHazards returned ${response.total} BMKG event(s)`);
  console.dir(response.hazards, { depth: null });
  console.log("IDEMPOTENCY TEST PASSED");
}

main()
  .catch((error) => {
    console.error("TEST FAILED", error);
    process.exitCode = 1;
  })
  .finally(() => {
    client.close();
  });
