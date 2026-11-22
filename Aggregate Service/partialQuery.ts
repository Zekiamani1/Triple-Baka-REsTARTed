import { randomUUID } from "node:crypto";
import { MongoClient, type Collection, type Db } from "mongodb";

export interface PartialResult<T> {
  data: T;
  partial: boolean;
  unavailable_units: string[];
}

type QueryOptions = {
  allowPartialResults: boolean;
  maxTimeMS: number;
  comment: string;
};

type ShardDocument = { _id: string; host: string };

const CONNECTIVITY_ERROR_NAMES = new Set([
  "MongoNetworkError",
  "MongoNetworkTimeoutError",
  "MongoServerSelectionError",
  "MongoOperationTimeoutError",
  "MongoTopologyClosedError",
]);

const CONNECTIVITY_ERROR_CODES = new Set([6, 7, 89, 91, 189, 9001, 11600, 11602]);

function isConnectivityError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const value = error as { name?: string; code?: number; errorLabels?: string[] };
  return CONNECTIVITY_ERROR_NAMES.has(value.name ?? "") ||
    (typeof value.code === "number" && CONNECTIVITY_ERROR_CODES.has(value.code)) ||
    Boolean(value.errorLabels?.includes("NetworkError"));
}

function cursorIdString(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  const id = String(value);
  return id === "0" ? undefined : id;
}

async function runWithPartialTracking<T>(
  mongoClient: MongoClient,
  queryId: string,
  run: (opts: QueryOptions) => Promise<T>,
): Promise<{ data: T; partial: boolean }> {
  let partial = false;
  const requestOwners = new Map<number, { owner: string; cursorId?: string }>();
  const cursorOwners = new Map<string, string>();

  const onCommandStarted = (event: any): void => {
    if (event.commandName === "find" && event.command?.comment === queryId) {
      requestOwners.set(event.requestId, { owner: queryId });
      return;
    }

    if (event.commandName === "getMore") {
      const owner = cursorOwners.get(String(event.command?.getMore));
      if (owner) requestOwners.set(event.requestId, { owner, cursorId: String(event.command?.getMore) });
    }
  };

  const onCommandSucceeded = (event: any): void => {
    const request = requestOwners.get(event.requestId);
    if (!request) return;

    const cursor = event.reply?.cursor;
    if (cursor?.partialResultsReturned === true) partial = true;

    if (request.cursorId) cursorOwners.delete(request.cursorId);

    const nextCursorId = cursorIdString(cursor?.id);
    if (nextCursorId) cursorOwners.set(nextCursorId, request.owner);
    requestOwners.delete(event.requestId);
  };

  const onCommandFailed = (event: any): void => {
    requestOwners.delete(event.requestId);
  };

  mongoClient.on("commandStarted", onCommandStarted);
  mongoClient.on("commandSucceeded", onCommandSucceeded);
  mongoClient.on("commandFailed", onCommandFailed);

  try {
    const data = await run({ allowPartialResults: true, maxTimeMS: 2500, comment: queryId });
    return { data, partial };
  } finally {
    mongoClient.off("commandStarted", onCommandStarted);
    mongoClient.off("commandSucceeded", onCommandSucceeded);
    mongoClient.off("commandFailed", onCommandFailed);
  }
}

async function listShards(configDb: Db): Promise<ShardDocument[]> {
  return configDb
    .collection<ShardDocument>("shards")
    .find({}, { maxTimeMS: 1000 })
    .toArray();
}

async function isShardAvailable(mongoClient: MongoClient, shard: ShardDocument): Promise<boolean> {
  const slash = shard.host.indexOf("/");
  if (slash <= 0 || slash === shard.host.length - 1) return false;

  const replicaSet = shard.host.slice(0, slash);
  const seeds = shard.host.slice(slash + 1);
  const credentials = mongoClient.options.credentials;
  const probeClient = new MongoClient(
    `mongodb://${seeds}/?replicaSet=${encodeURIComponent(replicaSet)}`,
    {
      auth: credentials
        ? { username: credentials.username, password: credentials.password }
        : undefined,
      authSource: credentials?.source,
      authMechanism: credentials?.mechanism,
      authMechanismProperties: credentials?.mechanismProperties,
      tls: mongoClient.options.tls,
      tlsCAFile: mongoClient.options.tlsCAFile,
      tlsCertificateKeyFile: mongoClient.options.tlsCertificateKeyFile,
      tlsCertificateKeyFilePassword: (mongoClient.options as { tlsCertificateKeyFilePassword?: string }).tlsCertificateKeyFilePassword,
      tlsAllowInvalidCertificates: mongoClient.options.tlsAllowInvalidCertificates,
      tlsAllowInvalidHostnames: mongoClient.options.tlsAllowInvalidHostnames,
      tlsInsecure: mongoClient.options.tlsInsecure,
      readPreference: "secondaryPreferred",
      serverSelectionTimeoutMS: 1000,
      connectTimeoutMS: 1000,
      socketTimeoutMS: 1500,
    },
  );

  try {
    await probeClient.db("admin").command({ ping: 1 });
    return true;
  } catch {
    return false;
  } finally {
    await probeClient.close().catch(() => undefined);
  }
}

async function getUnavailableShards(
  mongoClient: MongoClient,
): Promise<{ shardIds: string[]; unavailable: string[] } | null> {
  try {
    const shards = await listShards(mongoClient.db("config"));
    if (shards.length === 0) return null;

    const availability = await Promise.all(
      shards.map(async (shard) => ({
        id: shard._id,
        available: await isShardAvailable(mongoClient, shard),
      })),
    );

    return {
      shardIds: availability.map(({ id }) => id),
      unavailable: availability.filter(({ available }) => !available).map(({ id }) => id),
    };
  } catch {
    return null;
  }
}

function explicitUnavailableUnits(unavailable: string[]): string[] {
  // A shard may recover between the partial response and the health probe.
  // Keep the incomplete marker explicit even when the exact unit is unknown.
  return unavailable.length > 0 ? unavailable : ["unknown"];
}

export async function queryWithPartialFallback<T>(
  mongoClient: MongoClient,
  dbName: string,
  colName: string,
  runOnCollection: (col: Collection, opts: QueryOptions) => Promise<T>,
  emptyResult: () => T = () => [] as unknown as T,
): Promise<PartialResult<T>> {
  const db: Db = mongoClient.db(dbName);
  const col = db.collection(colName);
  const queryId = randomUUID();

  try {
    const result = await runWithPartialTracking(mongoClient, queryId, (opts) => runOnCollection(col, opts));
    if (!result.partial) {
      return { data: result.data, partial: false, unavailable_units: [] };
    }

    const shardStatus = await getUnavailableShards(mongoClient);
    return {
      data: result.data,
      partial: true,
      unavailable_units: explicitUnavailableUnits(shardStatus?.unavailable ?? []),
    };
  } catch (queryError) {
    if (!isConnectivityError(queryError)) throw queryError;

    const shardStatus = await getUnavailableShards(mongoClient);
    if (!shardStatus) {
      return { data: emptyResult(), partial: true, unavailable_units: ["unknown"] };
    }
    if (shardStatus.unavailable.length === 0) throw queryError;

    // Retry through mongos with partial results enabled. Mongos remains the
    // authority for routing; direct connections are used only for health probes.
    try {
      const retry = await runWithPartialTracking(mongoClient, randomUUID(), (opts) =>
        runOnCollection(col, { ...opts, maxTimeMS: 1500 }),
      );
      return {
        data: retry.data,
        partial: true,
        unavailable_units: shardStatus.unavailable,
      };
    } catch {
      return {
        data: emptyResult(),
        partial: true,
        unavailable_units: shardStatus.unavailable.length > 0
          ? shardStatus.unavailable
          : explicitUnavailableUnits(shardStatus.shardIds),
      };
    }
  }
}
