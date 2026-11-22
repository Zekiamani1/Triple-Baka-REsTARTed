import type { Collection, Db, MongoClient } from "mongodb";

export interface PartialResult<T> {
  data: T;
  partial: boolean;
  unavailable_units: string[];
}

export async function queryWithPartialFallback<T>(
  mongoClient: MongoClient,
  dbName: string,
  colName: string,
  runOnCollection: (col: Collection, opts: { allowPartialResults: boolean; maxTimeMS: number }) => Promise<T>,
  mergeResults?: (parts: T[]) => T,
): Promise<PartialResult<T>> {
  const db: Db = mongoClient.db(dbName);
  const col = db.collection(colName);

  try {
    const data = await runOnCollection(col, { allowPartialResults: true, maxTimeMS: 2500 });
    return { data, partial: false, unavailable_units: [] };
  } catch (primaryErr) {
    // Attempt per-shard health/query check if mongos failed due to an unreachable shard
    const configDb = mongoClient.db("config");
    let shards: Array<{ _id: string; host: string }> = [];
    try {
      shards = await configDb
        .collection<{ _id: string; host: string }>("shards")
        .find({}, { maxTimeMS: 1000 })
        .toArray();
    } catch {
      throw primaryErr;
    }

    if (shards.length === 0) {
      throw primaryErr;
    }

    const unavailable: string[] = [];
    const healthyParts: T[] = [];

    await Promise.all(
      shards.map(async (shard) => {
        try {
          const res = await db.admin().command({ ping: 1 }, { timeoutMS: 1000 });
          if (res?.ok !== 1) {
            unavailable.push(shard._id);
            return;
          }
          if (mergeResults) {
            const part = await runOnCollection(col, { allowPartialResults: true, maxTimeMS: 1000 });
            healthyParts.push(part);
          }
        } catch {
          unavailable.push(shard._id);
        }
      }),
    );

    if (mergeResults && healthyParts.length > 0) {
      return {
        data: mergeResults(healthyParts),
        partial: unavailable.length > 0,
        unavailable_units: unavailable,
      };
    }

    if (unavailable.length === shards.length) {
      throw primaryErr;
    }

    // Fallback empty/partial signal if mongos couldn't return rows
    return {
      data: ([] as unknown) as T,
      partial: true,
      unavailable_units: unavailable.length > 0 ? unavailable : shards.map((s) => s._id),
    };
  }
}
