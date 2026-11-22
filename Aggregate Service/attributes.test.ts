import assert from "node:assert/strict";
import test from "node:test";
import { normalizeAttributes, toStruct } from "./attributes";

test("plain JS attributes stay plain on ingest and wrap once on read", () => {
  const plain = {
    magnitude: 5.4,
    depth_km: 10,
    region: "Banda Sea",
    tags: ["seismic", 2],
    nested: { felt: true },
  };
  const stored = normalizeAttributes(plain);
  assert.deepEqual(stored, plain);

  const responseStruct = toStruct(normalizeAttributes(stored));
  assert.equal("fields" in responseStruct.fields, false);
  assert.deepEqual(responseStruct.fields.magnitude, { numberValue: 5.4 });
  assert.deepEqual(responseStruct.fields.region, { stringValue: "Banda Sea" });
  assert.deepEqual(responseStruct.fields.nested, {
    structValue: { fields: { felt: { boolValue: true } } },
  });
});

test("legacy single-wrapped Struct in MongoDB is unwrapped cleanly without double fields", () => {
  const legacySingle = {
    fields: {
      mag: { numberValue: 5.4 },
      nested: {
        structValue: {
          fields: {
            a: { stringValue: "x" },
          },
        },
      },
    },
  };

  const normalized = normalizeAttributes(legacySingle);
  assert.deepEqual(normalized, { mag: 5.4, nested: { a: "x" } });

  const responseStruct = toStruct(normalized);
  assert.equal("fields" in responseStruct.fields, false);
  assert.deepEqual(responseStruct.fields.mag, { numberValue: 5.4 });
  assert.deepEqual(responseStruct.fields.nested, {
    structValue: { fields: { a: { stringValue: "x" } } },
  });
});

test("legacy double-wrapped Struct (fields.fields) in MongoDB is unwrapped cleanly", () => {
  const legacyDouble = {
    fields: {
      fields: {
        structValue: {
          fields: {
            mag: { numberValue: 5.4 },
            label: { stringValue: "alert" },
          },
        },
      },
    },
  };

  const normalized = normalizeAttributes(legacyDouble);
  assert.deepEqual(normalized, { mag: 5.4, label: "alert" });

  const responseStruct = toStruct(normalized);
  assert.equal("fields" in responseStruct.fields, false);
  assert.deepEqual(responseStruct.fields.mag, { numberValue: 5.4 });
  assert.deepEqual(responseStruct.fields.label, { stringValue: "alert" });
});
