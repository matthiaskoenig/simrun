import { describe, expect, it } from "vitest";
import { fields, setValue, type JsonSchema } from "./schema-form";

// what pydantic's model_json_schema() gives of a model with a nested model, an optional int,
// an enum, a list of floats, a dict, and a list of models
const schema: JsonSchema = {
  $defs: {
    Mode: { enum: ["fast", "slow"], title: "Mode", type: "string" },
    Inner: {
      properties: { depth: { default: 3, title: "Depth", type: "integer" } },
      title: "Inner",
      type: "object",
    },
  },
  properties: {
    name: { description: "The name.", title: "Name", type: "string" },
    n: { anyOf: [{ type: "integer" }, { type: "null" }], default: null, title: "N" },
    mode: { $ref: "#/$defs/Mode", default: "fast" },
    weights: { items: { type: "number" }, title: "Weights", type: "array" },
    labels: { additionalProperties: { type: "string" }, title: "Labels", type: "object" },
    inner: { $ref: "#/$defs/Inner" },
    many: { items: { $ref: "#/$defs/Inner" }, title: "Many", type: "array" },
    on: { default: true, title: "On", type: "boolean" },
  },
  required: ["name"],
  title: "Settings",
  type: "object",
};

describe("fields", () => {
  const byKey = Object.fromEntries(fields(schema).map((f) => [f.key, f]));

  it("reads scalars with title, description, required", () => {
    expect(byKey.name).toMatchObject({ kind: "string", title: "Name", description: "The name.", required: true, nullable: false });
    expect(byKey.on).toMatchObject({ kind: "boolean", default: true, required: false });
  });

  it("reads an optional int as the int, nullable", () => {
    expect(byKey.n).toMatchObject({ kind: "integer", nullable: true, default: null });
  });

  it("resolves an enum through $ref", () => {
    expect(byKey.mode).toMatchObject({ kind: "enum", options: ["fast", "slow"], default: "fast" });
  });

  it("reads a list of numbers and falls back to json for other items", () => {
    expect(byKey.weights).toMatchObject({ kind: "list", itemKind: "number" });
    expect(byKey.many!.kind).toBe("json");
  });

  it("reads a dict as json and a nested model as an object", () => {
    expect(byKey.labels!.kind).toBe("json");
    expect(byKey.inner).toMatchObject({ kind: "object", fields: [{ key: "depth", kind: "integer", default: 3 }] });
  });

  it("leaves out a field the plan of the plugin sets, marked readOnly", () => {
    const planned: JsonSchema = {
      properties: {
        units: { anyOf: [{ type: "integer" }, { type: "null" }], default: null, readOnly: true, title: "Units" },
        seed: { anyOf: [{ type: "integer" }, { type: "null" }], default: null, title: "Seed" },
      },
      type: "object",
    };
    expect(fields(planned).map((f) => f.key)).toEqual(["seed"]);
  });

  it("gives none for a schema without properties", () => {
    expect(fields({})).toEqual([]);
  });
});

describe("setValue", () => {
  it("sets a nested value without touching the input", () => {
    const before = { a: 1 };
    expect(setValue(before, ["inner", "depth"], 4)).toEqual({ a: 1, inner: { depth: 4 } });
    expect(before).toEqual({ a: 1 });
  });

  it("removes the key and an object left empty", () => {
    expect(setValue({ a: 1, inner: { depth: 4 } }, ["inner", "depth"], undefined)).toEqual({ a: 1 });
    expect(setValue({ a: 1 }, ["a"], undefined)).toEqual({});
  });
});
