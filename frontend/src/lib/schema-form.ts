/**
 * The settings form of a task type, from the JSON Schema pydantic gives of its
 * settings model (`model_json_schema()`): the fields to render, and the
 * update of a settings object by path. Constructs the form does not know
 * (a union, a dict, a list of objects) edit as JSON text.
 */

/** A JSON Schema node. */
export type JsonSchema = Record<string, unknown>;

/** The kind of input a field gets. */
export type FieldKind = "string" | "number" | "integer" | "boolean" | "enum" | "list" | "object" | "json";

/** One field of the settings form. */
export interface Field {
  key: string;
  title: string;
  description: string;
  required: boolean;
  default: unknown;
  kind: FieldKind;
  /** The values of an `enum`. */
  options?: unknown[];
  /** The kind of the items of a `list`. */
  itemKind?: "string" | "number" | "integer";
  /** The fields of an `object`. */
  fields?: Field[];
  /** Whether `null` is allowed (`anyOf` with `{type: "null"}`). */
  nullable: boolean;
}

const ITEM_KINDS = new Set(["string", "number", "integer"]);

const isRecord = (value: unknown): value is JsonSchema => typeof value === "object" && value !== null && !Array.isArray(value);

/** The node a `$ref` into the root `$defs` (or `definitions`) points at, followed through chained refs. */
function deref(root: JsonSchema, node: JsonSchema): JsonSchema {
  let current = node;
  for (let depth = 0; depth < 32; depth += 1) {
    const ref = current.$ref;
    if (typeof ref !== "string") return current;
    const match = /^#\/(\$defs|definitions)\/(.+)$/.exec(ref);
    const defs = match ? root[match[1]!] : undefined;
    const target = isRecord(defs) ? defs[match![2]!] : undefined;
    if (!isRecord(target)) return {};
    const { $ref: _ref, ...rest } = current;
    current = { ...target, ...rest };
  }
  return {};
}

/** A node without its `anyOf` null branch or a one-element `allOf`, and whether it was nullable. */
function unwrap(root: JsonSchema, node: JsonSchema): { node: JsonSchema; nullable: boolean } | null {
  let current = deref(root, node);
  let nullable = false;
  if (Array.isArray(current.allOf) && current.allOf.length === 1 && isRecord(current.allOf[0])) {
    const { allOf: _all, ...rest } = current;
    current = { ...deref(root, current.allOf[0]), ...rest };
  }
  if (Array.isArray(current.anyOf)) {
    const branches = current.anyOf.filter(isRecord);
    const rest = branches.filter((b) => b.type !== "null");
    nullable = rest.length < branches.length;
    if (rest.length !== 1 || branches.length !== current.anyOf.length) return null;
    const { anyOf: _any, ...outer } = current;
    current = { ...deref(root, rest[0]!), ...outer };
  }
  if (Array.isArray(current.type)) {
    const types = current.type.filter((t) => t !== "null");
    nullable = types.length < current.type.length;
    if (types.length !== 1) return null;
    current = { ...current, type: types[0] };
  }
  return { node: current, nullable };
}

function field(root: JsonSchema, key: string, raw: JsonSchema, required: boolean): Field {
  const unwrapped = unwrap(root, raw);
  const base = { key, required, nullable: unwrapped?.nullable ?? false };
  const node = unwrapped?.node ?? {};
  const text = (name: string): string => {
    const own = raw[name];
    const inner = node[name];
    return typeof own === "string" ? own : typeof inner === "string" ? inner : "";
  };
  const common = {
    ...base,
    title: text("title") || key,
    description: text("description"),
    default: "default" in raw ? raw.default : node.default,
  };
  const json: Field = { ...common, kind: "json" };
  if (!unwrapped) return json;
  if (Array.isArray(node.enum)) return { ...common, kind: "enum", options: node.enum };
  switch (node.type) {
    case "string":
    case "number":
    case "integer":
    case "boolean":
      return { ...common, kind: node.type };
    case "array": {
      const item = isRecord(node.items) ? unwrap(root, node.items) : null;
      const type = item?.node.type;
      if (item && typeof type === "string" && ITEM_KINDS.has(type) && !item.node.enum) {
        return { ...common, kind: "list", itemKind: type as "string" | "number" | "integer" };
      }
      return json;
    }
    case "object":
      if (isRecord(node.properties)) return { ...common, kind: "object", fields: fieldsOf(root, node) };
      return json;
    default:
      return json;
  }
}

function fieldsOf(root: JsonSchema, node: JsonSchema): Field[] {
  if (!isRecord(node.properties)) return [];
  const required = new Set(Array.isArray(node.required) ? node.required : []);
  return Object.entries(node.properties).flatMap(([key, raw]) => (isRecord(raw) ? [field(root, key, raw, required.has(key))] : []));
}

/** The fields of the settings model a schema describes; none for a schema without properties. */
export function fields(schema: JsonSchema): Field[] {
  return fieldsOf(schema, deref(schema, schema));
}

/**
 * A copy of `settings` with the value at `path` set, or removed when `value`
 * is undefined; an object left empty by the removal is removed too, except
 * the root.
 */
export function setValue(settings: Record<string, unknown>, path: string[], value: unknown | undefined): Record<string, unknown> {
  const [head, ...rest] = path;
  if (head === undefined) return settings;
  const next = { ...settings };
  let child: unknown = value;
  if (rest.length > 0) {
    const existing = settings[head];
    child = setValue(isRecord(existing) ? existing : {}, rest, value);
    if (Object.keys(child as object).length === 0) child = undefined;
  }
  if (child === undefined) delete next[head];
  else next[head] = child;
  return next;
}
