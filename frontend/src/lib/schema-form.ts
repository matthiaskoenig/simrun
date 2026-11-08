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
  /** The longest text a `string` takes, when the schema says. */
  maxLength?: number;
}

/** The longest `maxLength` of a string which still fits one of the two columns of the form. */
export const SHORT_STRING = 32;

/**
 * Whether a field spans both columns of the form: lists, groups and JSON, and
 * strings which are not known to be short (an image, a path).
 */
export function wide(field: Field): boolean {
  if (field.kind === "list" || field.kind === "object" || field.kind === "json") return true;
  return field.kind === "string" && !(field.maxLength !== undefined && field.maxLength <= SHORT_STRING);
}

/**
 * The errors of the service by the dotted location of their field, from the
 * errors of a task (`<loc>: <msg>`, the task prefix removed); the others are
 * returned apart, as written.
 */
export function fieldErrors(errors: string[], keys: string[]): { byField: Record<string, string[]>; other: string[] } {
  const byField: Record<string, string[]> = {};
  const other: string[] = [];
  const known = new Set(keys);
  for (const error of errors) {
    const match = /^([\w.]+): (.*)$/s.exec(error);
    const loc = match?.[1];
    if (match && loc && known.has(loc.split(".")[0]!)) (byField[loc] ??= []).push(match[2]!);
    else other.push(error);
  }
  return { byField, other };
}

/** The errors of the fields under `key`, by their location below it. */
export function errorsBelow(errors: Record<string, string[]>, key: string): Record<string, string[]> {
  const prefix = `${key}.`;
  return Object.fromEntries(Object.entries(errors).filter(([loc]) => loc.startsWith(prefix)).map(([loc, list]) => [loc.slice(prefix.length), list]));
}

/** The errors of the field `key` itself, and those of the items of a list or JSON field (`key.<index>`), which have no field of their own. */
export function errorsAt(errors: Record<string, string[]>, field: Field): string[] {
  const own = errors[field.key] ?? [];
  if (field.kind === "object") return own;
  const prefix = `${field.key}.`;
  return [...own, ...Object.entries(errors).flatMap(([loc, list]) => (loc.startsWith(prefix) ? list.map((m) => `${loc.slice(prefix.length)}: ${m}`) : []))];
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
      return { ...common, kind: "string", ...(typeof node.maxLength === "number" ? { maxLength: node.maxLength } : {}) };
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
  // a field marked `readOnly` is set by the plan of the plugin, e.g. the units
  // of a split task, and is no setting of the form
  return Object.entries(node.properties).flatMap(([key, raw]) =>
    isRecord(raw) && raw.readOnly !== true ? [field(root, key, raw, required.has(key))] : [],
  );
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
