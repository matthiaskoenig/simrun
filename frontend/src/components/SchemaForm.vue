<!--
  The settings of a task as a form of their JSON Schema (`lib/schema-form.ts`):
  strings, numbers, booleans, enums, lists of primitives (one item per line)
  and nested models as a group; anything else is JSON text which keeps what is
  typed and says "not valid JSON" while it does not parse, without emitting.
  A field left empty is not written, so the default of the plugin applies; the
  default of the schema shows as the placeholder. Scalar fields take one of two
  columns, lists, groups, JSON and strings not known to be short both. The
  errors of the service for a field (`errors`, by dotted location) show under it.
-->
<script setup lang="ts">
import { reactive, watch } from "vue";
import { errorsAt, errorsBelow, fields as fieldsOf, setValue, wide, type Field, type JsonSchema } from "../lib/schema-form";

defineOptions({ name: "SchemaForm" });

const props = withDefaults(
  defineProps<{
    schema?: JsonSchema;
    fields?: Field[];
    value: Record<string, unknown>;
    /** The errors of the service by the dotted location of their field. */
    errors?: Record<string, string[]>;
  }>(),
  { schema: undefined, fields: undefined, errors: () => ({}) },
);
const emit = defineEmits<{ "update:value": [value: Record<string, unknown>] }>();

const list = (): Field[] => props.fields ?? fieldsOf(props.schema ?? {});

const INPUT = "w-full rounded-md border px-2 py-1.5 text-sm";

// the errors of the service for a field, and the border of its input
const said = (field: Field): string[] => errorsAt(props.errors, field);
const border = (field: Field): string => (problems[field.key] || said(field).length ? "border-red-400" : "border-gray-300");

// the text of the list and JSON fields, kept while it does not parse
const texts = reactive<Record<string, string>>({});
const problems = reactive<Record<string, string>>({});

function show(field: Field, value: unknown): string {
  if (value === undefined) return "";
  return field.kind === "list" ? (value as unknown[]).join("\n") : JSON.stringify(value, null, 2);
}

function parse(field: Field, text: string): { ok: true; value: unknown } | { ok: false; problem: string } {
  if (text.trim() === "") return { ok: true, value: undefined };
  if (field.kind === "json") {
    try {
      return { ok: true, value: JSON.parse(text) as unknown };
    } catch {
      return { ok: false, problem: "not valid JSON" };
    }
  }
  const items = text.split("\n").map((line) => line.trim()).filter((line) => line !== "");
  if (field.itemKind === "string") return { ok: true, value: items };
  const numbers = items.map(Number);
  const bad = numbers.some((n) => Number.isNaN(n) || (field.itemKind === "integer" && !Number.isInteger(n)));
  return bad ? { ok: false, problem: `not a list of ${field.itemKind === "integer" ? "integers" : "numbers"}` } : { ok: true, value: numbers };
}

// follow a value which changed from outside, but keep text which parses to the value it has
watch(
  [() => props.value, () => props.fields, () => props.schema],
  () => {
    for (const field of list()) {
      if (field.kind !== "list" && field.kind !== "json") continue;
      const current = props.value[field.key];
      const parsed = parse(field, texts[field.key] ?? "");
      const same = parsed.ok && JSON.stringify(parsed.value) === JSON.stringify(current);
      if (texts[field.key] === undefined || (!same && !problems[field.key])) {
        texts[field.key] = show(field, current);
        delete problems[field.key];
      }
    }
  },
  { immediate: true, deep: true },
);

function put(key: string, value: unknown): void {
  emit("update:value", setValue(props.value, [key], value));
}

function onText(field: Field, text: string): void {
  texts[field.key] = text;
  const parsed = parse(field, text);
  if (!parsed.ok) {
    problems[field.key] = parsed.problem;
    return;
  }
  delete problems[field.key];
  put(field.key, parsed.value);
}

function onNumber(field: Field, text: string): void {
  if (text.trim() === "") {
    delete problems[field.key];
    return put(field.key, undefined);
  }
  const n = Number(text);
  if (Number.isNaN(n) || (field.kind === "integer" && !Number.isInteger(n))) {
    problems[field.key] = field.kind === "integer" ? "a whole number" : "a number";
    return;
  }
  delete problems[field.key];
  put(field.key, n);
}

function onEnum(field: Field, index: string): void {
  put(field.key, index === "" ? undefined : field.options![Number(index)]);
}

const placeholder = (field: Field): string => (field.default === undefined || field.default === null ? "" : String(field.default));
const label = (field: Field): string => (field.required ? `${field.title} *` : field.title);

/** The rows of a text field: every item of a list in view (one more for the scrollbar of a long one), at most 8. */
const rows = (field: Field): number =>
  field.kind === "list" ? Math.min(8, Math.max(3, (texts[field.key] ?? "").split("\n").length + 1)) : 3;
</script>

<template>
  <div data-form class="grid grid-cols-2 gap-x-3 gap-y-3">
    <div v-for="field in list()" :key="field.key" :data-field="field.key" :class="wide(field) ? 'col-span-2' : ''">
      <fieldset v-if="field.kind === 'object'" class="rounded-lg border border-gray-200 p-3">
        <legend class="px-1 text-xs text-gray-600" :title="field.description">{{ label(field) }}</legend>
        <SchemaForm
          :fields="field.fields"
          :value="(value[field.key] as Record<string, unknown> | undefined) ?? {}"
          :errors="errorsBelow(errors, field.key)"
          @update:value="(sub) => put(field.key, Object.keys(sub).length ? sub : undefined)"
        />
      </fieldset>
      <label v-else-if="field.kind === 'boolean'" class="block">
        <span class="mb-1 block text-xs text-gray-600" :title="field.description">{{ label(field) }}</span>
        <select
          :class="[INPUT, border(field)]"
          :value="value[field.key] === undefined ? '' : String(value[field.key])"
          @change="(e) => put(field.key, (e.target as HTMLSelectElement).value === '' ? undefined : (e.target as HTMLSelectElement).value === 'true')"
        >
          <option value="">default{{ field.default === undefined ? "" : ` (${field.default})` }}</option>
          <option value="true">true</option>
          <option value="false">false</option>
        </select>
      </label>
      <label v-else-if="field.kind === 'enum'" class="block">
        <span class="mb-1 block text-xs text-gray-600" :title="field.description">{{ label(field) }}</span>
        <select
          :class="[INPUT, border(field)]"
          :value="String(field.options!.indexOf(value[field.key]) === -1 ? '' : field.options!.indexOf(value[field.key]))"
          @change="(e) => onEnum(field, (e.target as HTMLSelectElement).value)"
        >
          <option value="">default{{ field.default === undefined || field.default === null ? "" : ` (${field.default})` }}</option>
          <option v-for="(option, index) in field.options" :key="index" :value="String(index)">{{ option }}</option>
        </select>
      </label>
      <label v-else-if="field.kind === 'list' || field.kind === 'json'" class="block">
        <span class="mb-1 block text-xs text-gray-600" :title="field.description">
          {{ label(field) }}<span class="text-gray-400">{{ field.kind === "list" ? " (one per line)" : " (JSON)" }}</span>
        </span>
        <!-- a list is one item per line: a long item scrolls instead of wrapping into what looks like two -->
        <textarea
          :class="[INPUT, border(field), 'font-mono text-xs', field.kind === 'list' ? 'overflow-x-auto whitespace-pre' : '']"
          :wrap="field.kind === 'list' ? 'off' : 'soft'"
          :rows="rows(field)"
          :value="texts[field.key]"
          :placeholder="field.default === undefined ? '' : JSON.stringify(field.default)"
          @input="(e) => onText(field, (e.target as HTMLTextAreaElement).value)"
        ></textarea>
        <span v-if="problems[field.key]" class="mt-0.5 block text-[11px] text-red-700">{{ problems[field.key] }}</span>
      </label>
      <label v-else class="block">
        <span class="mb-1 block text-xs text-gray-600" :title="field.description">{{ label(field) }}</span>
        <input
          v-if="field.kind === 'string'"
          :class="[INPUT, border(field)]"
          :value="(value[field.key] as string | undefined) ?? ''"
          :placeholder="placeholder(field)"
          @input="(e) => put(field.key, (e.target as HTMLInputElement).value === '' ? undefined : (e.target as HTMLInputElement).value)"
        />
        <input
          v-else
          type="number"
          :step="field.kind === 'integer' ? 1 : 'any'"
          :class="[INPUT, border(field)]"
          :value="(value[field.key] as number | undefined) ?? ''"
          :placeholder="placeholder(field)"
          @input="(e) => onNumber(field, (e.target as HTMLInputElement).value)"
        />
        <span v-if="problems[field.key]" class="mt-0.5 block text-[11px] text-red-700">{{ problems[field.key] }}</span>
      </label>
      <span v-if="said(field).length" data-field-error class="mt-0.5 block text-[11px] text-red-700">{{ said(field).join("; ") }}</span>
      <p v-if="field.description && field.kind !== 'object'" class="mt-0.5 text-[11px] text-gray-500">{{ field.description }}</p>
    </div>
  </div>
</template>
