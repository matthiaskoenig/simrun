/**
 * The edits of a workflow on the canvas of the Editor, as pure functions: each
 * returns a new workflow and never changes its argument. A refusal is a
 * message (a string), which the canvas shows; the service validates the whole
 * workflow again afterwards.
 */
import type { TaskSpec, TaskTypeView, Workflow } from "../api/client";

/** The id of a task, `TaskSpec.id` of the service. */
const TASK_ID = /^[a-z][a-z0-9-]*$/;

/** The type of the drag data which carries the name of a task type from the palette to the canvas. */
export const DRAG_TYPE = "application/x-simrun-task-type";

/** A task and one of its ports. */
export interface PortRef {
  task: string;
  port: string;
}

/** The group of the palette: the types with one prefix. */
export interface PaletteGroup {
  title: string;
  types: TaskTypeView[];
}

const GENERAL = "General";

function groupTitle(name: string): string {
  const dot = name.indexOf(".");
  if (dot <= 0) return GENERAL;
  const prefix = name.slice(0, dot);
  return prefix.charAt(0).toUpperCase() + prefix.slice(1).toLowerCase();
}

/** The text of a type a search matches: its name and the title and description of its settings schema. */
function searchText(type: TaskTypeView): string {
  const { title, description } = type.settings_schema as { title?: unknown; description?: unknown };
  return [type.name, title, description].filter((part) => typeof part === "string").join(" ").toLowerCase();
}

/** The task types of the palette by the part of their name before the first dot, filtered by a search query. */
export function paletteGroups(types: TaskTypeView[], query: string): PaletteGroup[] {
  const needle = query.trim().toLowerCase();
  const groups = new Map<string, TaskTypeView[]>();
  for (const type of types) {
    if (needle && !searchText(type).includes(needle)) continue;
    const title = groupTitle(type.name);
    groups.set(title, [...(groups.get(title) ?? []), type]);
  }
  const rank = (title: string): number => (title === GENERAL ? 1 : 0);
  return [...groups.entries()]
    .sort(([a], [b]) => rank(a) - rank(b) || a.localeCompare(b))
    .map(([title, list]) => ({ title, types: [...list].sort((a, b) => a.name.localeCompare(b.name)) }));
}

/** The id of a new task of a type: the last part of the name, with `-2`, `-3` for duplicates. */
export function newTaskId(workflow: Workflow, typeName: string): string {
  let base = (typeName.split(".").pop() ?? "").toLowerCase().replace(/[^a-z0-9-]/g, "-");
  if (!/^[a-z]/.test(base)) base = `task-${base}`.replace(/-+$/, "");
  const taken = new Set(workflow.tasks.map((task) => task.id));
  if (!taken.has(base)) return base;
  let n = 2;
  while (taken.has(`${base}-${n}`)) n += 1;
  return `${base}-${n}`;
}

// a JSON round trip: the workflow may be a reactive proxy, which structuredClone refuses
const clone = (workflow: Workflow): Workflow => JSON.parse(JSON.stringify(workflow)) as Workflow;

/** The workflow with a new task of a type, and the id of the task. */
export function addTask(workflow: Workflow, type: TaskTypeView): { workflow: Workflow; id: string } {
  const id = newTaskId(workflow, type.name);
  const next = clone(workflow);
  next.tasks.push({ id, type: type.name, settings: {} });
  return { workflow: next, id };
}

/** The task of an input `<task>.<port>`; a task id has no dot. */
const sourceTask = (input: string): string => input.split(".", 1)[0] ?? "";

/** Drops the entries of `stage` for the ports an edit removed. */
function dropStage(task: TaskSpec): void {
  if (task.stage) task.stage = task.stage.filter((port) => task.inputs?.[port] !== undefined);
}

/** The workflow without a task, and without the inputs which read it. */
export function removeTask(workflow: Workflow, id: string): Workflow {
  const next = clone(workflow);
  next.tasks = next.tasks.filter((task) => task.id !== id);
  for (const task of next.tasks) {
    for (const [port, input] of Object.entries(task.inputs ?? {})) {
      if (sourceTask(input) === id) delete task.inputs![port];
    }
    dropStage(task);
  }
  return next;
}

/** Whether `task` reads, directly or not, the output of `from`. */
function reads(workflow: Workflow, task: string, from: string, seen = new Set<string>()): boolean {
  if (seen.has(task)) return false;
  seen.add(task);
  const spec = workflow.tasks.find((t) => t.id === task);
  return Object.values(spec?.inputs ?? {}).some((input) => {
    const source = sourceTask(input);
    return source === from || reads(workflow, source, from, seen);
  });
}

/** The handle of a task of a type which reads any input, `command`: a connection to it adds a port. */
export const NEW_INPUT = "+";

/** Whether a type takes inputs on ports of any name: the built-in `command`, whose args get them. */
export const anyInputs = (typeName: string): boolean => typeName === "command";

/** The input ports a task shows: those of its type and those it reads, and `NEW_INPUT` for a `command`. */
export function inputPorts(task: TaskSpec, type: TaskTypeView | null): string[] {
  const used = Object.keys(task.inputs ?? {});
  const ports = [...(type?.inputs ?? []), ...used.filter((port) => !type?.inputs.includes(port)).sort()];
  return anyInputs(task.type) ? [...ports, NEW_INPUT] : ports;
}

/** The workflow with the input `target` reading the output `source`, or the reason it is refused.
 * A connection to `NEW_INPUT` adds a port named after the source task. */
export function connect(workflow: Workflow, source: PortRef, target: PortRef): Workflow | string {
  if (source.task === target.task) return "a task cannot read its own output";
  const spec = workflow.tasks.find((task) => task.id === target.task);
  if (!spec) return `there is no task ${target.task}`;
  if (target.port === NEW_INPUT) {
    const taken = new Set(Object.keys(spec.inputs ?? {}));
    let port = source.task;
    for (let n = 2; taken.has(port); n += 1) port = `${source.task}-${n}`;
    target = { ...target, port };
  }
  if (spec.inputs?.[target.port] !== undefined) return `the input ${target.port} of ${target.task} is already connected`;
  if (reads(workflow, source.task, target.task)) return "this connection makes a cycle";
  const next = clone(workflow);
  const task = next.tasks.find((t) => t.id === target.task)!;
  task.inputs = { ...task.inputs, [target.port]: `${source.task}.${source.port}` };
  return next;
}

/** The workflow without the connection of the input `target`. */
export function disconnect(workflow: Workflow, target: PortRef): Workflow {
  const next = clone(workflow);
  const task = next.tasks.find((t) => t.id === target.task);
  if (task?.inputs) {
    delete task.inputs[target.port];
    dropStage(task);
  }
  return next;
}

/** The workflow with a task under another id and the inputs which read it rewritten, or the reason it is refused. */
export function renameTask(workflow: Workflow, from: string, to: string): Workflow | string {
  if (to === from) return clone(workflow);
  if (!TASK_ID.test(to)) return `the id ${to} is not valid: lowercase letters, digits and dashes, starting with a letter`;
  if (workflow.tasks.some((task) => task.id === to)) return `the id ${to} is taken`;
  const next = clone(workflow);
  for (const task of next.tasks) {
    if (task.id === from) task.id = to;
    for (const [port, input] of Object.entries(task.inputs ?? {})) {
      if (sourceTask(input) === from) task.inputs![port] = `${to}${input.slice(from.length)}`;
    }
  }
  return next;
}

/** The pan and zoom of the canvas, as Vue Flow names them. */
export interface View {
  x: number;
  y: number;
  zoom: number;
}

/**
 * The view which shows a node whole, panned as little as possible and at the
 * same zoom, or null when the node is shown whole already. `rect` is the node
 * in flow coordinates, `pane` the size of the canvas on screen, `margin` the
 * room to keep free at each side (the bar floats over the top).
 */
export function panInto(
  view: View,
  pane: { width: number; height: number },
  rect: { x: number; y: number; width: number; height: number },
  margin: { top: number; right: number; bottom: number; left: number },
): View | null {
  const shift = (start: number, size: number, low: number, high: number): number => {
    if (start < low) return low - start;
    if (start + size > high) return Math.max(high - (start + size), low - start);
    return 0;
  };
  const dx = shift(rect.x * view.zoom + view.x, rect.width * view.zoom, margin.left, pane.width - margin.right);
  const dy = shift(rect.y * view.zoom + view.y, rect.height * view.zoom, margin.top, pane.height - margin.bottom);
  if (dx === 0 && dy === 0) return null;
  return { x: view.x + dx, y: view.y + dy, zoom: view.zoom };
}
