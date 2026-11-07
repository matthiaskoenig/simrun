/**
 * The graph of a workflow on the canvas: a task is a node, an input
 * `<task>.<port>` an edge from that task labelled with the port.
 *
 * The layout is layered without a layout library: a task without inputs is in
 * the first column, any other one column after the deepest task it reads
 * (x = depth * 320); the tasks of a column are rows 200 apart in the order of
 * the workflow, centered on y = 0, so that the columns line up on their middle
 * like the mockup's.
 */
import type { TaskState, Workflow } from "../api/client";

/** The distance of two columns, a node (256 px) and its edges. */
export const COLUMN_PX = 320;
/** The distance of two rows, a node with its worker bars and some air. */
export const ROW_PX = 200;

/** A task on the canvas. */
export interface GraphNode {
  id: string;
  position: { x: number; y: number };
  data: { taskId: string; type: string };
}

/** An input of a task on the canvas, from the task it reads. */
export interface GraphEdge {
  id: string;
  source: string;
  target: string;
  label: string;
}

/** The task and the port of an input `<task>.<port>`; a task id has no dot, a port may. */
function parseInput(input: string): { task: string; port: string } {
  const dot = input.indexOf(".");
  return dot === -1 ? { task: input, port: "" } : { task: input.slice(0, dot), port: input.slice(dot + 1) };
}

/** The nodes and edges of a workflow; an input of an unknown task (which the service refuses) is left out. */
export function layout(workflow: Workflow): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const tasks = new Map(workflow.tasks.map((task) => [task.id, task]));
  const sources = (id: string): string[] =>
    Object.values(tasks.get(id)?.inputs ?? {})
      .map((input) => parseInput(input).task)
      .filter((task) => tasks.has(task));

  const depths = new Map<string, number>();
  const depth = (id: string, path: ReadonlySet<string>): number => {
    const known = depths.get(id);
    if (known !== undefined) return known;
    // a cycle, which the service refuses, does not loop
    const inputs = sources(id).filter((task) => !path.has(task));
    const next = new Set(path).add(id);
    const value = inputs.length === 0 ? 0 : 1 + Math.max(...inputs.map((task) => depth(task, next)));
    depths.set(id, value);
    return value;
  };

  const columns = new Map<number, string[]>();
  for (const task of workflow.tasks) {
    const column = depth(task.id, new Set());
    columns.set(column, [...(columns.get(column) ?? []), task.id]);
  }
  const rows = new Map<string, { row: number; of: number }>();
  for (const ids of columns.values()) ids.forEach((id, row) => rows.set(id, { row, of: ids.length }));

  const nodes = workflow.tasks.map((task): GraphNode => {
    const { row, of } = rows.get(task.id) ?? { row: 0, of: 1 };
    return {
      id: task.id,
      position: { x: (depths.get(task.id) ?? 0) * COLUMN_PX, y: (row - (of - 1) / 2) * ROW_PX },
      data: { taskId: task.id, type: task.type },
    };
  });
  const edges = workflow.tasks.flatMap((task) =>
    Object.entries(task.inputs ?? {}).flatMap(([name, input]): GraphEdge[] => {
      const { task: source, port } = parseInput(input);
      if (!tasks.has(source)) return [];
      return [{ id: `${source}.${port}->${task.id}.${name}`, source, target: task.id, label: port }];
    }),
  );
  return { nodes, edges };
}

/** How an edge is drawn: its color, whether it is dashed or animated. */
export interface EdgeLook {
  stroke: string;
  dash: string | undefined;
  animated: boolean;
}

const FLOWING: ReadonlySet<TaskState> = new Set<TaskState>(["queued", "running", "preempted", "retrying"]);

/**
 * The look of an edge by the states of its tasks, as in the mockup: blue and
 * animated while the output feeds a task at work, green once both are done,
 * red dashed from a failed task, gray dashed otherwise.
 */
export function edgeLook(source: TaskState | undefined, target: TaskState | undefined): EdgeLook {
  if (source === "done" && target !== undefined && FLOWING.has(target)) {
    return { stroke: "#3b82f6", dash: undefined, animated: true };
  }
  if (source === "done" && target === "done") return { stroke: "#10b981", dash: undefined, animated: false };
  if (source === "failed") return { stroke: "#fca5a5", dash: "6 5", animated: false };
  return { stroke: "#cbd5e1", dash: "6 5", animated: false };
}
