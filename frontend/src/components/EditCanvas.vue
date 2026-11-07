<!--
  The canvas of the Editor, the Vue Flow canvas of the mockup with editable
  tasks: a node per task of the workflow (`EditNode`), an edge per input.
  Dragging from an output handle to an input handle connects, a drop of a
  palette item adds a task at the drop point, Delete or Backspace removes the
  selected task or edge. The canvas holds no workflow: every edit is a pure
  function of `lib/edit-graph.ts` whose result goes up as `change`, a refusal
  as `refused`. Positions are the canvas's: the layered layout when it opens,
  then wherever a node is dragged or dropped; a task which appears later is
  placed right of the right-most node.
-->
<script setup lang="ts">
import { Background } from "@vue-flow/background";
import { Controls } from "@vue-flow/controls";
import { MarkerType, VueFlow, useVueFlow, type Connection, type Edge, type GraphNode, type Node } from "@vue-flow/core";
import { MiniMap } from "@vue-flow/minimap";
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import type { TaskEstimate, TaskTypeView, Workflow } from "../api/client";
import { DRAG_TYPE, addTask, connect, disconnect, removeTask } from "../lib/edit-graph";
import { COLUMN_PX, layout } from "../lib/graph";
import { kindOf } from "../lib/kinds";
import EditNode from "./EditNode.vue";

import "@vue-flow/core/dist/style.css";
import "@vue-flow/core/dist/theme-default.css";
import "@vue-flow/controls/dist/style.css";
import "@vue-flow/minimap/dist/style.css";

const props = defineProps<{
  workflow: Workflow;
  types: TaskTypeView[];
  estimate: Record<string, TaskEstimate>;
  taskErrors: Record<string, string[]>;
  selected: string | null;
}>();
const emit = defineEmits<{
  change: [workflow: Workflow];
  "update:selected": [task: string | null];
  refused: [message: string];
}>();

const { fitView, onNodesInitialized, screenToFlowCoordinate } = useVueFlow("editor");

const FIT = { padding: 0.14, maxZoom: 1 };

// the places of the nodes, the layered layout of the workflow when the canvas opens
const positions = new Map<string, { x: number; y: number }>(layout(props.workflow).nodes.map((n) => [n.id, n.position]));

function place(id: string): { x: number; y: number } {
  let known = positions.get(id);
  if (!known) {
    const xs = [...positions.values()].map((p) => p.x);
    known = { x: xs.length ? Math.max(...xs) + COLUMN_PX : 0, y: 0 };
    positions.set(id, known);
  }
  return known;
}

const specs = computed(() => new Map(props.workflow.tasks.map((task) => [task.id, task])));
const typesByName = computed(() => new Map(props.types.map((type) => [type.name, type])));

const nodes = computed<Node[]>(() => props.workflow.tasks.map((task) => ({ id: task.id, type: "task", position: { ...place(task.id) }, data: { type: task.type } })));

const selectedEdge = ref<string | null>(null);

const edges = computed<Edge[]>(() =>
  props.workflow.tasks.flatMap((task) =>
    Object.entries(task.inputs ?? {}).flatMap(([port, input]): Edge[] => {
      const dot = input.indexOf(".");
      const source = dot === -1 ? input : input.slice(0, dot);
      const sourcePort = dot === -1 ? "" : input.slice(dot + 1);
      if (!specs.value.has(source)) return [];
      const id = `${source}.${sourcePort}->${task.id}.${port}`;
      const stroke = selectedEdge.value === id ? "#3b82f6" : "#94a3b8";
      return [
        {
          id,
          source,
          sourceHandle: sourcePort,
          target: task.id,
          targetHandle: port,
          label: sourcePort,
          style: { stroke, strokeWidth: 2 },
          markerEnd: { type: MarkerType.ArrowClosed, color: stroke, width: 14, height: 14 },
          labelStyle: { fill: "#6b7280", fontSize: "10px", fontFamily: "var(--font-mono)" },
          labelBgStyle: { fill: "#f9fafb" },
          labelBgPadding: [4, 2] as [number, number],
          labelBgBorderRadius: 4,
        },
      ];
    }),
  ),
);

/** A connection dragged between two handles: refused here, before it reaches the workflow. */
function onConnect(connection: Connection): void {
  if (!connection.sourceHandle || !connection.targetHandle) return;
  const next = connect(
    props.workflow,
    { task: connection.source, port: connection.sourceHandle },
    { task: connection.target, port: connection.targetHandle },
  );
  if (typeof next === "string") emit("refused", next);
  else emit("change", next);
}

function onDrop(event: DragEvent): void {
  const name = event.dataTransfer?.getData(DRAG_TYPE);
  const type = name ? typesByName.value.get(name) : undefined;
  if (!type) return;
  event.preventDefault();
  const added = addTask(props.workflow, type);
  const at = screenToFlowCoordinate({ x: event.clientX, y: event.clientY });
  // the drop point is the middle of the card
  positions.set(added.id, { x: at.x - 128, y: at.y - 40 });
  emit("change", added.workflow);
  emit("update:selected", added.id);
}

function onKey(event: KeyboardEvent): void {
  if (event.key !== "Delete" && event.key !== "Backspace") return;
  const target = event.target as HTMLElement | null;
  if (target && (target.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName))) return;
  if (selectedEdge.value) {
    const edge = edges.value.find((e) => e.id === selectedEdge.value);
    selectedEdge.value = null;
    if (edge?.targetHandle) emit("change", disconnect(props.workflow, { task: edge.target, port: edge.targetHandle }));
  } else if (props.selected && specs.value.has(props.selected)) {
    const id = props.selected;
    positions.delete(id);
    emit("update:selected", null);
    emit("change", removeTask(props.workflow, id));
  } else return;
  event.preventDefault();
}

watch(
  () => props.selected,
  (task) => {
    if (task) selectedEdge.value = null;
  },
);

// the view fits the first layout; Vue Flow initializes the nodes again on every added task,
// which must not move the view away from where the person works
let fitted = false;
onNodesInitialized(() => {
  if (fitted) return;
  fitted = true;
  void fitView(FIT);
});

// a changed size of the canvas fits the view again, once per frame
const container = ref<HTMLElement | null>(null);
let frame = 0;
const observer = new ResizeObserver(() => {
  cancelAnimationFrame(frame);
  frame = requestAnimationFrame(() => void fitView(FIT));
});
onMounted(() => {
  if (container.value) observer.observe(container.value);
  window.addEventListener("keydown", onKey);
});
onBeforeUnmount(() => {
  cancelAnimationFrame(frame);
  observer.disconnect();
  window.removeEventListener("keydown", onKey);
});

const minimapColor = (node: GraphNode): string => kindOf(String(node.data?.type ?? "")).color;

/** Moves the position of a node to the new id of a renamed task, so the node stays where it is. */
function rename(from: string, to: string): void {
  const known = positions.get(from);
  if (known) positions.set(to, known);
  positions.delete(from);
}

defineExpose({ onConnect, rename });
</script>

<template>
  <div ref="container" class="edit-canvas relative min-h-0 flex-1" @drop="onDrop" @dragover.prevent>
    <VueFlow
      id="editor"
      :nodes="nodes"
      :edges="edges"
      :min-zoom="0.3"
      :max-zoom="1.6"
      :nodes-connectable="true"
      :delete-key-code="null"
      class="bg-gray-50"
      @connect="onConnect"
      @node-click="({ node }) => emit('update:selected', node.id)"
      @edge-click="({ edge }) => ((selectedEdge = edge.id), emit('update:selected', null))"
      @pane-click="(selectedEdge = null), emit('update:selected', null)"
      @node-drag-stop="({ node }) => positions.set(node.id, { ...node.position })"
    >
      <template #node-task="{ id }">
        <EditNode
          v-if="specs.get(id)"
          :task="specs.get(id)!"
          :type="typesByName.get(specs.get(id)!.type) ?? null"
          :errors="taskErrors[id] ?? []"
          :estimate="estimate[id] ?? null"
          :selected="selected === id"
        />
      </template>
      <Background :gap="22" :size="1.3" pattern-color="#d1d5db" />
      <Controls position="bottom-left" :show-interactive="false" />
      <MiniMap position="bottom-right" pannable zoomable :node-color="minimapColor" :width="160" :height="100" />
    </VueFlow>
    <slot />
  </div>
</template>

<style>
.edit-canvas .vue-flow__handle {
  width: 10px;
  height: 10px;
  background: #fff;
  border: 2px solid #94a3b8;
}
.edit-canvas .vue-flow__handle:hover {
  border-color: #3b82f6;
}
.edit-canvas .vue-flow__node-task {
  padding: 0;
  border: 0;
  background: transparent;
}
.edit-canvas .vue-flow__minimap {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  overflow: hidden;
}
.edit-canvas .vue-flow__controls {
  box-shadow: none;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  overflow: hidden;
}
.edit-canvas .vue-flow__controls-button {
  border-bottom: 1px solid #f1f5f9;
}
</style>
