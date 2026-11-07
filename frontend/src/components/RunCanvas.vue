<!--
  The canvas of a run, the Vue Flow canvas of the mockup: its tasks as
  `TaskNode` cards with their live state, the inputs as edges colored by the
  states of their tasks, the dots, the controls and the minimap. The view is
  fitted to the tasks once they are measured and whenever the canvas changes
  its size, so that a resized window never leaves them off screen. A click on
  a task selects it, a click on the pane clears the selection.

  The layout is the layout of the workflow, which does not change within a
  run: the parent keys the canvas by the run, and the live state reaches the
  nodes through the slot, not through the nodes of Vue Flow.
-->
<script setup lang="ts">
import { Background } from "@vue-flow/background";
import { Controls } from "@vue-flow/controls";
import { Handle, MarkerType, Position, VueFlow, useVueFlow, type Edge, type GraphNode, type Node } from "@vue-flow/core";
import { MiniMap } from "@vue-flow/minimap";
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import type { RunRecord, TaskProgress, TaskSpec, WorkerView } from "../api/client";
import { edgeLook, layout } from "../lib/graph";
import { kindOf } from "../lib/kinds";
import TaskNode from "./TaskNode.vue";

import "@vue-flow/core/dist/style.css";
import "@vue-flow/core/dist/theme-default.css";
import "@vue-flow/controls/dist/style.css";
import "@vue-flow/minimap/dist/style.css";

const props = defineProps<{
  record: RunRecord;
  progress: Record<string, TaskProgress>;
  workers: Record<string, WorkerView[]>;
  selected: string | null;
}>();
const emit = defineEmits<{ "update:selected": [task: string | null] }>();

const flowId = `run-${props.record.id}`;
const { fitView, onNodesInitialized } = useVueFlow(flowId);

// the workflow of a run does not change: its layout is made once (the parent keys the canvas by the run)
const graph = layout(props.record.workflow);
const specs = new Map<string, TaskSpec>(props.record.workflow.tasks.map((task) => [task.id, task]));
const targets = new Set(graph.edges.map((e) => e.target));
const sources = new Set(graph.edges.map((e) => e.source));

const nodes: Node[] = graph.nodes.map((node) => ({ ...node, type: "task" }));

const edges = computed<Edge[]>(() =>
  graph.edges.map((edge) => {
    const look = edgeLook(props.record.tasks[edge.source], props.record.tasks[edge.target]);
    return {
      ...edge,
      animated: look.animated,
      style: { stroke: look.stroke, strokeWidth: 2, strokeDasharray: look.dash },
      markerEnd: { type: MarkerType.ArrowClosed, color: look.stroke, width: 14, height: 14 },
      labelStyle: { fill: "#6b7280", fontSize: "10px", fontFamily: "var(--font-mono)" },
      labelBgStyle: { fill: "#f9fafb" },
      labelBgPadding: [4, 2] as [number, number],
      labelBgBorderRadius: 4,
    };
  }),
);

const FIT = { padding: 0.14, maxZoom: 1 };

onNodesInitialized(() => void fitView(FIT));

// a changed size of the canvas (the window, the inspector) fits the view again, once per frame
const container = ref<HTMLElement | null>(null);
let frame = 0;
const observer = new ResizeObserver(() => {
  cancelAnimationFrame(frame);
  frame = requestAnimationFrame(() => void fitView(FIT));
});
onMounted(() => {
  if (container.value) observer.observe(container.value);
});
onBeforeUnmount(() => {
  cancelAnimationFrame(frame);
  observer.disconnect();
});

const minimapColor = (node: GraphNode): string => kindOf(String(node.data?.type ?? "")).color;
</script>

<template>
  <div ref="container" class="run-canvas relative min-h-0 flex-1">
    <VueFlow
      :id="flowId"
      :nodes="nodes"
      :edges="edges"
      :min-zoom="0.3"
      :max-zoom="1.6"
      :nodes-connectable="false"
      :nodes-draggable="false"
      :edges-updatable="false"
      :edges-focusable="false"
      :delete-key-code="null"
      class="bg-gray-50"
      @node-click="({ node }) => emit('update:selected', node.id)"
      @pane-click="emit('update:selected', null)"
    >
      <template #node-task="{ id }">
        <TaskNode
          v-if="specs.get(id)"
          :task="specs.get(id)!"
          :state="record.tasks[id] ?? 'waiting'"
          :progress="progress[id] ?? null"
          :workers="workers[id] ?? []"
          :selected="selected === id"
        >
          <Handle v-if="targets.has(id)" type="target" :position="Position.Left" :connectable="false" />
          <Handle v-if="sources.has(id)" type="source" :position="Position.Right" :connectable="false" />
        </TaskNode>
      </template>
      <Background :gap="22" :size="1.3" pattern-color="#d1d5db" />
      <Controls position="bottom-left" :show-interactive="false" />
      <MiniMap position="bottom-right" pannable zoomable :node-color="minimapColor" :width="160" :height="100" />
    </VueFlow>
    <slot />
  </div>
</template>

<style>
/* the canvas of the mockup over the default theme of Vue Flow */
.run-canvas .vue-flow__handle {
  width: 10px;
  height: 10px;
  background: #fff;
  border: 2px solid #94a3b8;
}
.run-canvas .vue-flow__node-task {
  padding: 0;
  border: 0;
  background: transparent;
}
.run-canvas .vue-flow__minimap {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  overflow: hidden;
}
.run-canvas .vue-flow__controls {
  box-shadow: none;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  overflow: hidden;
}
.run-canvas .vue-flow__controls-button {
  border-bottom: 1px solid #f1f5f9;
}
</style>
