import { describe, expect, it } from "vitest";
import type { Workflow } from "../api/client";
import { layout } from "./graph";

const diamond: Workflow = {
  name: "diamond",
  split: "fastest",
  tasks: [
    { id: "a", type: "demo.squares" },
    { id: "b", type: "demo.squares", inputs: { left: "a.squares" } },
    { id: "c", type: "demo.squares", inputs: { right: "a.squares" } },
    { id: "d", type: "command", inputs: { x: "b.squares", y: "c.out" } },
  ],
};

describe("layout", () => {
  it("places a task one column after the deepest task of its inputs", () => {
    const { nodes } = layout(diamond);
    expect(nodes.map((n) => n.id)).toEqual(["a", "b", "c", "d"]);
    expect(nodes.map((n) => n.position.x)).toEqual([0, 320, 320, 640]);
    const [, b, c] = nodes;
    expect(b?.position.y).not.toBe(c?.position.y);
    expect(Math.abs((b?.position.y ?? 0) - (c?.position.y ?? 0))).toBe(200);
    expect(nodes.map((n) => n.data)).toEqual([
      { taskId: "a", type: "demo.squares" },
      { taskId: "b", type: "demo.squares" },
      { taskId: "c", type: "demo.squares" },
      { taskId: "d", type: "command" },
    ]);
  });

  it("draws an edge per input, labelled with the port it reads", () => {
    const { edges } = layout(diamond);
    expect(edges.map(({ source, target, label }) => ({ source, target, label }))).toEqual([
      { source: "a", target: "b", label: "squares" },
      { source: "a", target: "c", label: "squares" },
      { source: "b", target: "d", label: "squares" },
      { source: "c", target: "d", label: "out" },
    ]);
    expect(new Set(edges.map((e) => e.id)).size).toBe(4);
  });

  it("gives a workflow of one task one node and no edge", () => {
    const { nodes, edges } = layout({ name: "one", split: "fastest", tasks: [{ id: "squares", type: "demo.squares" }] });
    expect(nodes).toEqual([{ id: "squares", position: { x: 0, y: 0 }, data: { taskId: "squares", type: "demo.squares" } }]);
    expect(edges).toEqual([]);
  });

  it("places a task after its inputs whatever their order in the workflow", () => {
    const { nodes } = layout({
      name: "late",
      split: "fastest",
      tasks: [
        { id: "report", type: "command", inputs: { results: "scan.results" } },
        { id: "scan", type: "demo.squares" },
      ],
    });
    expect(nodes.map((n) => [n.id, n.position.x])).toEqual([
      ["report", 320],
      ["scan", 0],
    ]);
  });
});
