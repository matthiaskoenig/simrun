/**
 * The look of a task type on the canvas, the `KINDS` of the mockup: the icon
 * and the color of the square of a node and of the node in the minimap.
 *
 * The types the app knows get their own; any other type of a plugin gets a
 * color of the mockup's kinds by a hash of its name, so that it keeps it from
 * run to run.
 */
import type { IconName } from "./icons";

/** The icon and the color of a task type. */
export interface Kind {
  icon: IconName;
  color: string;
}

const KNOWN: Record<string, Kind> = {
  command: { icon: "terminal", color: "#334155" },
  "demo.squares": { icon: "layers", color: "#4f46e5" },
};

// the colors of the analyses of the mockup
const COLORS = ["#0284c7", "#4f46e5", "#7c3aed", "#0891b2", "#d97706", "#db2777", "#059669", "#0d9488"];

/** The look of a task type. */
export function kindOf(type: string): Kind {
  const known = KNOWN[type];
  if (known) return known;
  let hash = 0;
  for (const char of type) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return { icon: "box", color: COLORS[hash % COLORS.length] ?? "#94a3b8" };
}
