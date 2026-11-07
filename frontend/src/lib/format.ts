/** The numbers, sizes and times of the app, as the mockup names them. */

const pad = (n: number): string => String(n).padStart(2, "0");

/** A duration: `"38 s"`, `"12 min"`, `"1 h 05 min"`. */
export function formatDuration(seconds: number): string {
  const total = Math.max(0, Math.round(seconds));
  if (total < 60) return `${total} s`;
  const minutes = Math.round(total / 60);
  if (minutes < 60) return `${minutes} min`;
  return `${Math.floor(minutes / 60)} h ${pad(minutes % 60)} min`;
}

const UNITS = ["B", "KB", "MB", "GB", "TB", "PB"];

/** A number of a unit: one decimal below 10, none from 10 on, no trailing `.0`. */
function amount(value: number): string {
  return value < 10 ? String(Math.round(value * 10) / 10) : String(Math.round(value));
}

/** A size in bytes with binary units: `"512 B"`, `"1.5 MB"`, `"38 MB"`. */
export function formatBytes(bytes: number): string {
  let value = Math.max(0, bytes);
  let unit = 0;
  // 1023.6 KB rounds to 1024 KB: such a value is named in the next unit
  while (unit < UNITS.length - 1 && Math.round(value) >= 1024) {
    value /= 1024;
    unit += 1;
  }
  return unit === 0 ? `${Math.round(value)} B` : `${amount(value)} ${UNITS[unit]}`;
}

/** A size in GiB, the unit of the memory of servers and pods: `"0 GB"`, `"7.8 GB"`, `"1 TB"`. */
export function formatGb(gb: number): string {
  return gb >= 1024 ? `${amount(gb / 1024)} TB` : `${amount(Math.max(0, gb))} GB`;
}

/**
 * Where a task runs: its pods, the worker processes of one pod and the memory
 * of one pod, `"1 pod of 8 workers, 12 GB"`, `"4 pods of 36 workers, 180 GB each"`.
 */
export function formatPlacement(pods: number, processes: number, memoryGb: number): string {
  const workers = `${processes} ${processes === 1 ? "worker" : "workers"}`;
  if (pods === 1) return `1 pod of ${workers}, ${formatGb(memoryGb)}`;
  return `${pods} pods of ${workers}, ${formatGb(memoryGb)} each`;
}

/** A number of cores, which a pod may request in fractions: `"4"`, `"1.5"`. */
export function formatCores(cores: number): string {
  return String(Math.round(cores * 10) / 10);
}

/** A time of the local day: `"14:02"` today, `"Nov 5, 14:02"` on another day. */
export function formatTime(iso: string): string {
  const date = new Date(iso);
  const clock = `${pad(date.getHours())}:${pad(date.getMinutes())}`;
  if (date.toDateString() === new Date().toDateString()) return clock;
  return `${date.toLocaleDateString("en-US", { month: "short", day: "numeric" })}, ${clock}`;
}

/** How long ago a time was, in its largest whole unit: `"just now"`, `"5 min ago"`, `"3 h ago"`, `"2 d ago"`. */
export function formatAgo(iso: string, now: Date = new Date()): string {
  const seconds = (now.getTime() - new Date(iso).getTime()) / 1000;
  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} h ago`;
  return `${Math.floor(hours / 24)} d ago`;
}

/** The short name of a run, the last part of its id: `"5f81f7"` of `"20261107-111319-5f81f7"`. */
export function shortRun(id: string): string {
  return id.slice(id.lastIndexOf("-") + 1);
}

/** The directory (with its trailing `/`) and the name of the file of a path. */
export function fileOf(path: string): { dir: string; name: string } {
  const cut = path.lastIndexOf("/") + 1;
  return { dir: path.slice(0, cut), name: path.slice(cut) };
}

/** The initials of a name for an avatar: the first letters of two words, else its first two letters. */
export function initials(name: string): string {
  const words = name.split(/[\s._-]+/).filter((w) => w !== "");
  const letters = words.length >= 2 ? (words[0]?.[0] ?? "") + (words[1]?.[0] ?? "") : (words[0] ?? "").slice(0, 2);
  return letters === "" ? "?" : letters.toUpperCase();
}
