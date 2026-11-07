import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fileOf, formatAgo, formatBytes, formatCores, formatDuration, formatGb, formatPlacement, formatTime, initials, shortRun } from "./format";

describe("formatDuration", () => {
  it("names seconds, minutes and hours with minutes", () => {
    expect(formatDuration(38)).toBe("38 s");
    expect(formatDuration(720)).toBe("12 min");
    expect(formatDuration(3900)).toBe("1 h 05 min");
  });

  it("rounds to the next unit without showing 60 of the smaller one", () => {
    expect(formatDuration(0)).toBe("0 s");
    expect(formatDuration(59.7)).toBe("1 min");
    expect(formatDuration(3599)).toBe("1 h 00 min");
    expect(formatDuration(-3)).toBe("0 s");
  });
});

describe("formatBytes", () => {
  it("names bytes with binary units, one decimal below 10", () => {
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(4096)).toBe("4 KB");
    expect(formatBytes(1.5 * 2 ** 20)).toBe("1.5 MB");
    expect(formatBytes(38 * 2 ** 20)).toBe("38 MB");
    expect(formatBytes(2 ** 40)).toBe("1 TB");
  });
});

describe("formatGb", () => {
  it("names GiB as GB, TB from 1024", () => {
    expect(formatGb(0)).toBe("0 GB");
    expect(formatGb(7.75)).toBe("7.8 GB");
    expect(formatGb(15.6)).toBe("16 GB");
    expect(formatGb(0.5)).toBe("0.5 GB");
    expect(formatGb(1024)).toBe("1 TB");
  });
});

describe("formatCores", () => {
  it("drops the decimals of whole cores", () => {
    expect(formatCores(4)).toBe("4");
    expect(formatCores(1.5)).toBe("1.5");
    expect(formatCores(0.33)).toBe("0.3");
  });
});

describe("formatTime", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2026, 10, 7, 16, 30));
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("names the time of today", () => {
    expect(formatTime(new Date(2026, 10, 7, 14, 2).toISOString())).toBe("14:02");
  });

  it("names the day of another day", () => {
    expect(formatTime(new Date(2026, 10, 5, 14, 2).toISOString())).toBe("Nov 5, 14:02");
    expect(formatTime(new Date(2026, 10, 6, 9, 5).toISOString())).toBe("Nov 6, 09:05");
  });
});

describe("initials", () => {
  it("takes the first letters of two words, else the first two letters", () => {
    expect(initials("mk")).toBe("MK");
    expect(initials("Matthias Koenig")).toBe("MK");
    expect(initials("alice")).toBe("AL");
    expect(initials("alice-bot")).toBe("AB");
    expect(initials("")).toBe("?");
  });
});

describe("formatPlacement", () => {
  it("names the pods, the worker processes of a pod and the memory of a pod", () => {
    expect(formatPlacement(1, 1, 6)).toBe("1 pod of 1 worker, 6 GB");
    expect(formatPlacement(1, 8, 12)).toBe("1 pod of 8 workers, 12 GB");
    expect(formatPlacement(4, 36, 180)).toBe("4 pods of 36 workers, 180 GB each");
  });
});

describe("shortRun", () => {
  it("is the last part of the id of a run", () => {
    expect(shortRun("20261107-111319-5f81f7")).toBe("5f81f7");
    expect(shortRun("r9")).toBe("r9");
  });
});

describe("formatAgo", () => {
  const now = new Date("2026-11-07T13:00:00Z");
  it("names how long ago in the largest whole unit", () => {
    expect(formatAgo("2026-11-07T12:59:30Z", now)).toBe("just now");
    expect(formatAgo("2026-11-07T12:55:00Z", now)).toBe("5 min ago");
    expect(formatAgo("2026-11-07T10:00:00Z", now)).toBe("3 h ago");
    expect(formatAgo("2026-11-05T12:00:00Z", now)).toBe("2 d ago");
  });
});

describe("fileOf", () => {
  it("splits a path into its directory and its file name", () => {
    expect(fileOf("/home/mk/git/model/squares.yaml")).toEqual({ dir: "/home/mk/git/model/", name: "squares.yaml" });
    expect(fileOf("squares.yaml")).toEqual({ dir: "", name: "squares.yaml" });
  });
});
