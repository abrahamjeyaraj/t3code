import { describe, expect, it } from "vite-plus/test";
import { decodeMastraRunHistory, MASTRA_HISTORY_SCRIPT } from "./MastraRunHistory.ts";

const run = (runId: string, createdAt: string, status = "success", extra = {}) => ({
  workflow: "workerWorkflow",
  runId,
  status,
  createdAt,
  updatedAt: "2026-11-07T16:00:45.188Z",
  provider: "codex",
  model: "gpt-6-luna",
  verdict: null,
  error: null,
  receipt: [{ label: "account", value: "primary" }],
  ...extra,
});

describe("decodeMastraRunHistory", () => {
  it("sorts newest first, caps at 20, shortens ids, drops bad rows and duplicates", () => {
    const rows = Array.from({ length: 25 }, (_, i) =>
      run(
        `run-${String(i).padStart(2, "0")}-abcdef`,
        `2026-11-01T00:${String(i).padStart(2, "0")}:00.000Z`,
      ),
    );
    const text = JSON.stringify({
      runs: [
        ...rows,
        run("bad id!", "2026-11-02T00:00:00.000Z"),
        run("run-00-abcdef", "2026-11-01T00:00:00.000Z"),
        run("nodate", "not a date"),
      ],
    });
    const decoded = decodeMastraRunHistory(text);
    expect(decoded).toHaveLength(20);
    expect(decoded?.[0]?.runId).toBe("run-24-abcdef");
    expect(decoded?.[0]?.shortId).toBe("run-24-a");
    expect(decoded?.map((r) => r.runId)).not.toContain("bad id!");
  });
  it("only finished runs carry an end time", () => {
    const decoded = decodeMastraRunHistory(
      JSON.stringify({
        runs: [
          run("r1", "2026-11-07T10:00:00.000Z", "running"),
          run("r2", "2026-11-07T09:00:00.000Z"),
        ],
      }),
    );
    expect(decoded?.map((r) => r.endedAt)).toEqual([null, "2026-11-07T16:00:45.188Z"]);
  });
  it("rejects malformed output instead of showing it as empty", () => {
    expect(decodeMastraRunHistory("not json")).toBeUndefined();
    expect(decodeMastraRunHistory(JSON.stringify({ runs: [{ runId: "x" }] }))).toBeUndefined();
  });
  it("host script never reads the prompt field", () => {
    expect(MASTRA_HISTORY_SCRIPT).not.toContain("prompt");
  });
});
