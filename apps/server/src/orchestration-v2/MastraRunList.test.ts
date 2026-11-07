import { describe, expect, it } from "vite-plus/test";
import { decodeMastraPanelList, mastraToolPayload } from "./MastraRunList.ts";

describe("MastraRunList decoder", () => {
  const list = JSON.stringify({
    activeRuns: [
      { runId: "run_1", workflowId: "worker", state: "running", elapsedSec: 12, provider: "codex" },
      { runId: "bad id!", state: "running" },
    ],
    autoPick: { usageLeft: { codex: "77% (Weekly)" } },
    providers: [{ provider: "codex", health: "ok", usable: true, recent: "16/22 ok", last: { model: "gpt-6-luna" } }],
  });
  it("extracts the tool text for the matching JSON-RPC id", () => {
    const line = JSON.stringify({ jsonrpc: "2.0", id: 2, result: { content: [{ type: "text", text: list }] } });
    expect(mastraToolPayload(`{"jsonrpc":"2.0","id":1,"result":{}}\n${line}\n`, 2)).toBe(list);
  });
  it("keeps safe runs and joins provider health with usage left", () => {
    const snapshot = decodeMastraPanelList(list);
    expect(snapshot?.runs.map((r) => r.runId)).toEqual(["run_1"]);
    expect(snapshot?.providers[0]).toMatchObject({ provider: "codex", usageLeft: "77% (Weekly)", lastModel: "gpt-6-luna" });
  });
  it("rejects malformed payloads", () => {
    expect(decodeMastraPanelList("not json")).toBeUndefined();
  });
});
