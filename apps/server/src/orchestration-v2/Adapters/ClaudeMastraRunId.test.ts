import { afterEach, describe, expect, it } from "vite-plus/test";
import { claudeMastraWorkflowRunId } from "./ClaudeAdapterV2.ts";

const output = (text: string) =>
  ({ type: "content_block", value: [{ type: "text", text }] }) as never;

describe("claudeMastraWorkflowRunId", () => {
  const previous = process.env.T3_MASTRA_EVENT_HOST;
  afterEach(() => {
    if (previous === undefined) delete process.env.T3_MASTRA_EVENT_HOST;
    else process.env.T3_MASTRA_EVENT_HOST = previous;
  });
  it("reads the run id from a mastra workflow MCP result when the event host is set", () => {
    process.env.T3_MASTRA_EVENT_HOST = "macmini";
    expect(
      claudeMastraWorkflowRunId("mcp__mastra__mastra_workflow_worker", output('{"runId":"run_abc"}')),
    ).toBe("run_abc");
    expect(
      claudeMastraWorkflowRunId("mcp__mastra__mastra_workflow_worker", { type: "content_block", value: '{"runId":"run_s"}' } as never),
    ).toBe("run_s");
  });
  it("ignores other tools and a missing event host", () => {
    process.env.T3_MASTRA_EVENT_HOST = "macmini";
    expect(claudeMastraWorkflowRunId("mcp__mastra__mastra_status", output('{"runId":"r"}'))).toBeUndefined();
    delete process.env.T3_MASTRA_EVENT_HOST;
    expect(claudeMastraWorkflowRunId("mcp__mastra__mastra_workflow_worker", output('{"runId":"r"}'))).toBeUndefined();
  });
});
