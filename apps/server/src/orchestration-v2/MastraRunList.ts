import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as ProcessRunner from "../processRunner.ts";

const RunState = Schema.Literals(["queued", "running", "done", "failed", "cancelled"]);
const MastraActiveRun = Schema.Struct({
  runId: Schema.String,
  workflowId: Schema.optional(Schema.String),
  state: Schema.optional(Schema.String),
  currentStep: Schema.optional(Schema.NullOr(Schema.String)),
  elapsedSec: Schema.optional(Schema.Number),
  startedAt: Schema.optional(Schema.String),
  provider: Schema.optional(Schema.String),
  model: Schema.optional(Schema.String),
});
const MastraProviderHealth = Schema.Struct({
  provider: Schema.String,
  health: Schema.optional(Schema.String),
  usable: Schema.optional(Schema.Boolean),
  recent: Schema.optional(Schema.String),
  models: Schema.optional(Schema.Array(Schema.String)),
  last: Schema.optional(
    Schema.Struct({
      at: Schema.optional(Schema.String),
      model: Schema.optional(Schema.String),
      status: Schema.optional(Schema.String),
    }),
  ),
});
const MastraList = Schema.Struct({
  activeRuns: Schema.optional(Schema.Array(MastraActiveRun)),
  stuckRuns: Schema.optional(Schema.Array(MastraActiveRun)),
  providers: Schema.optional(Schema.Array(MastraProviderHealth)),
  autoPick: Schema.optional(
    Schema.Struct({ usageLeft: Schema.optional(Schema.Record(Schema.String, Schema.String)) }),
  ),
});
const MastraRunStatus = Schema.Struct({
  state: Schema.optional(RunState),
  currentStep: Schema.optional(Schema.NullOr(Schema.String)),
  elapsedSec: Schema.optional(Schema.Number),
});
const ToolText = Schema.Struct({
  result: Schema.Struct({
    content: Schema.Array(Schema.Struct({ type: Schema.String, text: Schema.optional(Schema.String) })),
  }),
});

export type MastraPanelRun = {
  readonly runId: string;
  readonly workflowId: string | null;
  readonly state: "queued" | "running" | "done" | "failed" | "cancelled";
  readonly currentStep: string | null;
  readonly elapsedSec: number | null;
  readonly startedAt: string | null;
  readonly provider: string | null;
  readonly model: string | null;
};
export type MastraPanelSnapshot = {
  readonly runs: ReadonlyArray<MastraPanelRun>;
  readonly providers: ReadonlyArray<{
    readonly provider: string;
    readonly health: string | null;
    readonly usable: boolean | null;
    readonly recent: string | null;
    readonly lastModel: string | null;
    readonly usageLeft: string | null;
  }>;
};

const decodeToolText = Schema.decodeUnknownOption(Schema.fromJsonString(ToolText));
const decodeList = Schema.decodeUnknownOption(Schema.fromJsonString(MastraList));
const decodeStatus = Schema.decodeUnknownOption(Schema.fromJsonString(MastraRunStatus));
const SAFE = /^[A-Za-z0-9:_-]{1,200}$/;
const RESULT_MAX_CHARS = 4_000;

/** Extract the text payload of the MCP JSON-RPC response with the given id from stdout lines. */
export function mastraToolPayload(stdout: string, id: number): string | undefined {
  for (const line of stdout.split("\n")) {
    const decoded = decodeToolText(line);
    if (Option.isNone(decoded)) continue;
    if (!line.includes(`"id":${id}`) && !line.includes(`"id": ${id}`)) continue;
    return decoded.value.result.content.find((entry) => entry.type === "text")?.text;
  }
  return undefined;
}

export function decodeMastraPanelList(text: string): MastraPanelSnapshot | undefined {
  const decoded = decodeList(text);
  if (Option.isNone(decoded)) return undefined;
  const list = decoded.value;
  const usage = list.autoPick?.usageLeft ?? {};
  const seen = new Set<string>();
  const runs = [...(list.activeRuns ?? []), ...(list.stuckRuns ?? [])].flatMap((run) => {
    if (!SAFE.test(run.runId) || seen.has(run.runId)) return [];
    seen.add(run.runId);
    const state = run.state === "queued" || run.state === "running" ? run.state : "running";
    return [
      {
        runId: run.runId,
        workflowId: run.workflowId ?? null,
        state,
        currentStep: run.currentStep ?? null,
        elapsedSec: run.elapsedSec ?? null,
        startedAt: run.startedAt ?? null,
        provider: run.provider ?? null,
        model: run.model ?? null,
      } satisfies MastraPanelRun,
    ];
  });
  return {
    runs,
    providers: (list.providers ?? []).map((p) => ({
      provider: p.provider,
      health: p.health ?? null,
      usable: p.usable ?? null,
      recent: p.recent ?? null,
      lastModel: p.last?.model ?? null,
      usageLeft: usage[p.provider] ?? null,
    })),
  };
}

export function decodeMastraRunState(text: string): MastraPanelRun["state"] | undefined {
  const decoded = decodeStatus(text);
  return Option.isSome(decoded) ? decoded.value.state : undefined;
}

export function mastraPanelEnabled(): boolean {
  const host = process.env.T3_MASTRA_EVENT_HOST;
  return typeof host === "string" && host.length > 0;
}

const MCP_INIT = [
  { jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "t3-mastra-panel", version: "1" } } },
  { jsonrpc: "2.0", method: "notifications/initialized" },
];

/** Read-only call of one Mastra MCP tool through the same host as the event transport. */
export const callMastraTool = Effect.fn("MastraRunList.callMastraTool")(function* (
  name: "mastra_list" | "mastra_status" | "mastra_result",
  args: { readonly runId?: string },
) {
  const host = process.env.T3_MASTRA_EVENT_HOST;
  if (host === undefined || host.length === 0) return undefined;
  if (args.runId !== undefined && !SAFE.test(args.runId)) return undefined;
  if (host !== "local" && !/^[A-Za-z0-9][A-Za-z0-9._@:-]{0,252}$/.test(host)) return undefined;
  const runner = yield* ProcessRunner.ProcessRunner;
  const stdin = [...MCP_INIT, { jsonrpc: "2.0", id: 2, method: "tools/call", params: { name, arguments: args } }]
    .map((m) => JSON.stringify(m))
    .join("\n");
  const target =
    host === "local"
      ? { command: `${process.env.HOME ?? ""}/.local/bin/mastra-mcp`, args: [] as string[] }
      : {
          command: "ssh",
          args: ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, "~/.local/bin/mastra-mcp"],
        };
  const output = yield* runner
    .run({ ...target, stdin: `${stdin}\n`, env: process.env, timeout: "20 seconds", maxOutputBytes: 512 * 1024, outputMode: "error" })
    .pipe(Effect.orElseSucceed(() => undefined));
  if (output === undefined || output.timedOut || output.stdoutTruncated) return undefined;
  return mastraToolPayload(output.stdout, 2);
});

export const readMastraPanelSnapshot = Effect.fn("MastraRunList.snapshot")(function* () {
  const text = yield* callMastraTool("mastra_list", {});
  return text === undefined ? undefined : decodeMastraPanelList(text);
});

export const readMastraRunResult = Effect.fn("MastraRunList.result")(function* (runId: string) {
  const text = yield* callMastraTool("mastra_result", { runId });
  return text === undefined ? undefined : text.slice(0, RESULT_MAX_CHARS);
});
