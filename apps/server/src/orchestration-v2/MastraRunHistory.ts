import type { MastraPanelRun, MastraPanelSnapshot } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as ProcessRunner from "../processRunner.ts";
import { mastraPanelEnabled, readMastraPanelSnapshot } from "./MastraRunList.ts";

const MAX_RUNS = 20;
const CACHE_MS = 10_000;

/**
 * Fixed read-only script fed to `python3 -I -` on the Mastra host (stdin, never argv), so no
 * run data reaches a shell. It lists workflows, reads one page of runs each, and emits only an
 * allowlist of scalar fields. The Mastra REST API is mini-local, hence the SSH hop.
 */
export const MASTRA_HISTORY_SCRIPT = `import json, re, urllib.request
B = "http://127.0.0.1:4111/api/workflows"
def get(p):
    return json.load(urllib.request.urlopen(B + p, timeout=8))
def scalar(v, n=160):
    if isinstance(v, bool) or isinstance(v, (int, float)):
        return str(v)
    return v[:n] if isinstance(v, str) else None
ALLOW = ["provider", "account", "stopReason", "reason", "status", "mode", "reviewer", "exitCode", "seconds", "durationSeconds", "queuedSeconds", "sha", "project", "commit"]
out = []
try:
    keys = [k for k in get("") if re.fullmatch(r"[A-Za-z0-9_-]{1,80}", k)]
except Exception:
    keys = []
for key in keys:
    try:
        runs = get("/" + key + "/runs?page=0&perPage=20")["runs"]
    except Exception:
        continue
    for r in runs:
        snap = r.get("snapshot") or {}
        res = snap.get("result") if isinstance(snap.get("result"), dict) else {}
        err = snap.get("error")
        msg = err.get("message") if isinstance(err, dict) else err
        receipt = []
        for k in ALLOW:
            v = scalar(res.get(k))
            if v is not None:
                receipt.append({"label": k, "value": v})
        git = res.get("git") if isinstance(res.get("git"), dict) else {}
        for k in ["branch", "commit", "pushed"]:
            v = scalar(git.get(k))
            if v is not None:
                receipt.append({"label": "git." + k, "value": v})
        out.append({
            "workflow": key,
            "runId": scalar(r.get("runId"), 200),
            "status": scalar(snap.get("status"), 40),
            "createdAt": scalar(r.get("createdAt"), 40),
            "updatedAt": scalar(r.get("updatedAt"), 40),
            "provider": scalar(res.get("provider"), 60),
            "model": scalar(res.get("reportedModel") or res.get("model") or res.get("requestedModel"), 80),
            "verdict": scalar(res.get("verdict"), 40),
            "error": scalar(msg, 300),
            "receipt": receipt[:16],
        })
print(json.dumps({"runs": out}))
`;

const RawRun = Schema.Struct({
  workflow: Schema.String,
  runId: Schema.String,
  status: Schema.String,
  createdAt: Schema.String,
  updatedAt: Schema.NullOr(Schema.String),
  provider: Schema.NullOr(Schema.String),
  model: Schema.NullOr(Schema.String),
  verdict: Schema.NullOr(Schema.String),
  error: Schema.NullOr(Schema.String),
  receipt: Schema.Array(Schema.Struct({ label: Schema.String, value: Schema.String })),
});
const RawHistory = Schema.Struct({ runs: Schema.Array(RawRun) });
const decodeHistory = Schema.decodeUnknownOption(Schema.fromJsonString(RawHistory));

const RUN_ID = /^[A-Za-z0-9:_-]{1,200}$/;
const FINISHED = new Set(["success", "failed", "canceled", "cancelled", "tripwire"]);

/** Decode the host script output; newest started first, capped, malformed rows dropped. */
export function decodeMastraRunHistory(text: string): ReadonlyArray<MastraPanelRun> | undefined {
  const decoded = decodeHistory(text);
  if (Option.isNone(decoded)) return undefined;
  const seen = new Set<string>();
  return decoded.value.runs
    .filter((run) => {
      const key = `${run.workflow}/${run.runId}`;
      if (!RUN_ID.test(run.runId) || Number.isNaN(Date.parse(run.createdAt)) || seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    })
    .toSorted((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt))
    .slice(0, MAX_RUNS)
    .map((run) => ({
      workflow: run.workflow,
      runId: run.runId,
      shortId: run.runId.slice(0, 8),
      status: run.status,
      startedAt: run.createdAt,
      endedAt: FINISHED.has(run.status) ? run.updatedAt : null,
      provider: run.provider,
      model: run.model,
      verdict: run.verdict,
      error: run.error,
      receipt: run.receipt,
    }));
}

const SAFE_HOST = /^[A-Za-z0-9][A-Za-z0-9._@:-]{0,252}$/;

const readHistory = Effect.fn("MastraRunHistory.readHistory")(function* () {
  const host = process.env.T3_MASTRA_EVENT_HOST;
  if (host === undefined || host.length === 0) return undefined;
  if (host !== "local" && !SAFE_HOST.test(host)) return undefined;
  const runner = yield* ProcessRunner.ProcessRunner;
  const target =
    host === "local"
      ? { command: "python3", args: ["-I", "-"] }
      : {
          command: "ssh",
          args: ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, "python3 -I -"],
        };
  const output = yield* runner
    .run({
      ...target,
      stdin: MASTRA_HISTORY_SCRIPT,
      env: process.env,
      timeout: "25 seconds",
      maxOutputBytes: 1024 * 1024,
      outputMode: "error",
    })
    .pipe(Effect.orElseSucceed(() => undefined));
  if (output === undefined || output.timedOut || output.stdoutTruncated) return undefined;
  return decodeMastraRunHistory(output.stdout);
});

let cache: { readonly at: number; readonly value: MastraPanelSnapshot } | undefined;

export const readMastraPanel = Effect.fn("MastraRunHistory.readMastraPanel")(function* () {
  const now = yield* DateTime.now;
  const nowMs = DateTime.toEpochMillis(now);
  const nowIso = DateTime.formatIso(now);
  if (!mastraPanelEnabled()) {
    return {
      enabled: false,
      unavailable: false,
      fetchedAt: nowIso,
      runs: [],
      providers: [],
    } satisfies MastraPanelSnapshot;
  }
  if (cache !== undefined && nowMs - cache.at < CACHE_MS) return cache.value;
  const [runs, list] = yield* Effect.all([readHistory(), readMastraPanelSnapshot()], {
    concurrency: 2,
  });
  const value = {
    enabled: true,
    unavailable: runs === undefined && list === undefined,
    fetchedAt: nowIso,
    runs: runs ?? [],
    providers: list?.providers ?? [],
  } satisfies MastraPanelSnapshot;
  // Do not cache a total failure; the next poll should retry at once.
  if (!value.unavailable) cache = { at: nowMs, value };
  return value;
});
