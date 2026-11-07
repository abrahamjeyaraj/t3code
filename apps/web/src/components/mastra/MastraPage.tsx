import type { MastraPanelProvider, MastraPanelRun, MastraPanelSnapshot } from "@t3tools/contracts";
import {
  CheckCircle2Icon,
  ChevronRightIcon,
  CircleDashedIcon,
  LoaderCircleIcon,
  XCircleIcon,
} from "lucide-react";
import { useEffect, useState } from "react";

import { useMastraPanel } from "../../lib/mastraPanelState";
import { cn } from "../../lib/utils";
import {
  formatMastraElapsed,
  formatMastraStarted,
  mastraStatusLabel,
  mastraStatusTone,
  type MastraStatusTone,
} from "./mastraPanel.logic";

const TONE_CLASS: Record<MastraStatusTone, string> = {
  success: "text-success-foreground",
  danger: "text-destructive-foreground",
  running: "text-info-foreground",
  muted: "text-muted-foreground",
};

function StatusIcon({ status }: { readonly status: string }) {
  const tone = mastraStatusTone(status);
  const className = cn("size-4 shrink-0", TONE_CLASS[tone]);
  if (tone === "success") return <CheckCircle2Icon className={className} />;
  if (tone === "danger") return <XCircleIcon className={className} />;
  if (tone === "running") return <LoaderCircleIcon className={cn(className, "animate-spin")} />;
  return <CircleDashedIcon className={className} />;
}

export function MastraProvidersStrip({
  providers,
}: {
  readonly providers: ReadonlyArray<MastraPanelProvider>;
}) {
  if (providers.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-2" data-testid="mastra-providers">
      {providers.map((provider) => (
        <div
          key={provider.provider}
          className="rounded-md border border-border px-2.5 py-1.5 text-xs"
        >
          <div className="flex items-center gap-1.5 font-medium">
            <span
              className={cn(
                "size-2 rounded-full",
                provider.usable === false ? "bg-destructive" : "bg-success",
              )}
            />
            {provider.provider}
            {provider.health !== null ? (
              <span className="text-muted-foreground">{provider.health}</span>
            ) : null}
          </div>
          <div className="text-muted-foreground">
            {provider.usageLeft !== null ? `${provider.usageLeft} left` : "usage unknown"}
            {provider.recent !== null ? ` · ${provider.recent}` : ""}
          </div>
        </div>
      ))}
    </div>
  );
}

function RunDetail({ run }: { readonly run: MastraPanelRun }) {
  return (
    <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 px-9 pb-3 text-xs">
      <dt className="text-muted-foreground">status</dt>
      <dd>{mastraStatusLabel(run.status)}</dd>
      {run.verdict !== null ? (
        <>
          <dt className="text-muted-foreground">verdict</dt>
          <dd>{run.verdict}</dd>
        </>
      ) : null}
      {run.error !== null ? (
        <>
          <dt className="text-muted-foreground">error</dt>
          <dd className="break-words text-destructive-foreground">{run.error}</dd>
        </>
      ) : null}
      {run.receipt.map((field) => (
        <div key={field.label} className="contents">
          <dt className="text-muted-foreground">{field.label}</dt>
          <dd className="break-all">{field.value}</dd>
        </div>
      ))}
      <dt className="text-muted-foreground">run id</dt>
      <dd className="break-all">{run.runId}</dd>
    </dl>
  );
}

export function MastraRunRow({ run, now }: { readonly run: MastraPanelRun; readonly now: number }) {
  const [open, setOpen] = useState(false);
  const elapsed = formatMastraElapsed(run.startedAt, run.endedAt, now);
  const via = [run.provider, run.model].filter((part) => part !== null).join(" / ");
  return (
    <li className="border-b border-border/60" data-testid="mastra-run">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
        className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm hover:bg-accent/50"
      >
        <ChevronRightIcon className={cn("size-3.5 shrink-0 transition-transform", open && "rotate-90")} />
        <StatusIcon status={run.status} />
        <span className="min-w-0 flex-1 truncate">
          <span className="font-medium">{run.workflow}</span>{" "}
          <span className="font-mono text-xs text-muted-foreground">{run.shortId}</span>
          {via.length > 0 ? <span className="text-xs text-muted-foreground"> · {via}</span> : null}
        </span>
        <span className="shrink-0 text-xs text-muted-foreground">
          {formatMastraStarted(run.startedAt)}
          {elapsed !== null ? ` · ${elapsed}` : ""}
        </span>
      </button>
      {open ? <RunDetail run={run} /> : null}
    </li>
  );
}

export function MastraPanelView({
  snapshot,
  now,
}: {
  readonly snapshot: MastraPanelSnapshot;
  readonly now: number;
}) {
  return (
    <div className="flex flex-col gap-4">
      <MastraProvidersStrip providers={snapshot.providers} />
      {snapshot.unavailable ? (
        <p className="text-sm text-muted-foreground">
          The Mastra host did not answer. Retrying every 15 seconds.
        </p>
      ) : snapshot.runs.length === 0 ? (
        <p className="text-sm text-muted-foreground">No runs yet.</p>
      ) : (
        <ul className="rounded-md border border-border" data-testid="mastra-runs">
          {snapshot.runs.map((run) => (
            <MastraRunRow key={`${run.workflow}/${run.runId}`} run={run} now={now} />
          ))}
        </ul>
      )}
    </div>
  );
}

export function MastraPage() {
  const { data, error, isPending } = useMastraPanel();
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1_000);
    return () => clearInterval(id);
  }, []);
  return (
    <div className="flex h-full min-h-0 flex-col overflow-y-auto p-4 sm:p-6">
      <h1 className="mb-4 text-lg font-semibold">Mastra</h1>
      {data === null ? (
        <p className="text-sm text-muted-foreground">
          {error !== null ? `Could not load Mastra: ${error}` : isPending ? "Loading…" : ""}
        </p>
      ) : data.enabled ? (
        <MastraPanelView snapshot={data} now={now} />
      ) : (
        <p className="text-sm text-muted-foreground">Mastra is not configured on this server.</p>
      )}
    </div>
  );
}
