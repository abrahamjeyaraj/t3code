import * as Schema from "effect/Schema";

/** Read-only view of Mastra workflow runs and provider health, fetched by the server. */
export const MastraPanelRun = Schema.Struct({
  workflow: Schema.String,
  runId: Schema.String,
  shortId: Schema.String,
  status: Schema.String,
  startedAt: Schema.String,
  endedAt: Schema.NullOr(Schema.String),
  provider: Schema.NullOr(Schema.String),
  model: Schema.NullOr(Schema.String),
  verdict: Schema.NullOr(Schema.String),
  error: Schema.NullOr(Schema.String),
  receipt: Schema.Array(Schema.Struct({ label: Schema.String, value: Schema.String })),
});
export type MastraPanelRun = typeof MastraPanelRun.Type;

export const MastraPanelProvider = Schema.Struct({
  provider: Schema.String,
  health: Schema.NullOr(Schema.String),
  usable: Schema.NullOr(Schema.Boolean),
  recent: Schema.NullOr(Schema.String),
  lastModel: Schema.NullOr(Schema.String),
  usageLeft: Schema.NullOr(Schema.String),
});
export type MastraPanelProvider = typeof MastraPanelProvider.Type;

export const MastraPanelSnapshot = Schema.Struct({
  /** False when T3_MASTRA_EVENT_HOST is unset; the panel stays hidden. */
  enabled: Schema.Boolean,
  /** True when the host was configured but neither source answered. */
  unavailable: Schema.Boolean,
  fetchedAt: Schema.String,
  runs: Schema.Array(MastraPanelRun),
  providers: Schema.Array(MastraPanelProvider),
});
export type MastraPanelSnapshot = typeof MastraPanelSnapshot.Type;
