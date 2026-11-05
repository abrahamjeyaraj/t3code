import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as ProcessRunner from "../processRunner.ts";

const RunOutcomeReceipt = Schema.Struct({
  verdict: Schema.optional(Schema.Literals(["CLEAN", "BLOCKING", "ADVISORY", "UNPARSED"])),
  branch: Schema.optional(Schema.String),
  commit: Schema.optional(Schema.String),
  prUrl: Schema.optional(Schema.String),
  testSkipCount: Schema.optional(Schema.Number),
  exitCode: Schema.optional(Schema.Number),
});
const RunOutcome = Schema.Struct({
  summary: Schema.Literals([
    "Workflow completed successfully.",
    "Workflow failed.",
    "Workflow was cancelled.",
    "Workflow ended without a successful result.",
  ]),
  receipt: Schema.optional(RunOutcomeReceipt),
});

const RunEventFields = {
  id: Schema.String,
  runId: Schema.String,
  workflowId: Schema.String,
  state: Schema.String,
  stage: Schema.optional(Schema.NullOr(Schema.String)),
  at: Schema.String,
};
const RunEvent = Schema.Union([
  Schema.Struct({ ...RunEventFields, kind: Schema.Literal("started") }),
  Schema.Struct({ ...RunEventFields, kind: Schema.Literal("progress") }),
  Schema.Struct({ ...RunEventFields, kind: Schema.Literal("terminal"), outcome: RunOutcome }),
]);
export type RunEvent = typeof RunEvent.Type;

const EventDelivery = Schema.Struct({
  event: RunEvent,
  ackToken: Schema.String,
});

const ReadResult = Schema.fromJsonString(Schema.Struct({ result: Schema.Array(EventDelivery) }));
const AckResult = Schema.fromJsonString(
  Schema.Struct({ result: Schema.Struct({ ok: Schema.Boolean }) }),
);
const encodeCommand = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeReadResult = Schema.decodeEffect(ReadResult);
const decodeAckResult = Schema.decodeEffect(AckResult);

export class MastraRunEventTransportError extends Schema.TaggedError<MastraRunEventTransportError>()(
  "MastraRunEventTransportError",
  { operation: Schema.Literals(["configuration", "read", "ack"]), message: Schema.String },
) {}

export interface MastraRunEventTransportShape {
  readonly read: (input: {
    readonly consumerId: string;
    readonly max: number;
  }) => Effect.Effect<
    ReadonlyArray<{ readonly event: RunEvent; readonly ackToken: string }>,
    MastraRunEventTransportError
  >;
  readonly ack: (input: {
    readonly consumerId: string;
    readonly ackToken: string;
  }) => Effect.Effect<void, MastraRunEventTransportError>;
}

export class MastraRunEventTransport extends Context.Service<
  MastraRunEventTransport,
  MastraRunEventTransportShape
>()("t3/orchestration-v2/MastraRunEventTransport") {}

const COMMAND = "~/.local/bin/mastra-mcp --event-command";
const MAX_STDOUT_BYTES = 256 * 1024;
const COMMAND_TIMEOUT = "10 seconds";

const configuredHost = (): Effect.Effect<
  { readonly command: string; readonly args: ReadonlyArray<string> },
  MastraRunEventTransportError
> => {
  const host = process.env.T3_MASTRA_EVENT_HOST;
  if (host === undefined || host.length === 0) {
    return Effect.fail(
      new MastraRunEventTransportError({
        operation: "configuration",
        message: "Mastra event transport is disabled; set T3_MASTRA_EVENT_HOST explicitly.",
      }),
    );
  }
  if (host === "local") {
    return Effect.succeed({
      command: `${process.env.HOME ?? ""}/.local/bin/mastra-mcp`,
      args: ["--event-command"],
    });
  }
  if (!/^[A-Za-z0-9][A-Za-z0-9._@:-]{0,252}$/.test(host)) {
    return Effect.fail(
      new MastraRunEventTransportError({
        operation: "configuration",
        message: "Configured Mastra SSH host is invalid.",
      }),
    );
  }
  return Effect.succeed({
    command: "ssh",
    args: ["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, COMMAND],
  });
};

export const layer = Layer.effect(
  MastraRunEventTransport,
  Effect.gen(function* () {
    const runner = yield* ProcessRunner.ProcessRunner;
    const execute = Effect.fn("MastraRunEventTransport.execute")(function* (
      operation: "read" | "ack",
      command: unknown,
    ) {
      const target = yield* configuredHost();
      const stdin = yield* encodeCommand(command).pipe(
        Effect.mapError(
          () =>
            new MastraRunEventTransportError({
              operation,
              message: "Mastra event command could not be encoded.",
            }),
        ),
      );
      const output = yield* runner
        .run({
          ...target,
          stdin,
          env: process.env,
          timeout: COMMAND_TIMEOUT,
          maxOutputBytes: MAX_STDOUT_BYTES,
          outputMode: "error",
        })
        .pipe(
          Effect.mapError(
            () =>
              new MastraRunEventTransportError({
                operation,
                message: "Mastra event command could not be completed.",
              }),
          ),
        );
      if (output.timedOut || output.code !== 0 || output.stdoutTruncated) {
        return yield* new MastraRunEventTransportError({
          operation,
          message: output.timedOut
            ? "Mastra event command timed out."
            : "Mastra event command failed or exceeded its output limit.",
        });
      }
      return output.stdout;
    });

    const validateConsumer = (consumerId: string) =>
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(consumerId)
        ? Effect.void
        : Effect.fail(
            new MastraRunEventTransportError({
              operation: "configuration",
              message: "Mastra event consumer identity is invalid.",
            }),
          );

    return MastraRunEventTransport.of({
      read: Effect.fn("MastraRunEventTransport.read")(function* ({ consumerId, max }) {
        yield* validateConsumer(consumerId);
        if (!Number.isInteger(max) || max < 1 || max > 100) {
          return yield* new MastraRunEventTransportError({
            operation: "read",
            message: "Mastra event read batch size must be between 1 and 100.",
          });
        }
        const text = yield* execute("read", { op: "read", consumerId, max });
        return yield* decodeReadResult(text).pipe(
          Effect.map((decoded) => decoded.result),
          Effect.mapError(
            () =>
              new MastraRunEventTransportError({
                operation: "read",
                message: "Mastra event command returned invalid JSON or an invalid result.",
              }),
          ),
        );
      }),
      ack: Effect.fn("MastraRunEventTransport.ack")(function* ({ consumerId, ackToken }) {
        yield* validateConsumer(consumerId);
        if (ackToken.length === 0 || ackToken.length > 4096) {
          return yield* new MastraRunEventTransportError({
            operation: "ack",
            message: "Mastra event acknowledgement token is invalid.",
          });
        }
        const text = yield* execute("ack", { op: "ack", consumerId, ackToken });
        const decoded = yield* decodeAckResult(text).pipe(
          Effect.mapError(
            () =>
              new MastraRunEventTransportError({
                operation: "ack",
                message: "Mastra event command returned invalid JSON or an invalid result.",
              }),
          ),
        );
        const result = decoded.result;
        if (!result.ok) {
          return yield* new MastraRunEventTransportError({
            operation: "ack",
            message: "Mastra event acknowledgement was rejected.",
          });
        }
      }),
    });
  }),
);
