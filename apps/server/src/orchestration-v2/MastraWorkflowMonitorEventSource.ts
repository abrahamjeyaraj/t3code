import {
  CommandId,
  MessageId,
  ProviderThreadId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as MastraRunEventTransport from "./MastraRunEventTransport.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

const encodeOutcomeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const safeState = (state: string) =>
  /^[a-z][a-z0-9_-]{0,31}$/i.test(state) ? state.toLowerCase() : "updated";

const safeStage = (stage: string | null | undefined) => {
  if (stage == null) return undefined;
  const normalized = stage.trim().toLowerCase();
  return new Set([
    "queued",
    "starting",
    "planning",
    "research",
    "implementation",
    "implementing",
    "review",
    "testing",
    "test-run",
    "building",
    "complete",
    "completed",
  ]).has(normalized)
    ? normalized
    : undefined;
};

const isTerminal = (event: MastraRunEventTransport.RunEvent) => event.kind === "terminal";
const safeRunId = (runId: string) => (/^[A-Za-z0-9:_-]{1,200}$/.test(runId) ? runId : "unknown");

const itemStatus = (event: MastraRunEventTransport.RunEvent) => {
  if (event.kind !== "terminal") {
    return /^(waiting|blocked|paused)$/i.test(event.state)
      ? ("waiting" as const)
      : ("running" as const);
  }
  if (/^(done|completed|success|succeeded)$/i.test(event.state)) return "completed" as const;
  if (/^(cancelled|canceled|stopped)$/i.test(event.state)) return "cancelled" as const;
  return "failed" as const;
};

const statusText = (event: MastraRunEventTransport.RunEvent) => {
  const stage = safeStage(event.stage);
  return `Mastra workflow ${safeRunId(event.runId)}: ${safeState(event.state)}${stage === undefined ? "" : ` · ${stage}`}`;
};

const safeOutcome = (event: Extract<MastraRunEventTransport.RunEvent, { kind: "terminal" }>) => {
  const outcome = event.outcome;
  const receipt = outcome.receipt;
  return {
    summary: outcome.summary,
    ...(receipt === undefined
      ? {}
      : {
          receipt: {
            ...(receipt.verdict === undefined ? {} : { verdict: receipt.verdict }),
            ...(typeof receipt.branch === "string" &&
            /^[A-Za-z0-9][A-Za-z0-9._/-]{0,199}$/.test(receipt.branch) &&
            !receipt.branch.includes("..")
              ? { branch: receipt.branch }
              : {}),
            ...(typeof receipt.commit === "string" && /^[a-f0-9]{7,40}$/i.test(receipt.commit)
              ? { commit: receipt.commit }
              : {}),
            ...(typeof receipt.prUrl === "string" &&
            receipt.prUrl.length <= 256 &&
            /^https:\/\/github\.com\/[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\/pull\/[1-9][0-9]*$/.test(
              receipt.prUrl,
            )
              ? { prUrl: receipt.prUrl }
              : {}),
            ...(Number.isSafeInteger(receipt.testSkipCount) && (receipt.testSkipCount ?? -1) >= 0
              ? { testSkipCount: receipt.testSkipCount }
              : {}),
            ...(Number.isSafeInteger(receipt.exitCode) ? { exitCode: receipt.exitCode } : {}),
          },
        }),
  };
};

const outcomeText = (outcome: ReturnType<typeof safeOutcome>) => {
  const receipt = outcome.receipt;
  const facts =
    receipt === undefined
      ? []
      : [
          receipt.verdict === undefined ? undefined : `verdict ${receipt.verdict}`,
          receipt.branch === undefined ? undefined : `branch ${receipt.branch}`,
          receipt.commit === undefined ? undefined : `commit ${receipt.commit}`,
          receipt.prUrl === undefined ? undefined : `PR ${receipt.prUrl}`,
          receipt.testSkipCount === undefined
            ? undefined
            : `${receipt.testSkipCount} skipped tests`,
          receipt.exitCode === undefined ? undefined : `exit code ${receipt.exitCode}`,
        ].filter((fact): fact is string => fact !== undefined);
  return `${outcome.summary}${facts.length === 0 ? "" : ` Receipt: ${facts.join("; ")}.`}`;
};

export const make = (input: {
  readonly consumerId: string;
  readonly outbox: Pick<
    EffectOutbox.EffectOutboxV2Shape,
    | "registerMastraWorkflowMonitor"
    | "listMastraWorkflowMonitors"
    | "recordMastraWorkflowTerminalIntent"
  >;
  readonly transport: MastraRunEventTransport.MastraRunEventTransportShape;
  readonly eventSink: Pick<EventSink.EventSinkV2Shape, "commitCommand">;
  readonly projection: Pick<ProjectionStore.ProjectionStoreV2Shape, "getTurnItem">;
  readonly threads: Pick<
    ThreadManagementService.ThreadManagementServiceShape,
    "getThreadProjection" | "sendToThread"
  >;
  readonly ids: {
    readonly allocate: Pick<IdAllocator.IdAllocatorV2["Service"]["allocate"], "event">;
  };
}) => {
  const register: EffectWorker.MastraWorkflowMonitorEventSourceShape["register"] = (registration) =>
    input.outbox
      .registerMastraWorkflowMonitor({
        ...registration,
        threadId: ThreadId.make(registration.threadId),
        runId: RunId.make(registration.runId),
        turnItemId: TurnItemId.make(registration.turnItemId),
        providerThreadId: ProviderThreadId.make(registration.providerThreadId),
        consumerId: input.consumerId,
      })
      .pipe(
        Effect.mapError(
          () =>
            new EffectWorker.MastraWorkflowMonitorEventSourceError({
              message: "Mastra workflow monitor registration could not be persisted.",
            }),
        ),
      );

  const applyDelivery = Effect.fn("MastraWorkflowMonitorEventSource.applyDelivery")(function* (
    delivery: {
      readonly event: MastraRunEventTransport.RunEvent;
      readonly ackToken: string;
    },
    registrations: ReadonlyArray<EffectOutbox.MastraWorkflowMonitorRegistration>,
  ) {
    const event = delivery.event;
    const registration = registrations.find(
      (candidate) =>
        candidate.consumerId === input.consumerId && candidate.mastraRunId === event.runId,
    );
    if (registration === undefined) return;

    const current = yield* input.projection.getTurnItem({
      threadId: registration.threadId,
      itemId: TurnItemId.make(registration.turnItemId),
    });
    if (current === null || current.type !== "dynamic_tool") return;

    const commandDigest = yield* Effect.promise(async () => {
      const digest = await globalThis.crypto.subtle.digest(
        "SHA-256",
        new TextEncoder().encode(event.id),
      );
      return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0"))
        .join("")
        .slice(0, 32);
    });
    const commandId = CommandId.make(`command:mastra-event:${commandDigest}`);
    const occurredAt = DateTime.makeUnsafe(event.at);
    const terminal = isTerminal(event);
    const status = itemStatus(event);
    if (event.kind === "terminal") {
      const outcomeJson = yield* encodeOutcomeJson(safeOutcome(event));
      yield* input.outbox.recordMastraWorkflowTerminalIntent({
        consumerId: input.consumerId,
        eventId: event.id,
        mastraRunId: event.runId,
        threadId: registration.threadId,
        turnItemId: registration.turnItemId,
        state: safeState(event.state),
        stage: safeStage(event.stage) ?? null,
        outcomeJson,
        occurredAt: event.at,
      });
    }
    const updatedItem = {
      ...current,
      status,
      updatedAt: occurredAt,
      ...(terminal ? { completedAt: occurredAt } : {}),
      output: {
        content: [
          {
            type: "text",
            text:
              event.kind === "terminal"
                ? `${statusText(event)} · ${outcomeText(safeOutcome(event))}`
                : statusText(event),
          },
        ],
      },
    };
    const eventId = yield* input.ids.allocate.event({
      threadId: registration.threadId,
      commandId,
    });
    yield* input.eventSink.commitCommand({
      commandId,
      threadId: registration.threadId,
      commandType: "mastra-workflow.event",
      acceptedAt: occurredAt,
      events: [
        {
          id: eventId,
          type: "turn-item.updated",
          threadId: registration.threadId,
          runId: RunId.make(registration.runId),
          occurredAt,
          payload: updatedItem,
        },
      ],
      effects: [],
    });

    if (event.kind === "terminal") {
      const thread = yield* input.threads.getThreadProjection(registration.threadId);
      const wakeCommandId = CommandId.make(`command:mastra-terminal:${commandDigest}`);
      const wakeMessageId = MessageId.make(`message:mastra-terminal:${commandDigest}`);
      const state = safeState(event.state);
      const runId = safeRunId(event.runId);
      const stage = safeStage(event.stage);
      const detail = [
        `Mastra workflow run ${runId} ended with status ${state}${stage === undefined ? "" : ` at stage ${stage}`}.`,
        outcomeText(safeOutcome(event)),
      ].join(" ");
      yield* input.threads.sendToThread({
        projectId: thread.thread.projectId,
        commandId: wakeCommandId,
        threadId: registration.threadId,
        messageId: wakeMessageId,
        text: detail,
        attachments: [],
        mode: "queue",
        createdBy: "agent",
        creationSource: "server",
      });
    }
    yield* input.transport.ack({ consumerId: input.consumerId, ackToken: delivery.ackToken });
  });

  const consumeOnce = Effect.fn("MastraWorkflowMonitorEventSource.consumeOnce")(function* () {
    const registrations = yield* input.outbox.listMastraWorkflowMonitors;
    const deliveries = yield* input.transport.read({ consumerId: input.consumerId, max: 50 });
    for (const delivery of deliveries) {
      yield* applyDelivery(delivery, registrations).pipe(Effect.ignoreCause);
    }
  });

  const run = consumeOnce().pipe(
    Effect.ignoreCause,
    Effect.repeat(Schedule.spaced("1 second")),
    Effect.asVoid,
  );

  return {
    service: EffectWorker.MastraWorkflowMonitorEventSource.of({ register }),
    consumeOnce,
    run,
  };
};

export const layer = Layer.effect(
  EffectWorker.MastraWorkflowMonitorEventSource,
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const transport = yield* MastraRunEventTransport.MastraRunEventTransport;
    const eventSink = yield* EventSink.EventSinkV2;
    const projection = yield* ProjectionStore.ProjectionStoreV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const environment = yield* ServerEnvironment.ServerEnvironmentIdentity;
    const environmentId = yield* environment.getEnvironmentId;
    const consumerId = `t3-${String(environmentId)}`
      .replace(/[^A-Za-z0-9._:-]/g, "-")
      .slice(0, 128);
    const source = make({ consumerId, outbox, transport, eventSink, projection, threads, ids });
    yield* Effect.forkScoped(source.run);
    return source.service;
  }),
);
