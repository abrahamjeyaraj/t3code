import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ProcessRunner from "../processRunner.ts";

import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as MastraRunEventTransport from "./MastraRunEventTransport.ts";
import * as MastraWorkflowMonitorEventSource from "./MastraWorkflowMonitorEventSource.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const originThreadId = ThreadId.make("thread:mastra-origin-seeded-mvp");
const originRunId = RunId.make("run:mastra-origin-seeded-mvp");
const toolId = TurnItemId.make("turn-item:mastra-origin-seeded-mvp");
const mastraRunId = "bd1fc2d9-f5d9-460f-b4dc-86f7a7e56572";
const events: ReadonlyArray<MastraRunEventTransport.RunEvent> = [
  {
    at: "2026-11-05T17:00:00.000Z",
    id: `${mastraRunId}:workerWorkflow:started:-:running`,
    kind: "started",
    runId: mastraRunId,
    stage: null,
    state: "running",
    workflowId: "workerWorkflow",
  },
  {
    at: "2026-11-05T17:42:55.949Z",
    id: `${mastraRunId}:workerWorkflow:progress:test-run:running`,
    kind: "progress",
    runId: mastraRunId,
    stage: "test-run",
    state: "running",
    workflowId: "workerWorkflow",
  },
  {
    at: "2026-11-05T17:42:55.970Z",
    id: `${mastraRunId}:workerWorkflow:terminal:-:success`,
    kind: "terminal",
    outcome: { summary: "Workflow completed successfully." },
    runId: mastraRunId,
    stage: null,
    state: "success",
    workflowId: "workerWorkflow",
  },
];

const fixtureAdapter = {
  instanceId: ProviderInstanceId.make("codex"),
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("Queued-message proof must not execute a provider session."),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerRegistry = ProviderAdapterRegistry.layerFromAdapters([fixtureAdapter]);

const baseLayer = ProviderReplayHarness.layerWithRegistry<never>(
  { name: "mastra-monitor-real-services" },
  layerRegistry,
  { runEffectWorker: false, realThreadManagement: true },
);
const processRunnerLayer = ProcessRunner.layer.pipe(Layer.provideMerge(NodeServices.layer));
const realTransportLayer = MastraRunEventTransport.layer.pipe(
  Layer.provideMerge(processRunnerLayer),
);
const layer = baseLayer.pipe(Layer.provideMerge(realTransportLayer));

it.layer(layer)("Mastra monitor real-service integration", (it) => {
  it.effect("persists progress and one idempotent queued terminal message", () =>
    Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const projection = yield* ProjectionStore.ProjectionStoreV2;
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const ids = yield* IdAllocator.IdAllocatorV2;
      const now = DateTime.makeUnsafe("2026-11-05T17:00:00.000Z");
      const providerInstanceId = ProviderInstanceId.make("codex");
      const providerSessionId = ProviderSessionId.make("provider-session:mastra-origin-seeded-mvp");
      const providerThreadId = ProviderThreadId.make("provider-thread:mastra-origin-seeded-mvp");
      const projectId = ProjectId.make("project:mastra-origin-seeded-mvp");
      const thread = {
        id: originThreadId,
        projectId,
        title: "Seeded Mastra monitor origin",
        providerInstanceId,
        modelSelection: { instanceId: providerInstanceId, model: "fixture-no-execution" },
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
        branch: null,
        worktreePath: null,
        activeProviderThreadId: providerThreadId,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: originThreadId },
        forkedFrom: null,
        createdBy: "user" as const,
        creationSource: "web" as const,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      };
      const run = {
        id: originRunId,
        threadId: originThreadId,
        ordinal: 1,
        providerInstanceId,
        modelSelection: thread.modelSelection,
        providerThreadId,
        userMessageId: MessageId.make("message:mastra-origin-seeded-mvp"),
        rootNodeId: null,
        activeAttemptId: RunAttemptId.make("run-attempt:mastra-origin-seeded-mvp"),
        status: "running" as const,
        queuePosition: null,
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      };
      const tool = {
        id: toolId,
        type: "dynamic_tool" as const,
        threadId: originThreadId,
        runId: originRunId,
        nodeId: null,
        providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 1,
        status: "running" as const,
        title: "Mastra workflow",
        toolName: "mastra_workflow_detached",
        input: { runId: mastraRunId },
        output: { content: [{ type: "text" as const, text: "starting" }] },
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      };

      yield* eventSink.write({
        events: [
          {
            id: EventId.make("event:mastra-origin-seeded-mvp:thread"),
            type: "thread.created",
            threadId: originThreadId,
            providerInstanceId,
            occurredAt: now,
            payload: thread,
          },
          {
            id: EventId.make("event:mastra-origin-seeded-mvp:provider-thread"),
            type: "provider-thread.updated",
            threadId: originThreadId,
            providerInstanceId,
            occurredAt: now,
            payload: {
              id: providerThreadId,
              driver: ProviderDriverKind.make("codex"),
              providerInstanceId,
              providerSessionId,
              appThreadId: originThreadId,
              ownerNodeId: null,
              nativeThreadRef: null,
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: 1,
              lastRunOrdinal: 1,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            },
          },
          {
            id: EventId.make("event:mastra-origin-seeded-mvp:run"),
            type: "run.created",
            threadId: originThreadId,
            runId: originRunId,
            providerInstanceId,
            occurredAt: now,
            payload: run,
          },
          {
            id: EventId.make("event:mastra-origin-seeded-mvp:tool"),
            type: "turn-item.updated",
            threadId: originThreadId,
            runId: originRunId,
            occurredAt: now,
            payload: tool,
          },
        ],
      });

      const realSmoke = process.env.T3_MASTRA_EVENT_REAL_SMOKE === "1";
      const publish = (event: MastraRunEventTransport.RunEvent) =>
        ProcessRunner.make().pipe(
          Effect.flatMap((publisher) =>
            publisher.run({
              command: `${process.env.HOME ?? ""}/.local/bin/mastra-mcp`,
              args: ["--publish-run-event"],
              env: process.env,
              stdin: JSON.stringify(event),
              timeout: "10 seconds",
              outputMode: "error",
            }),
          ),
          Effect.provide(processRunnerLayer),
          Effect.flatMap((result) =>
            result.code === 0 && !result.timedOut
              ? Effect.void
              : Effect.fail(
                  new MastraRunEventTransport.MastraRunEventTransportError({
                    operation: "read",
                    message: "Mastra event publisher process failed.",
                  }),
                ),
          ),
        );
      const fixtureAcked: string[] = [];
      const fixtureBatches = [
        events.slice(0, 2).map((event) => ({ event, ackToken: `fixture:${event.id}` })),
        [events[2]!].map((event) => ({ event, ackToken: `fixture:${event.id}` })),
        [events[2]!].map((event) => ({ event, ackToken: `fixture:${event.id}` })),
      ];
      let fixtureBatchIndex = 0;
      const fixtureTransport: MastraRunEventTransport.MastraRunEventTransportShape = {
        read: () => Effect.sync(() => fixtureBatches[fixtureBatchIndex++] ?? []),
        ack: ({ ackToken }) => Effect.sync(() => fixtureAcked.push(ackToken)).pipe(Effect.asVoid),
      };
      const transport = realSmoke
        ? yield* MastraRunEventTransport.MastraRunEventTransport
        : fixtureTransport;
      const source = MastraWorkflowMonitorEventSource.make({
        consumerId: "t3-seeded-origin-integration",
        outbox,
        transport,
        eventSink,
        projection,
        threads,
        ids,
      });
      yield* source.service.register({
        effectId: "effect:mastra-origin-seeded-mvp",
        threadId: originThreadId,
        runId: originRunId,
        turnItemId: toolId,
        providerThreadId,
        mastraRunId,
        poll: 0,
      });

      if (realSmoke) {
        assert.equal(process.env.T3_MASTRA_EVENT_HOST, "local");
        yield* publish(events[0]!);
        yield* publish(events[1]!);
      }
      yield* source.consumeOnce();
      const progressTool = yield* projection.getTurnItem({
        threadId: originThreadId,
        itemId: toolId,
      });
      assert.isNotNull(progressTool);
      assert.equal(progressTool?.type, "dynamic_tool");
      if (progressTool?.type === "dynamic_tool") {
        assert.equal(progressTool.status, "running");
        assert.match(JSON.stringify(progressTool.output), /running · test-run/);
      }
      assert.equal(
        (yield* threads.getThreadProjection(originThreadId)).runs.find(
          (candidate) => candidate.id === originRunId,
        )?.status,
        "running",
      );
      if (realSmoke) {
        yield* publish(events[2]!);
        yield* publish(events[2]!);
      }
      yield* source.consumeOnce();
      if (realSmoke) {
        assert.lengthOf(
          yield* transport.read({ consumerId: "t3-seeded-origin-integration", max: 10 }),
          0,
        );
      }
      const completedTool = yield* projection.getTurnItem({
        threadId: originThreadId,
        itemId: toolId,
      });
      assert.equal(completedTool?.type, "dynamic_tool");
      if (completedTool?.type === "dynamic_tool") {
        assert.equal(completedTool.status, "completed");
        assert.match(JSON.stringify(completedTool.output), /Workflow completed successfully/);
      }
      const terminalIntents = yield* outbox.listMastraWorkflowTerminalIntents({
        consumerId: "t3-seeded-origin-integration",
      });
      assert.lengthOf(terminalIntents, 1);
      const firstRecords = yield* threads.getThreadRecords(originThreadId, ["messages", "runs"]);
      const queuedFirst = firstRecords.messages.filter((message) =>
        message.text.includes("Mastra workflow run"),
      );
      assert.lengthOf(queuedFirst, 1);
      const queuedId = queuedFirst[0]!.id;
      const terminalDigest = yield* Effect.promise(async () => {
        const digest = await globalThis.crypto.subtle.digest(
          "SHA-256",
          new TextEncoder().encode(events[2]!.id),
        );
        return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0"))
          .join("")
          .slice(0, 32);
      });
      assert.equal(queuedId, MessageId.make(`message:mastra-terminal:${terminalDigest}`));
      const queuedRun = firstRecords.runs.find(
        (candidate) => candidate.id === queuedFirst[0]!.runId,
      );
      assert.equal(queuedRun?.status, "queued");

      if (!realSmoke) yield* source.consumeOnce();
      const secondRecords = yield* threads.getThreadRecords(originThreadId, ["messages"]);
      const queuedSecond = secondRecords.messages.filter((message) =>
        message.text.includes("Mastra workflow run"),
      );
      assert.lengthOf(queuedSecond, 1);
      assert.equal(queuedSecond[0]!.id, queuedId);
      if (!realSmoke) assert.lengthOf(fixtureAcked, 4);
      assert.equal(
        (yield* outbox.listMastraWorkflowTerminalIntents({
          consumerId: "t3-seeded-origin-integration",
        })).length,
        1,
      );
      assert.equal(
        (yield* threads.getThreadProjection(originThreadId)).runs.find(
          (candidate) => candidate.id === originRunId,
        )?.status,
        "running",
      );
      const evidence = {
        evidence: realSmoke
          ? "seeded-origin real T3 services; replayed real Mastra run through Rust publisher and authenticated JetStream process transport"
          : "seeded-origin real T3 services; self-contained sanitized event fixture transport",
        mastraRunId,
        threadId: originThreadId,
        activeRunId: originRunId,
        toolId,
        progressToolOutput:
          progressTool?.type === "dynamic_tool" ? JSON.stringify(progressTool.output) : null,
        progressCheckpointBeforeTerminal:
          progressTool?.type === "dynamic_tool" ? progressTool.status === "running" : false,
        queuedTerminalMessageId: queuedId,
        terminalAcked: realSmoke || fixtureAcked.includes(`fixture:${events[2]!.id}`),
        queuedTerminalMessages: queuedSecond.length,
        eventIds: events.map((event) => event.id),
        publisherBinary: realSmoke ? `${process.env.HOME ?? ""}/.local/bin/mastra-mcp` : null,
      };
      const evidencePath = process.env.T3_MASTRA_EVENT_EVIDENCE_PATH;
      if (realSmoke && evidencePath !== undefined) {
        const fileSystem = yield* FileSystem.FileSystem;
        yield* fileSystem.writeFileString(evidencePath, `${JSON.stringify(evidence, null, 2)}\n`);
      }
    }).pipe(Effect.scoped),
  );
});
