import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  ProviderThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as MastraRunEventTransport from "./MastraRunEventTransport.ts";
import * as MastraWorkflowMonitorEventSource from "./MastraWorkflowMonitorEventSource.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

const registration: EffectOutbox.MastraWorkflowMonitorRegistration = {
  effectId: "effect:monitor:1",
  consumerId: "t3-environment-1",
  threadId: ThreadId.make("thread:1"),
  runId: RunId.make("run:1"),
  turnItemId: TurnItemId.make("item:1"),
  providerThreadId: ProviderThreadId.make("provider-thread:1"),
  mastraRunId: "mastra-run-1",
};

const startedAt = DateTime.makeUnsafe("2026-11-05T10:00:00Z");
const item = {
  id: registration.turnItemId,
  threadId: registration.threadId,
  runId: registration.runId,
  nodeId: null,
  providerThreadId: registration.providerThreadId,
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 1,
  status: "running" as const,
  title: "Mastra workflow",
  startedAt,
  completedAt: null,
  updatedAt: startedAt,
  type: "dynamic_tool" as const,
  toolName: "mastra_workflow_detached",
  input: {},
  output: { content: [{ type: "text", text: "starting" }] },
};

const event: MastraRunEventTransport.RunEvent = {
  id: "event-progress-1",
  runId: registration.mastraRunId,
  workflowId: "workerWorkflow",
  kind: "progress",
  state: "running",
  stage: "Review\nprivate-prompt",
  at: "2026-11-05T10:01:00Z",
};

const layerDatabase = SqlitePersistence.layerMemory;
const layerPersistentHandoff = Layer.merge(
  EffectOutbox.layer.pipe(Layer.provideMerge(layerDatabase)),
  CommandReceiptStore.layer.pipe(Layer.provideMerge(layerDatabase)),
);

const processOutput = (
  stdout: string,
  code = ChildProcessSpawner.ExitCode(0),
): ProcessRunner.ProcessRunOutput => ({
  stdout,
  stderr: "",
  code,
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});

const harness = () => {
  const rows = new Map<string, EffectOutbox.MastraWorkflowMonitorRegistration>();
  const terminalIntents = new Map<string, EffectOutbox.MastraWorkflowTerminalIntent>();
  const commits: Array<Parameters<EventSink.EventSinkV2Shape["commitCommand"]>[0]> = [];
  const acks: Array<string> = [];
  const wakeAttempts: Array<
    Parameters<
      import("./ThreadManagementService.ts").ThreadManagementServiceShape["sendToThread"]
    >[0]
  > = [];
  const persistedWakeCommands = new Set<string>();
  let failWakeOnce = false;
  let failAckOnce = false;
  const deliveries: Array<{
    readonly event: MastraRunEventTransport.RunEvent;
    readonly ackToken: string;
  }> = [];
  const outbox = {
    registerMastraWorkflowMonitor: (row: EffectOutbox.MastraWorkflowMonitorRegistration) =>
      Effect.sync(() => {
        const existing = rows.get(row.effectId);
        if (existing !== undefined && JSON.stringify(existing) !== JSON.stringify(row)) {
          throw new Error("conflicting registration");
        }
        rows.set(row.effectId, row);
      }),
    listMastraWorkflowMonitors: Effect.sync(() => [...rows.values()]),
    recordMastraWorkflowTerminalIntent: (intent: EffectOutbox.MastraWorkflowTerminalIntent) =>
      Effect.sync(() => {
        const key = `${intent.consumerId}:${intent.eventId}`;
        const existing = terminalIntents.get(key);
        if (existing !== undefined && JSON.stringify(existing) !== JSON.stringify(intent)) {
          throw new Error("conflicting terminal intent");
        }
        terminalIntents.set(key, intent);
      }),
    listMastraWorkflowTerminalIntents: ({ consumerId }: { readonly consumerId: string }) =>
      Effect.sync(() =>
        [...terminalIntents.values()].filter((intent) => intent.consumerId === consumerId),
      ),
  };
  const transport = {
    read: () => Effect.succeed(deliveries.filter(({ ackToken }) => !acks.includes(ackToken))),
    ack: ({ ackToken }: { readonly consumerId: string; readonly ackToken: string }) =>
      failAckOnce
        ? Effect.sync(() => {
            failAckOnce = false;
            throw new Error("ack unavailable");
          })
        : Effect.sync(() => void acks.push(ackToken)),
  };
  const eventSink = {
    commitCommand: (command: Parameters<EventSink.EventSinkV2Shape["commitCommand"]>[0]) =>
      Effect.sync(() => {
        const committed = !commits.some((stored) => stored.commandId === command.commandId);
        if (committed) commits.push(command);
        return {
          receipt: {} as never,
          storedEvents: [],
          committed,
          cancelledEffectCount: 0,
        };
      }),
  };
  const projection = { getTurnItem: () => Effect.succeed(item) };
  const ids = {
    allocate: {
      event: ({ commandId }: { readonly commandId?: CommandId }) =>
        Effect.succeed(`event:${commandId}` as never),
    },
  };
  const threads = {
    getThreadProjection: () =>
      Effect.succeed({ thread: { projectId: ProjectId.make("project:1") } } as never),
    sendToThread: (input: (typeof wakeAttempts)[number]) => {
      wakeAttempts.push(input);
      if (failWakeOnce) {
        failWakeOnce = false;
        return Effect.die("dispatch unavailable");
      }
      persistedWakeCommands.add(input.commandId);
      return Effect.succeed({} as never);
    },
  };
  return {
    rows,
    commits,
    acks,
    deliveries,
    outbox,
    transport,
    eventSink,
    projection,
    threads,
    ids,
    wakeAttempts,
    persistedWakeCommands,
    setFailWakeOnce: () => (failWakeOnce = true),
    setFailAckOnce: () => (failAckOnce = true),
  };
};

it.effect("persists monitor ownership before polling and recovers it in a new source", () =>
  Effect.gen(function* () {
    const state = harness();
    const first = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
    });
    yield* first.service.register({ ...registration, poll: 0 });
    assert.equal(state.rows.size, 1);

    state.deliveries.push({ event, ackToken: "ack-progress" });
    const restarted = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
    });
    yield* restarted.consumeOnce();
    assert.equal(state.commits.length, 1);
    assert.deepEqual(state.acks, ["ack-progress"]);
    assert.equal(JSON.stringify(state.commits[0]?.events).includes("private-prompt"), false);
  }),
);

it.effect("persists and queues a terminal continuation before acknowledging", () =>
  Effect.gen(function* () {
    const state = harness();
    const source = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
    });
    yield* source.service.register({ ...registration, poll: 0 });
    state.deliveries.push({
      event: {
        ...event,
        kind: "terminal",
        state: "done",
        stage: "complete",
        outcome: {
          summary: "Workflow completed successfully.",
          receipt: {
            verdict: "CLEAN",
            branch: "mastra/fix-visibility",
            commit: "abcdef0123456",
            prUrl: "https://github.com/acme/project/pull/42",
            testSkipCount: 0,
          },
        },
      },
      ackToken: "ack-terminal",
    });
    yield* source.consumeOnce();
    yield* source.consumeOnce();
    assert.equal(state.commits.length, 1);
    assert.deepEqual(state.acks, ["ack-terminal"]);
    assert.equal(state.wakeAttempts.length, 1);
    assert.equal(state.wakeAttempts[0]?.mode, "queue");
    assert.match(state.wakeAttempts[0]?.text ?? "", /Workflow completed successfully/);
    assert.match(state.wakeAttempts[0]?.text ?? "", /verdict CLEAN/);
    assert.match(state.wakeAttempts[0]?.text ?? "", /commit abcdef0123456/);
    assert.match(
      state.wakeAttempts[0]?.text ?? "",
      /https:\/\/github.com\/acme\/project\/pull\/42/,
    );
    assert.isFalse((state.wakeAttempts[0]?.text ?? "").includes("private-prompt"));
    const itemUpdate = state.commits[0]?.events[0];
    assert.isTrue(
      itemUpdate?.type === "turn-item.updated" &&
        itemUpdate.payload.type === "dynamic_tool" &&
        JSON.stringify(itemUpdate.payload.output).includes(
          "https://github.com/acme/project/pull/42",
        ),
    );
    assert.equal(state.persistedWakeCommands.size, 1);
    const intents = yield* state.outbox.listMastraWorkflowTerminalIntents({
      consumerId: registration.consumerId,
    });
    assert.equal(intents.length, 1);
    assert.equal(intents[0]?.eventId, "event-progress-1");
    assert.equal(
      intents[0]?.outcomeJson,
      JSON.stringify({
        summary: "Workflow completed successfully.",
        receipt: {
          verdict: "CLEAN",
          branch: "mastra/fix-visibility",
          commit: "abcdef0123456",
          prUrl: "https://github.com/acme/project/pull/42",
          testSkipCount: 0,
        },
      }),
    );
  }),
);

it.effect("retries a failed handoff after restart without acknowledging early", () =>
  Effect.gen(function* () {
    const state = harness();
    const terminal = {
      ...event,
      kind: "terminal" as const,
      state: "failed",
      stage: "review",
      outcome: { summary: "Workflow failed." as const },
    };
    state.deliveries.push({ event: terminal, ackToken: "ack-retry" });
    state.setFailWakeOnce();
    const first = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
    });
    yield* first.service.register({ ...registration, poll: 0 });
    yield* first.consumeOnce();
    assert.deepEqual(state.acks, []);
    assert.equal(state.persistedWakeCommands.size, 0);
    assert.equal(
      (yield* state.outbox.listMastraWorkflowTerminalIntents({
        consumerId: registration.consumerId,
      })).length,
      1,
    );

    const restarted = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
    });
    yield* restarted.consumeOnce();
    assert.deepEqual(state.acks, ["ack-retry"]);
    assert.equal(state.persistedWakeCommands.size, 1);
    assert.equal(state.wakeAttempts.length, 2);
    assert.equal(state.wakeAttempts[0]?.commandId, state.wakeAttempts[1]?.commandId);
  }),
);

it.effect("redelivers after handoff-before-ack crash using the same durable command identity", () =>
  Effect.gen(function* () {
    const state = harness();
    const terminal = {
      ...event,
      kind: "terminal" as const,
      state: "done",
      outcome: { summary: "Workflow completed successfully." as const },
    };
    state.deliveries.push({ event: terminal, ackToken: "ack-crash" });
    state.setFailAckOnce();
    const first = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
    });
    yield* first.service.register({ ...registration, poll: 0 });
    yield* first.consumeOnce();
    assert.equal(state.persistedWakeCommands.size, 1);
    assert.deepEqual(state.acks, []);

    const restarted = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
    });
    yield* restarted.consumeOnce();
    assert.equal(state.wakeAttempts.length, 2);
    assert.equal(state.wakeAttempts[0]?.commandId, state.wakeAttempts[1]?.commandId);
    assert.equal(state.persistedWakeCommands.size, 1);
    assert.deepEqual(state.acks, ["ack-crash"]);
  }),
);

it.effect("persists terminal intent and command receipt across source recovery", () =>
  Effect.gen(function* () {
    const state = harness();
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    const queuedMessageIds = new Set<string>();
    const threads = {
      ...state.threads,
      sendToThread: (input: (typeof state.wakeAttempts)[number]) =>
        Effect.gen(function* () {
          state.wakeAttempts.push(input);
          const inserted = yield* receipts.insertIfAbsent({
            commandId: CommandId.make(input.commandId),
            threadId: input.threadId,
            commandType: "message.dispatch",
            acceptedAt: yield* DateTime.now,
            resultSequence: 1,
            status: "accepted",
            error: null,
          });
          if (inserted) queuedMessageIds.add(input.messageId);
          return {} as never;
        }).pipe(Effect.orDie),
    };
    state.deliveries.push({
      event: {
        ...event,
        kind: "terminal",
        state: "done",
        outcome: {
          summary: "Workflow completed successfully.",
          receipt: {
            verdict: "CLEAN",
            commit: "abcdef0123456",
            exitCode: 0,
          },
        },
      },
      ackToken: "ack-persistent-replay",
    });
    state.setFailAckOnce();

    const first = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
      outbox,
      threads,
    });
    yield* first.service.register({ ...registration, poll: 0 });
    yield* first.consumeOnce();
    assert.deepEqual(state.acks, []);
    assert.equal(
      (yield* outbox.listMastraWorkflowTerminalIntents({ consumerId: registration.consumerId }))
        .length,
      1,
    );

    const restarted = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
      outbox,
      threads,
    });
    yield* restarted.consumeOnce();

    const commandId = state.wakeAttempts[0]?.commandId;
    assert.isDefined(commandId);
    assert.match(state.wakeAttempts[0]?.text ?? "", /Workflow completed successfully/);
    assert.match(state.wakeAttempts[0]?.text ?? "", /verdict CLEAN/);
    assert.match(state.wakeAttempts[0]?.text ?? "", /commit abcdef0123456/);
    assert.isFalse((state.wakeAttempts[0]?.text ?? "").includes("private-prompt"));
    const receipt = yield* receipts.getByCommandId(CommandId.make(commandId));
    assert.isTrue(receipt._tag === "Some");
    assert.equal(state.wakeAttempts.length, 2);
    assert.equal(queuedMessageIds.size, 1);
    assert.deepEqual(state.acks, ["ack-persistent-replay"]);
  }).pipe(Effect.provide(layerPersistentHandoff)),
);

it.effect("queues one terminal message through ThreadManagementService after replay", () => {
  const previousHost = process.env.T3_MASTRA_EVENT_HOST;
  process.env.T3_MASTRA_EVENT_HOST = "local";
  return Effect.gen(function* () {
    const state = harness();
    const projectId = ProjectId.make("project:mastra-monitor-replay");
    const threadId = registration.threadId;
    const queuedMessages = new Map<
      string,
      { readonly id: MessageId; readonly runId: RunId; readonly text: string }
    >();
    const runs = new Map<string, { readonly id: RunId; readonly status: "queued" }>();
    const dispatchCommandIds: CommandId[] = [];
    const acceptedCommands = new Set<CommandId>();
    const terminalEvent: MastraRunEventTransport.RunEvent = {
      ...event,
      kind: "terminal",
      state: "done",
      outcome: {
        summary: "Workflow completed successfully.",
        receipt: { verdict: "CLEAN", commit: "abcdef0123456", exitCode: 0 },
      },
    };
    let ackCount = 0;
    const ackTokens: string[] = [];
    const transportLayer = MastraRunEventTransport.layer.pipe(
      Layer.provide(
        Layer.succeed(
          ProcessRunner.ProcessRunner,
          ProcessRunner.ProcessRunner.of({
            run: (input) => {
              const command = JSON.parse(input.stdin ?? "") as {
                readonly op: string;
                readonly ackToken?: string;
              };
              if (command.op === "read") {
                return Effect.succeed(
                  processOutput(
                    JSON.stringify({
                      result: [{ event: terminalEvent, ackToken: "ack-production-transport" }],
                    }),
                  ),
                );
              }
              ackCount += 1;
              if (command.ackToken !== undefined) ackTokens.push(command.ackToken);
              return Effect.succeed(
                processOutput(
                  '{"result":{"ok":true}}',
                  ackCount === 1 ? ChildProcessSpawner.ExitCode(1) : undefined,
                ),
              );
            },
          }),
        ),
      ),
    );
    const transport = yield* MastraRunEventTransport.MastraRunEventTransport.pipe(
      Effect.provide(transportLayer),
    );
    const instanceId = ProviderInstanceId.make("codex_work");
    const thread = {
      id: threadId,
      projectId,
      deletedAt: null,
      archivedAt: null,
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex_work"),
        model: "fixture",
      },
    };
    const orchestratorLayer = Layer.mock(Orchestrator.OrchestratorV2)({
      getThreadRecords: () =>
        Effect.sync(
          () =>
            ({
              thread,
              runs: [...runs.values()],
              messages: [...queuedMessages.values()].map((message) => ({
                id: message.id,
                runId: message.runId,
                text: message.text,
                inputIntent: "queued_turn",
              })),
              turnItems: [],
              providerTurns: [],
            }) as never,
        ),
      getThreadProjection: () =>
        Effect.sync(() => ({ thread, runs: [...runs.values()], providerTurns: [] }) as never),
      dispatch: (command: import("@t3tools/contracts").OrchestrationV2ServerCommand) =>
        Effect.sync(() => {
          dispatchCommandIds.push(command.commandId);
          if (command.type === "message.dispatch" && !acceptedCommands.has(command.commandId)) {
            acceptedCommands.add(command.commandId);
            if (!queuedMessages.has(command.messageId)) {
              const runId = RunId.make(`run:${command.commandId}`);
              queuedMessages.set(command.messageId, {
                id: command.messageId,
                runId,
                text: command.text,
              });
              runs.set(runId, { id: runId, status: "queued" });
            }
          }
          return { sequence: 1, storedEvents: [] } as never;
        }),
    });
    const threadManagementLayer = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(orchestratorLayer),
      Layer.provide(
        ServerSettings.layerTest({
          providerInstances: {
            [instanceId]: { driver: "codex", enabled: true },
          },
        }),
      ),
      Layer.provide(layerPersistentHandoff),
    );
    const threads = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(threadManagementLayer),
    );
    const source = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
      transport,
      threads,
    });
    yield* source.service.register({ ...registration, poll: 0 });
    yield* source.consumeOnce();
    assert.equal(queuedMessages.size, 1);
    assert.equal(ackCount, 1);
    const queuedMessage = [...queuedMessages.values()][0];
    assert.isDefined(queuedMessage);
    assert.match(queuedMessage.text, /Workflow completed successfully/);
    assert.match(queuedMessage.text, /verdict CLEAN/);
    assert.isFalse(queuedMessage.text.includes("private-prompt"));

    const restarted = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
      transport,
      threads,
    });
    yield* restarted.consumeOnce();
    assert.equal(queuedMessages.size, 1);
    assert.equal(ackCount, 2);
    assert.deepEqual(ackTokens, ["ack-production-transport", "ack-production-transport"]);
    assert.equal(dispatchCommandIds.length, 2);
    assert.equal(dispatchCommandIds[0], dispatchCommandIds[1]);
    assert.equal([...queuedMessages.keys()][0], queuedMessages.values().next().value?.id);
  }).pipe(
    Effect.provide(layerPersistentHandoff),
    Effect.ensuring(
      Effect.sync(() => {
        if (previousHost === undefined) delete process.env.T3_MASTRA_EVENT_HOST;
        else process.env.T3_MASTRA_EVENT_HOST = previousHost;
      }),
    ),
  );
});

it.effect("leaves pre-registration events unacknowledged for a later registration", () =>
  Effect.gen(function* () {
    const state = harness();
    const source = MastraWorkflowMonitorEventSource.make({
      consumerId: registration.consumerId,
      ...state,
    });
    state.deliveries.push({ event, ackToken: "ack-race" });
    yield* source.consumeOnce();
    assert.equal(state.commits.length, 0);
    assert.deepEqual(state.acks, []);
    yield* source.service.register({ ...registration, poll: 0 });
    yield* source.consumeOnce();
    assert.equal(state.commits.length, 1);
    assert.deepEqual(state.acks, ["ack-race"]);
  }),
);
