import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as MastraRunEventTransport from "./MastraRunEventTransport.ts";
import * as ProcessRunner from "../processRunner.ts";

const event = {
  id: "run-1:worker:progress:test-run:running",
  runId: "run-1",
  workflowId: "worker",
  kind: "progress",
  state: "running",
  stage: "test-run",
  at: "2026-11-05T10:00:00Z",
} as const;

const output = (
  stdout: string,
  code = ChildProcessSpawner.ExitCode(0),
  timedOut = false,
): ProcessRunner.ProcessRunOutput => ({
  stdout,
  stderr: "",
  code,
  timedOut,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});

const withRunner = <A, E>(
  run: ProcessRunner.ProcessRunner["Service"]["run"],
  effect: Effect.Effect<A, E, MastraRunEventTransport.MastraRunEventTransport>,
) =>
  effect.pipe(
    Effect.provide(
      MastraRunEventTransport.layer.pipe(
        Layer.provide(
          Layer.succeed(ProcessRunner.ProcessRunner, ProcessRunner.ProcessRunner.of({ run })),
        ),
      ),
    ),
  );

it.effect("reads and acknowledges through the fixed local event command", () => {
  const calls: Array<ProcessRunner.ProcessRunInput> = [];
  const previousHost = process.env.T3_MASTRA_EVENT_HOST;
  process.env.T3_MASTRA_EVENT_HOST = "local";
  return Effect.gen(function* () {
    const transport = yield* MastraRunEventTransport.MastraRunEventTransport;
    const deliveries = yield* transport.read({ consumerId: "t3-env-1", max: 10 });
    yield* transport.ack({ consumerId: "t3-env-1", ackToken: "opaque-token" });
    assert.deepEqual(deliveries, [{ event, ackToken: "opaque-token" }]);
    assert.equal(calls.length, 2);
    assert.deepEqual(calls[0]?.args, ["--event-command"]);
    assert.deepEqual(JSON.parse(calls[0]?.stdin ?? ""), {
      op: "read",
      consumerId: "t3-env-1",
      max: 10,
    });
    assert.deepEqual(JSON.parse(calls[1]?.stdin ?? ""), {
      op: "ack",
      consumerId: "t3-env-1",
      ackToken: "opaque-token",
    });
    assert.equal(calls[0]?.maxOutputBytes, 256 * 1024);
    assert.equal(calls[0]?.timeout, "10 seconds");
  }).pipe(
    Effect.provide(
      MastraRunEventTransport.layer.pipe(
        Layer.provide(
          Layer.succeed(
            ProcessRunner.ProcessRunner,
            ProcessRunner.ProcessRunner.of({
              run: (input) =>
                Effect.sync(() => {
                  calls.push(input);
                  return output(
                    input.stdin?.includes('"op":"read"')
                      ? JSON.stringify({ result: [{ event, ackToken: "opaque-token" }] })
                      : JSON.stringify({ result: { ok: true } }),
                  );
                }),
            }),
          ),
        ),
      ),
    ),
    Effect.ensuring(
      Effect.sync(() => {
        if (previousHost === undefined) delete process.env.T3_MASTRA_EVENT_HOST;
        else process.env.T3_MASTRA_EVENT_HOST = previousHost;
      }),
    ),
  );
});

it.effect("uses validated SSH target and fixed remote command", () => {
  const previousHost = process.env.T3_MASTRA_EVENT_HOST;
  process.env.T3_MASTRA_EVENT_HOST = "mastra-mini";
  const calls: Array<ProcessRunner.ProcessRunInput> = [];
  return withRunner(
    (input) =>
      Effect.sync(() => {
        calls.push(input);
        return output('{"result":[]}');
      }),
    Effect.gen(function* () {
      const transport = yield* MastraRunEventTransport.MastraRunEventTransport;
      yield* transport.read({ consumerId: "env-7", max: 1 });
      assert.equal(calls[0]?.command, "ssh");
      assert.deepEqual(calls[0]?.args, [
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=5",
        "mastra-mini",
        "~/.local/bin/mastra-mcp --event-command",
      ]);
    }),
  ).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        if (previousHost === undefined) delete process.env.T3_MASTRA_EVENT_HOST;
        else process.env.T3_MASTRA_EVENT_HOST = previousHost;
      }),
    ),
  );
});

it.effect("rejects malformed output without exposing subprocess diagnostics", () => {
  const previousHost = process.env.T3_MASTRA_EVENT_HOST;
  process.env.T3_MASTRA_EVENT_HOST = "local";
  return withRunner(
    () => Effect.succeed(output('{"result":[{"bad":"shape"}]}')),
    Effect.gen(function* () {
      const transport = yield* MastraRunEventTransport.MastraRunEventTransport;
      const exit = yield* Effect.exit(transport.read({ consumerId: "env-8", max: 2 }));
      assert.isTrue(exit._tag === "Failure");
      if (Exit.isFailure(exit)) {
        const text = String(exit.cause);
        assert.isTrue(text.includes("invalid JSON or an invalid result"));
        assert.isFalse(text.includes("bad"));
      }
    }),
  ).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        if (previousHost === undefined) delete process.env.T3_MASTRA_EVENT_HOST;
        else process.env.T3_MASTRA_EVENT_HOST = previousHost;
      }),
    ),
  );
});

it.effect("rejects terminal events without the producer's bounded outcome payload", () => {
  const previousHost = process.env.T3_MASTRA_EVENT_HOST;
  process.env.T3_MASTRA_EVENT_HOST = "local";
  const missingOutcome = {
    event: { ...event, kind: "terminal", state: "success" },
    ackToken: "terminal-without-outcome",
  };
  return withRunner(
    () => Effect.succeed(output(JSON.stringify({ result: [missingOutcome] }))),
    Effect.gen(function* () {
      const transport = yield* MastraRunEventTransport.MastraRunEventTransport;
      const exit = yield* Effect.exit(transport.read({ consumerId: "env-terminal", max: 1 }));
      assert.isTrue(exit._tag === "Failure");
    }),
  ).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        if (previousHost === undefined) delete process.env.T3_MASTRA_EVENT_HOST;
        else process.env.T3_MASTRA_EVENT_HOST = previousHost;
      }),
    ),
  );
});

it.effect("reports unavailable SSH and deadline failures with redacted errors", () => {
  const previousHost = process.env.T3_MASTRA_EVENT_HOST;
  process.env.T3_MASTRA_EVENT_HOST = "mastra-mini";
  return Effect.gen(function* () {
    const unavailable = yield* Effect.exit(
      withRunner(
        () =>
          Effect.succeed({
            ...output("", ChildProcessSpawner.ExitCode(255)),
            stderr: "private ssh credential diagnostic",
          }),
        Effect.flatMap(MastraRunEventTransport.MastraRunEventTransport, (transport) =>
          transport.read({ consumerId: "env-9", max: 2 }),
        ),
      ),
    );
    assert.isTrue(Exit.isFailure(unavailable));
    if (Exit.isFailure(unavailable)) {
      assert.isFalse(String(unavailable.cause).includes("private ssh credential diagnostic"));
    }
    const timedOut = yield* Effect.exit(
      withRunner(
        () => Effect.succeed(output("partial", ChildProcessSpawner.ExitCode(0), true)),
        Effect.flatMap(MastraRunEventTransport.MastraRunEventTransport, (transport) =>
          transport.read({ consumerId: "env-9", max: 2 }),
        ),
      ),
    );
    assert.isTrue(Exit.isFailure(timedOut));
    if (Exit.isFailure(timedOut)) {
      assert.isTrue(String(timedOut.cause).includes("timed out"));
    }
  }).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        if (previousHost === undefined) delete process.env.T3_MASTRA_EVENT_HOST;
        else process.env.T3_MASTRA_EVENT_HOST = previousHost;
      }),
    ),
  );
});

it.effect("propagates cancellation to the scoped process runner", () => {
  const previousHost = process.env.T3_MASTRA_EVENT_HOST;
  process.env.T3_MASTRA_EVENT_HOST = "local";
  return Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    let cleanedUp = false;
    const fiber = yield* withRunner(
      () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(Effect.sync(() => (cleanedUp = true))),
        ),
      Effect.flatMap(MastraRunEventTransport.MastraRunEventTransport, (transport) =>
        transport.read({ consumerId: "env-10", max: 2 }),
      ),
    ).pipe(Effect.forkChild({ startImmediately: true }));
    yield* Deferred.await(started);
    yield* Fiber.interrupt(fiber);
    assert.isTrue(cleanedUp);
  }).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        if (previousHost === undefined) delete process.env.T3_MASTRA_EVENT_HOST;
        else process.env.T3_MASTRA_EVENT_HOST = previousHost;
      }),
    ),
  );
});
