import type { MastraPanelSnapshot } from "@t3tools/contracts";
import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vite-plus/test";

import { MastraPanelView } from "./MastraPage";

const snapshot: MastraPanelSnapshot = {
  enabled: true,
  unavailable: false,
  fetchedAt: "2026-11-07T10:05:00.000Z",
  providers: [
    { provider: "codex", health: "ok", usable: true, recent: "16/22 ok", lastModel: null, usageLeft: "77% (Weekly)" },
  ],
  runs: [
    {
      workflow: "workerWorkflow",
      runId: "b4603e45-c53b-4bbc-9240-2e309d63d379",
      shortId: "b4603e45",
      status: "failed",
      startedAt: "2026-11-07T10:00:00.000Z",
      endedAt: "2026-11-07T10:03:12.000Z",
      provider: "codex",
      model: "gpt-6-luna",
      verdict: null,
      error: "codex stopped with cancelled",
      receipt: [{ label: "account", value: "primary" }],
    },
  ],
};

const now = Date.parse("2026-11-07T10:05:00.000Z");

it("shows the providers strip and a collapsed run row", () => {
  const html = renderToStaticMarkup(<MastraPanelView snapshot={snapshot} now={now} />);
  expect(html).toContain("77% (Weekly) left");
  expect(html).toContain("workerWorkflow");
  expect(html).toContain("b4603e45");
  expect(html).toContain("3m 12s");
  expect(html).not.toContain("codex stopped with cancelled");
});

it("reports an unreachable host and an empty history", () => {
  expect(
    renderToStaticMarkup(
      <MastraPanelView snapshot={{ ...snapshot, runs: [], providers: [], unavailable: true }} now={now} />,
    ),
  ).toContain("did not answer");
  expect(
    renderToStaticMarkup(<MastraPanelView snapshot={{ ...snapshot, runs: [] }} now={now} />),
  ).toContain("No runs yet");
});
