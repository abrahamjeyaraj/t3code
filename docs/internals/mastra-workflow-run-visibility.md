# Mastra workflow run visibility

The Codex adapter recognizes detached Mastra workflow run IDs from direct MCP
tool calls and the installed `functions.exec` bridge result envelope. It
persists monitor ownership in the orchestration EffectOutbox. When
`T3_MASTRA_EVENT_HOST` is explicitly configured, the core server consumes
sanitized run events through the fixed `mastra-mcp --event-command` transport
(local executable for `local`, otherwise BatchMode SSH to the configured host).
The event consumer updates the originating dynamic-tool item without model
calls. The legacy provider-session poller is disabled only in this configured
mode; without the setting it remains the compatibility fallback.

Terminal events contain a fixed outcome summary and allowlisted receipt fields
(verdict, validated branch/commit/PR URL, test-skip count, and exit code), never
the worker prompt, arbitrary result text, environment, or worker identity. The
consumer persists a terminal intent, updates the tool item, and queues a
server-owned message with deterministic event-derived command and message IDs
before acknowledging the broker delivery. The normal orchestration command
receipt/dispatch path makes retries idempotent and queues behind an active user
run instead of interrupting it. A missing or malformed terminal outcome is
rejected by the transport decoder and left unacknowledged for retry; it is not
converted into success or discarded. An unavailable transport likewise leaves
the event pending. The consumer recovers persisted registrations after server
restart and relies on broker redelivery plus stable command identity to retry a
handoff interrupted before acknowledgement.

The event transport is opt-in and requires the Mini-side fixed event command to
be reachable and configured. This local deterministic fixture does not prove
the production Mini broker authenticates clients; production broker
authentication remains a deployment gate. The current event contract exposes
state/stage and bounded receipt details, but no worker identity. Other provider
managed MCP paths are not tracked unless their adapter exposes the same run ID
to the server-owned registration path.

## Nightly patch maintenance

This patch was developed against upstream `pingdotgg/t3code` commit
`7f6f47b27b44456f1c8214d3b566b375c09f629e`. The installed app metadata
reported `0.1.8-nightly.20261104.4192` in the bundle and
`0.1.8-nightly.20261105.4205` in its plist. Keep this page with the adapter and
replay test changes when carrying the patch to a newer nightly.

1. Refresh a clean checkout from `pingdotgg/t3code` and record the new base SHA.
2. Carry the focused adapter, event transport/consumer, outbox, and tests as one
   vertical patch; do not patch installed application bundles.
3. Verify Codex result envelopes, the fixed SSH event-command contract, the
   event schema, and command-receipt idempotency against the refreshed source.
4. Run consumer replay/restart tests, direct and `functions.exec` registration
   tests, terminal failure/cancellation tests, transport tests, and normal
   changed-file checks. Use the `test-t3-app` integrated pass when available.
5. Verify Mini-side broker authentication separately before enabling the
   transport in production. Compare source commit and nightly app version; do
   not replace the user's app as part of patch maintenance without separate
   authorization.
