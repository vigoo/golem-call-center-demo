# Call Center Agent Development Plan

## Goal

Build a first, measurable version of the direct speech-to-text → language model →
text-to-speech call agent described as Option 1 in the design discussion. The application will
run on Golem using the Effect Golem SDK, Effect AI, Deepgram Flux, OpenAI Responses, and
ElevenLabs streaming TTS. A browser console served by the same Golem application will exercise
the complete pipeline.

This project also serves as an acceptance test for Golem 1.6. When work exposes a Golem defect or
release gap, it will be checked against and then reported to the
[Golem 1.6 finalization](https://linear.app/golem-cloud/project/golem-16-finalization-a019d2a034f9)
Linear project (`P-GOL-22`).

## First-version boundaries

### Included

- One durable `CallAgent(callId)` per call.
- A full-duplex Durable Streams method carrying browser input and agent output.
- Deepgram Flux `/v2/listen` over a Golem WebSocket.
- Committed-turn generation through Effect AI and OpenAI Responses streaming.
- ElevenLabs multi-context streaming TTS over one long-lived WebSocket.
- Explicit response IDs, turn IDs, audio sequence numbers, and playback acknowledgements.
- Barge-in from Deepgram `StartOfTurn` and an explicit browser interrupt action.
- Bounded queues and structured cleanup for all concurrent stages.
- Persistence of committed conversation state and playback progress.
- A same-origin browser console hosted by an ephemeral Golem HTTP router.
- Protocol, state-machine, provider-adapter, integration, and browser tests.
- Local deployment instructions and latency/failure diagnostics.

### Deferred

- Speculative generation from `EagerEndOfTurn`.
- Transparent restoration of live provider WebSocket sessions.
- Byte-identical continuation of interrupted synthesis.
- Server-side transcoding, resampling, echo cancellation, or other DSP.
- Twilio or another telephony gateway.
- Production authentication, retention policy, and privacy controls.
- Production load guarantees.

## Architecture

```text
Browser console (served by Golem)
  ├─ microphone → PCM16/16 kHz/mono → Durable input stream
  ├─ control events: interrupt, playback acknowledgement, hangup
  └─ Durable output stream → transcript/events + PCM playback
                         │
                         ▼
                 CallAgent(callId)
  ┌───────────────────────────────────────────────────────────┐
  │ input consumer ──binary audio──▶ Deepgram Flux           │
  │                                │                          │
  │                       Update / StartOfTurn / EndOfTurn     │
  │                                │                          │
  │                                ▼                          │
  │                         turn controller                   │
  │                                │ committed transcript      │
  │                                ▼                          │
  │              Effect AI LanguageModel.streamText           │
  │                                │ text deltas               │
  │                                ▼                          │
  │              ElevenLabs multi-context WebSocket           │
  │                                │ audio chunks              │
  │                                ▼                          │
  │                         output producer                   │
  └───────────────────────────────────────────────────────────┘
```

The Golem HTTP deployment will expose:

- `/` and `/assets/*` from a parameterless ephemeral `Web` router.
- `/calls/{callId}/run` from the durable `CallAgent`.

Keeping both on one deployment gives the browser same-origin access to Durable Streams without a
CORS exception or a separate development server.

## Core contracts and invariants

### Input events

- `audio(sequence, capturedAt, payload)`
- `playbackAck(responseId, playedThrough)`
- `interrupt`
- `hangup`

### Output events

- `transcript(turnId, text, final)`
- `audio(responseId, sequence, payload)`
- `clearPlayback(responseId)`
- `responseFinished(responseId)`
- `failure(stage, reason, recoverable)`

Schemas are the single source of truth for both the agent boundary and browser protocol. Unknown
provider messages are decoded deliberately: future-compatible fields are tolerated, while malformed
known messages fail with stage context.

The implementation must preserve these invariants:

1. Only an `EndOfTurn` transcript starts an LLM response in v1.
2. `StartOfTurn` while speaking invalidates the active response immediately.
3. At most one response generation is current for a call.
4. Every queued or emitted audio chunk carries the response generation that produced it.
5. Audio from an invalid generation is never emitted or played.
6. Durable delivery is not treated as playback; only `playbackAck` advances heard progress.
7. Every inter-stage queue is bounded. A slow reader produces explicit backpressure rather than
   unbounded memory growth.
8. Provider connections, fibers, queues, and stream readers are scoped and cleaned up on success,
   failure, interruption, and hangup.
9. Completed turns are persisted. An unfinished live turn or response is abandoned and restarted
   with a new ID after failure; provider session continuity is never claimed.
10. Secrets stay in Golem's redacted configuration and never enter logs, errors, snapshots, or
    output events.

## Provider choices

### Deepgram Flux

- Direct WebSocket connection to `/v2/listen`.
- `flux-general-en` initially, configurable for multilingual use.
- Raw `linear16`, 16 kHz, mono audio in approximately 80 ms chunks.
- `Authorization: Token <secret>` supplied only at connection time.
- `Update` is surfaced as a partial transcript.
- `StartOfTurn` is the automatic barge-in signal.
- `EndOfTurn` commits the user turn.
- `EagerEndOfTurn` and `TurnResumed` are decoded but do not trigger generation in v1.

### OpenAI through Effect AI

- `effect@4.0.0` and exact-compatible `@effect/ai-openai@4.0.0`.
- Imports from `effect/ai` and `effect/http`.
- Provider-neutral application dependency on `LanguageModel`.
- `LanguageModel.streamText` filtered to typed `text-delta` parts.
- OpenAI Responses API model and system prompt supplied through typed configuration.

### ElevenLabs

- Direct multi-context WebSocket connection.
- One long-lived socket per call and one context per response ID.
- Incremental text deltas, with bounded phrase grouping only if measurements show that sending raw
  deltas harms speech quality.
- PCM output selected to avoid agent-side transcoding.
- Interrupt closes the obsolete response context while retaining the socket.
- Late messages are rejected by context/response ID even after context closure.

## Browser console

The frontend will be plain HTML, CSS, and JavaScript with no framework or bundler. Static assets
will be provisioned read-only as Golem initial files and served by `HttpRouter.define("Web", ...)`.
The router is parameterless and ephemeral; call state remains exclusively in `CallAgent`.

The console will provide:

- Call ID entry and Start, Interrupt, and Hang Up actions.
- Microphone capture, client-side resampling, and PCM16 encoding.
- Durable Streams session creation, sequenced appends, close, and live output reads.
- Gap-free PCM playback through Web Audio with an explicitly bounded local queue.
- Immediate local queue clearing on `clearPlayback`.
- Playback acknowledgements based on audio actually completed, not merely downloaded or scheduled.
- Partial/final transcript display and assistant response text.
- A diagnostic event log with response IDs, sequence numbers, stage failures, and latency markers.

The local `*.localhost` deployment is a secure context for microphone access. Cloud testing must
use HTTPS.

## Local SDK and dependencies

The published Effect Golem SDK has a known issue, so the application dependency will point to the
local checkout at `../../golem/sdks/effect`. Every `golem` command must also receive the CLI SDK
override:

```nu
$env.GOLEM_EFFECT_GOLEM_PATH = "/Users/vigoo/projects/golem/golem/sdks/effect"
golem build
```

Build and issue reports will record the Golem CLI version and local Golem repository commit. The
SDK itself will not be modified from this application unless investigation proves that the fix
belongs there; cross-repository changes require their own reviewable work.

## Commit-by-commit implementation

Each numbered step ends in a commit. A step is not committed until its stated checks pass, except
when the commit intentionally captures a minimal failing reproduction for a Golem issue. Commit
messages may be refined, but their scope remains as described.

### Step 0 — Capture the generated baseline

Commit the untouched generated Effect streaming application so later changes have a clean base.

- Commit: `chore: capture generated call center scaffold`
- Check: clean Git status after the commit.

### Step 1 — Record the development plan

Add this plan, including architecture, invariants, staged commits, verification, and issue handling.

- Commit: `docs: add call center development plan`
- Check: inspect the rendered Markdown and confirm links and commands.

### Step 2 — Pin the local SDK and establish tests

- Replace the published SDK dependency with the local `file:` dependency.
- Add exact-compatible Effect testing and OpenAI provider dependencies.
- Add the smallest repository-native test configuration and scripts.
- Replace obsolete generated README configuration without yet changing runtime behavior.

- Commit: `build: use local effect golem sdk`
- Checks: dependency tree, TypeScript check, starter tests, and `golem build` with
  `GOLEM_EFFECT_GOLEM_PATH`.

### Step 3 — Define the call protocol and persistent state

- Add Effect schemas for input/output events, IDs, provider stages, turns, and saved call state.
- Add pure transition logic for response creation, interruption, playback acknowledgement, and
  stale-generation rejection.
- Test asymmetric IDs and sequence boundaries so plausible off-by-one and stale-response bugs fail.

- Commit: `feat: define call protocol and state machine`
- Checks: schema round trips and state-machine unit tests.

### Step 4 — Implement the Deepgram Flux adapter

- Add a focused Effect service around the Golem WebSocket API.
- Build and validate the Flux URL and authentication header.
- Send binary audio and control messages.
- Decode connected, turn, warning, configuration, and fatal-error messages with Effect Schema.
- Map provider events to typed transcript/turn events.

- Commit: `feat: add deepgram flux streaming client`
- Checks: adapter tests with a fake socket, malformed-message tests, interruption cleanup, and build.

### Step 5 — Implement streaming LLM generation

- Configure `OpenAiClient` and `OpenAiLanguageModel` using redacted and ordinary typed config.
- Build prompts from committed conversation turns.
- Expose only streamed text deltas and final completion to the coordinator.
- Preserve typed AI failures until mapping them to the call protocol boundary.

- Commit: `feat: stream responses through effect ai`
- Checks: fake `LanguageModel` tests for context ordering, deltas, failure, and cancellation.

### Step 6 — Implement the ElevenLabs adapter

- Add a scoped multi-context WebSocket service.
- Initialize response contexts, stream text, flush/finish, close interrupted contexts, and close the
  socket cleanly.
- Decode base64 provider audio immediately into bytes and reject output for closed contexts.

- Commit: `feat: add elevenlabs streaming tts client`
- Checks: fake socket tests for interleaved contexts, final messages, malformed base64, interruption,
  stale audio, and resource cleanup.

### Step 7 — Build the full-duplex call coordinator

- Connect input consumption, STT receiving, turn handling, LLM generation, TTS input/output, and
  output production under one structured scope.
- Use bounded queues at every push boundary.
- Implement automatic and explicit barge-in with one generation invalidation path.
- Emit stage-specific recoverable failures without leaking credentials.

- Commit: `feat: coordinate the full duplex call pipeline`
- Checks: deterministic fake-provider scenarios for happy path, overlapping speech, slow output,
  provider failure, hangup, and late audio.

### Step 8 — Expose the durable CallAgent

- Replace the generated `StreamingAgent` with `CallAgent(callId)`.
- Expose `run` through input/output Durable Stream slots.
- Save committed turns, counters, and playback progress with schema-driven snapshots.
- Explicitly abandon active provider work on an observable connection failure or restarted run.
- Update `golem.yaml` with typed config, secret defaults, and HTTP deployment wiring.

- Commit: `feat: expose durable call agent streams`
- Checks: typecheck, unit tests, `golem build`, generated metadata/OpenAPI inspection, and local stream
  protocol smoke test.

### Step 9 — Host the browser console from Golem

- Register the ephemeral `Web` router.
- Provision static HTML, JavaScript, and CSS as read-only initial files.
- Serve `/` and `/assets/*` with correct MIME types from the same HTTP deployment.
- Add the diagnostic UI shell before live microphone behavior.

- Commit: `feat: host call console from golem`
- Checks: build/deploy, HTTP status/content-type checks, browser render, accessibility/DOM checks, and
  an inspected screenshot.

### Step 10 — Add browser capture and Durable Streams transport

- Capture microphone frames with an AudioWorklet where supported.
- Resample to 16 kHz mono and encode PCM16 in the browser.
- Create the invocation/session and append sequenced input events idempotently.
- Follow output live with resumable offsets and reconnect-safe cursor handling.
- Implement Start, Interrupt, and Hang Up actions.

- Commit: `feat: stream browser microphone to call agent`
- Checks: browser permission/error states, generated audio fixture input, retry of an uncertain append,
  stream closure, and an inspected active-call UI.

### Step 11 — Add playback and heard-position acknowledgements

- Decode output audio and schedule bounded Web Audio playback.
- Clear scheduled and queued audio on interruption.
- Send acknowledgements only as audio completes.
- Surface first-audio and interruption-to-silence timing.

- Commit: `feat: play and acknowledge streamed responses`
- Checks: ordered playback, stale-generation drop, queue bound, clear behavior, acknowledgement
  boundaries, and an inspected interrupted-call UI.

### Step 12 — Exercise recovery and document the prototype

- Test disconnect/reconnect, slow readers, long calls, provider failures, and interruption during
  STT, LLM, and TTS.
- Verify and document the exact Golem 1.6 behavior observed for WebSocket replay and Durable Streams.
- Replace the generated README with setup, secret provisioning, build/deploy, browser use, known
  limitations, and measurement instructions.
- Record remaining application work separately from Golem release issues.

- Commit: `docs: document call center prototype and findings`
- Checks: full test suite, local-SDK `golem build`, local deploy, browser end-to-end exercise, and
  clean Git status.

## Verification strategy

### On every commit

- Run the narrowest tests that distinguish the changed behavior from plausible wrong behavior.
- Run TypeScript checking.
- Run `golem build` with `GOLEM_EFFECT_GOLEM_PATH` whenever schemas, agent metadata, capabilities,
  dependencies, or the manifest change.
- Inspect the staged diff before committing and leave unrelated files untouched.

### Before declaring the prototype complete

- Run all unit and adapter tests.
- Build and deploy against the local Golem SDK.
- Exercise a real browser call with provider credentials.
- Inspect the rendered default, active, interrupted, and failed UI states.
- Measure end-of-turn to first audio and interruption to silence.
- Test malformed provider messages, provider disconnects, a slow browser reader, cancellation, and
  stale output.
- Restart during STT, LLM, and TTS and document duplicate/missing behavior without overstating
  recovery guarantees.

Live-provider checks are conditional on credentials and network access. Mocked checks must not be
reported as proof of provider compatibility.

## Golem 1.6 issue workflow

When a failure may belong to Golem rather than this application:

1. Reduce it to the smallest deterministic reproduction and preserve complete logs and versions.
2. Confirm the failing path actually uses the local SDK and current Golem CLI/runtime.
3. Separate application misuse, provider behavior, and an SDK/runtime/CLI defect using source,
   protocol evidence, and a focused test.
4. Search all statuses in `P-GOL-22` using the capability, symptom, and affected layer. Read likely
   matches rather than relying only on titles.
5. If an issue exists, add the new reproduction/version evidence there instead of creating a
   duplicate.
6. Phrase reports as general Golem feature requests or bug reports. Do not mention this call-center
   project, its development narrative, or its acceptance-test role; describe the affected Golem
   capability and impact in terms useful to any SDK or application user.
7. If none exists, create an issue in `P-GOL-22` containing:
   - concise expected and actual behavior;
   - CLI/runtime and local Golem commit versions;
   - a standalone minimal reproduction or generally applicable code sample;
   - exact commands and sanitized logs;
   - general user and release impact;
   - suspected layer, clearly marked as a hypothesis until verified.
8. Link the Linear issue from the relevant commit documentation or README findings without adding
   project-specific context back to the Linear report.

The user has authorized reporting discovered Golem 1.6 issues under this workflow. Secrets,
customer audio, and provider credentials must never be attached to an issue.

## Commit discipline

- Do not combine numbered steps into one commit.
- Do not begin the next step with a dirty worktree from the previous step.
- Keep refactors separate from behavior changes when practical.
- If a step reveals a prerequisite, add the smallest prerequisite commit and record why it was
  inserted into this plan.
- If a Golem defect blocks progress, first commit a minimal failing reproduction when useful, report
  it through the workflow above, and then either apply a clearly documented local workaround or stop
  at the blocked boundary.
- Do not rewrite published history. These commits are intended to preserve the development narrative
  for later review and writing.

## Development checkpoint — 2026-10-06

Steps 1–7 are committed. Step 7 passed 59 unit tests, typechecking, a local-SDK build,
Oracle review, and a converged adversarial review. Its tests cover shutdown/backpressure races,
persisted active-response abandonment, and partial provider-acquisition cleanup.

Step 8 is implemented as a **blocked checkpoint**, not an accepted complete step. The isolated
server uses management port 19881, HTTP port 19006, MCP port 19007, and data under
`.amp/in/local-golem/data`. It has an integration deployment with dummy secrets and a `mock`
agent preset pointing at local protocol fixtures on port 19110. Nothing was deployed to cloud.

Observed on the deployed Wasm: session creation, WIT JSON input append, binary socket send,
Deepgram receive, and transcript output succeed. Response work then traps in `current-context`
with a missing span. Reported generically as
[GOL-742](https://linear.app/golem-cloud/issue/GOL-742/effect-streaming-invocation-traps-in-current-context-with-a-missing).
An Oracle-suggested native-tracer experiment avoided the trap but stalled after consuming input,
so it was removed rather than retained as an unsupported workaround. The SDK checkout changed
externally during testing; pin artifact provenance before further runtime investigation.

The imported config-class build limitation already exists as
[GOL-739](https://linear.app/golem-cloud/issue/GOL-739/effect-static-contract-extraction-cannot-resolve-an-imported).
The application uses an exported `defineConfig` value as a local workaround.

Reproduction (Nushell, two terminals; no live credentials required):

```nu
node test/provider-fixture.mjs
```

```nu
with-env { GOLEM_EFFECT_GOLEM_PATH: ($env.HOME | path join projects/golem/golem/sdks/effect) } {
  golem server run
}
```

Once the ports file is present, another terminal:

```nu
with-env { GOLEM_EFFECT_GOLEM_PATH: ($env.HOME | path join projects/golem/golem/sdks/effect) } {
  golem deploy -Y -E integration -P mock
}
node test/golem-smoke.mjs
```

The smoke test deliberately fails until the full response/hangup lifecycle works. Do not treat
mocked STT/TTS as proof of provider compatibility. Deepgram and ElevenLabs credentials were not
available, so live-provider testing remains pending.

Before accepting Step 8: resolve the runtime blocker, rerun the complete smoke test, verify
invocation completion and snapshots/restoration, and add cross-record snapshot consistency
validation raised by Oracle (IDs/counters, response references, active status, and playback bounds).
Steps 9–12 have not begun; continuing with a broken stream endpoint would mask the blocker.

## Development checkpoint — 2026-10-07

The GOL-742 missing-span fix is verified on CLI/server `v1.6.0-rc1-16-g17a2a589f8`
and the local SDK checkout at the same Golem revision. A forced build was necessary to eliminate
previously cached artifacts. No native-tracer workaround is installed.

Step 8 now validates snapshot cross-record invariants on both save and restore, with positive
reducer-produced lifecycle fixtures and negative corruption cases. The metadata schema stays
structural: the static evaluator still rejects block-bodied `Schema.makeFilter` refinements.
Additional evidence was added to the existing
[GOL-736](https://linear.app/golem-cloud/issue/GOL-736/effect-static-contract-extraction-rejects-schemaispattern-regexp),
not filed as a duplicate. The OpenAI SSE fixture was corrected to include required event fields;
its earlier invalid response was an application test-fixture error, not a Golem defect.

Verified on deployed Wasm with protocol fixtures: transcript, generated response audio, response
completion, playback acknowledgement, hangup, output closure, invocation completion, and an
automatic snapshot containing both committed turns and acknowledged playback. After restarting
the isolated server without cleaning its data, the same ended agent recovered and rejected a
new run. Its subsequent snapshot retained both turns, counters, and playback progress. These
checks establish preserved-data recovery but do not independently distinguish the SDK snapshot
restore path from oplog replay; directly observing that path remains an acceptance check.
The smoke supports this check through `GOLEM_TEST_CALL_ID` and
`GOLEM_TEST_EXPECT_ENDED`. All 61 unit tests and typechecking pass. Oracle reviewed the changes;
the bounded adversarial review found no application bugs.

The local memory override is now 4GiB. With 2GiB the combined server reported about 1.77GiB RSS
at startup, exceeding its 1.60GiB worker-admission budget; that produced additional queued/Idle
tests and obscured diagnosis. This configuration change is **not** a fix for the stream stall below.

Step 8 remains a **blocked checkpoint** because live input delivery intermittently stalls even
with 4GiB and RSS below 1GiB. Successful calls are not evidence that repeated calls are reliable.
A reduced string-echo agent with two pending WebSocket receive fibers reproduced the stall on
the second fresh invocation. The host records `ConsumerItemValue` but no guest input-handler
marker or binary send follows. No missing-span trap, crash, replay, or explicit suspension is observed; the exact
root cause is not established. After duplicate-checking the release project, reported generically as
[GOL-750](https://linear.app/golem-cloud/issue/GOL-750/concurrent-websocket-receives-can-stall-live-durable-stream-input).
The runnable reduced reproduction is in `test/stream-stall-repro/`.

Steps 9–12 remain pending rather than hiding this failure behind a browser UI. Resume with repeated
smoke/reduced-probe verification once GOL-750 is resolved. Live Deepgram/ElevenLabs credentials
are still unavailable; protocol-fixture results do not establish live provider compatibility.

## Step 8 acceptance — 2026-10-08

Verified GOL-750 fixed on CLI/runtime `v1.6.0-rc1-28-g5948c58e5f` with the freshly rebuilt
local Effect SDK. Ten fresh reduced-probe invocations pass echo **and output closure**; seven
full call invocations pass transcript, audio, response completion, acknowledgement and hangup.
Invocation completion and saved turns/playback were inspected in the oplog. After a same-version
server restart, the ended call rejects reuse and closes output. The `Call snapshot restored`
marker in its oplog confirms the SDK's restore strategy ran (`phase=ended`, `turns=2`), not merely
equivalent behavior reconstructed by replay. Step 8 is accepted; frontend work can proceed.

The older development-version data directory caused a compressed-oplog decode panic at startup,
reported generically as [GOL-797](https://linear.app/golem-cloud/issue/GOL-797/a-compressed-oplog-decode-failure-can-panic-the-local-server-during).
It is preserved, not cleaned. Current testing uses `.amp/in/local-golem/data-current`, with the
same isolated ports. Compatibility between unreleased development commits is not claimed.
The CLI refreshed its generated guidance; Playwright is installed as development-only browser
verification tooling. No changes were made to the SDK checkout.
