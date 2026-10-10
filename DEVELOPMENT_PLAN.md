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
- `assistantText(responseId, text)` after synthesis finalizes
- `interruptConfirmed` after an explicit interrupt (coalesced while pending)
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

## Step 9 acceptance — 2026-10-08

The static-only ephemeral Web router serves read-only initial HTML/CSS/JS files on the call API
origin. Local-SDK build/deploy, browser MIME/404 checks, desktop/mobile rendering and DOM checks
pass. Screenshots were inspected. Oracle found no Step 9 blockers. Golem reserves Cache-Control
for its file responses, so the router leaves caching to the platform rather than overriding it.
Capture and playback remain disabled in this deliberately separate UI-shell commit.

## Step 10 acceptance — 2026-10-08

AudioWorklet capture produces 80 ms PCM16/16 kHz mono frames with continuous resampling and a
browser low-pass filter. One bounded producer retries uncertain appends with unchanged identity;
output reads retry interrupted bodies at the same opaque cursor. The deployed Chrome test uses
fake microphone input but real Golem streams and provider fixtures: accepted append reply loss,
transcript, interrupt, confirmed hangup, permission denial, delayed hangup reply and late permission
after disposal pass. All 67 tests pass. Oracle's lifecycle/retry findings were fixed and accepted.
Capture is not a claim of live provider or high-quality DSP validation. Playback follows in Step 11.

## Step 11 acceptance — 2026-10-08

PCM playback schedules at most 64 sources / 8 seconds; overload stops the call rather than
blocking a later interrupt behind capacity waits. Only contiguous natural onended completion
advances heard progress; stopped/invalidated audio is never acknowledged. Explicit local
interrupt mutes in-flight audio until the agent's coalesced confirmation. Completed assistant
text is emitted after successful synthesis finalization, not as a speculative text stream.
The wire additions do not change snapshots. Unit tests (70), typecheck, deployed stream smoke,
and Chrome capture/playback/ACK/interrupt tests pass. Oracle accepted the functional change and
required honest browser-proxy timing labels, now applied. Adversarial review found no bugs.
Desktop/mobile, active, interrupted and failed screenshots were inspected; acoustic latency and
live-provider speech quality remain unmeasured.

## Step 12 recovery checkpoint — 2026-10-08

The 70-test suite, typecheck, local-SDK deployment, stream smoke, ten fresh reduced echo probes
and a 30-second Chrome fake-microphone call pass. The browser verifies an accepted playback ACK,
uncertain append retry, capture continuity, explicit interrupt, hangup, denied permission and
late permission after disposal. Final screenshots were inspected. Transcript/event panels scroll
when the sustained call exceeds their visible height.

The new serial recovery smoke passes delayed reading (1.5 seconds), stage-specific failures, and
explicit interruption during STT, OpenAI and ElevenLabs. Fixture hold/release handshakes prove the
nominated stage is pending when interrupted/crashed; scenario-local controls and resource disposal
isolate tests. LLM/TTS interruption releases obsolete output and rejects late audio/completion,
including during the final drain. STT interruption precedes committed speech, so the test hangs
up before releasing its held turn rather than incorrectly suppressing
future speech. Small fixture tests and a 30-second call are not production backpressure/load
or arbitrarily long-call guarantees.

Simulate-crash during all three stages stalls for the 20-second recovery deadline. The reduced
duplex string-echo agent reproduces this without AI, custom snapshots or a coordinator: after
the first echo, crash/replay accepts the next input append but emits no second echo/closure.
After duplicate-checking Golem 1.6 finalization, reported generically as
[GOL-799](https://linear.app/golem-cloud/issue/GOL-799/effect-duplex-agentstream-replay-stalls-after-simulate-crash-with).
Root cause remains unestablished. Both reproductions retain failing assertions; the recovery
suite is **not green**. Logs and event captures are retained under `.amp/in/`.

Oracle reviewed Step 12 and its follow-up. Harness findings were addressed with deterministic
stage gating, separate recovery deadlines, interval capture checks, and assertions rejecting
premature closure, late failures and stale response output through the final drain. The review
permits a blocked checkpoint with those corrections, not full recovery acceptance.

README now documents isolated setup, local SDK override, mock versus live secrets, browser use,
wire protocol, queue failure policy, proxy timings and release findings. Step 12 is a reviewable
**blocked acceptance checkpoint**, not a claim that crash recovery or live providers work.
Resume by verifying GOL-799's fix with both reproductions and obtaining Deepgram/ElevenLabs
credentials for live speech/latency checks. No SDK changes, cloud deploy, push or merge occurred.

## Follow-up — distinguish live and mock presets

A user test revealed that `local` still used fixture URLs even without `-P mock`. Deployed
component metadata confirmed this. CLI source inspection established that the first custom agent
preset is implicitly selected when none is specified; our lone `mock` preset was therefore the
default. This was an application manifest mistake, not a Golem defect.

Added an explicit empty `live` default and selected `mock` in the integration environment.
README now uses `-P live` to override shell preset selection too, and warns that existing call
agents retain their old configuration. Local CLI deployment plans verify all three live provider
URLs without a preset, while `-P mock` retains all three fixture URLs. Unit tests and typecheck
pass. No active agents are reset or deleted, and no live provider invocation is made by this fix.

## Follow-up — live synthesis and free-plan voice access

User testing confirmed recognition and LLM replies but no audio. The call's provider messages
contained final markers with no audio. A temporary CLI-only Golem agent, using the existing
opaque secret, established that Rachel (`21m00Tcm4TlvDq8ikWAM`) returns HTTP 402
`paid_plan_required`: free users cannot use library voices through the API. Voice listing was
unavailable because the key lacks `voices_read`; no secret was extracted or permission changed.

George (`JBFqnCBsd6RMkjVDRZzb`) succeeded with the same free-plan key: HTTP 200 and 56,470 PCM
bytes. A real multi-context WebSocket frame then exposed an application decoder bug:
`isFinal` is null on audio frames. Accepting null as non-final let the application's existing
TTS session and Golem transport receive 60,512 PCM bytes followed by finality. The default voice
now uses George. This is account-specific evidence, not a guarantee for every free account;
legacy default voices are scheduled to expire December 31, 2026. Starter should remove the
library-access restriction if a paid voice is preferred; our 16 kHz format does not need Pro.

The coordinator now reports sanitized recoverable failure for nonblank completed text with no
audio bytes, without committing an unspoken answer. Oracle's first review identified empty-frame
and premature-final edge cases. Zero-byte frames are ignored; a final before text completion
invalidates the response and cancels generation. Regression tests cover absent/empty audio,
successful subsequent turns, null audio-frame finality, and premature finality with a blocked
reader. All 73 unit tests and typecheck pass; the bounded bug-finder review found no bugs.
Oracle's follow-up approved the fix. The integration Golem smoke and Chrome call test pass,
including audio playback acknowledgements, interrupt and hangup. The temporary diagnostic agent
was removed and the normal live app redeployed with George. These are application/provider fixes,
not new Golem defects. Full live browser playback still needs a fresh-call user check, and GOL-799
remains the active-call crash-recovery blocker.

## Follow-up — paced playback (completed)

The user confirmed live speech-to-speech worked, then hit the browser's eight-second/64-chunk
fail-on-backlog guard. Replace that prototype guard, without changing persisted state or the wire
protocol, and commit this step separately before building the walkthrough.

Playback now splits provider chunks into at most one-second sources and awaits capacity within a
four-second/64-source scheduled window. It acknowledges original chunks only after all slices
naturally complete, contiguously. Clear, local interrupt and irreversible terminal stop wake
blocked scheduling; stopped sources are never acknowledged. Single provider frames over 8 MiB
remain rejected. The backlog stays in Golem's Durable output stream, apart from the currently
materialized HTTP batch and provider frame; this is not a total-memory/retention guarantee.

Oracle's design review established that confirmation must stay ordered with audio. One paced
cursor therefore retains the existing UI dispatcher; a second independent cursor only fast-paths
clear-playback, fatal failures and terminal EOF. Both cursors join before normal disposal.
This doubles output downloads but requires no Golem API or state migration. EOF cancels audio.
Oracle approved the implementation, and the bounded bug-finder found no bugs.

76 unit tests and typecheck pass. Real Chrome/Golem tests pass for a 12.25-second chunk, 100-chunk
burst, natural final ACK, explicit interrupt, provider StartOfTurn barge-in, fatal STT failure and
hangup while scheduling is blocked. Measured scheduling stays below four seconds. The fixture
uses explicit control triggers after playback begins instead of timing-dependent failures.
The ordinary microphone/retry/playback/interrupt/hangup browser regression also passes.

## Follow-up — interactive architecture walkthrough (completed)

Host a self-contained `/walkthrough` page through the existing ephemeral Web router. Keep the
console's visual language; no extra frontend framework, provider calls or credentials required.
Provide selectable scenarios and step-by-step navigation highlighting browser, Durable Streams,
CallAgent, STT, LLM, TTS and snapshot boundaries. Show representative wire payloads, ownership,
source paths, buffering/ACK semantics, interruption, retry/closure and the GOL-799 recovery limit.
Examples must be explicitly illustrative, not a live trace or a crash-recovery claim. Verify all
steps and deep links in a browser, inspect desktop/mobile screenshots, obtain Oracle review,
document the page and commit it separately. Redeploy the normal local app and leave it running.

Implemented 23 navigable steps across five scenarios, eight selectable boundary inspectors,
wire/protocol excerpts, ownership/source references and hash-based shareable step links.
The static page needs no microphone, provider requests, credentials or additional frontend library.
The console footer links to it; both local and integration Web deployments serve the same assets.

Oracle found one factual documentation error: the coordinator cancels on StartOfTurn, not on
TurnResumed. Corrected both views without changing coordinator behavior. It also corrected the
selected-call-ID wording and completed-state restoration's test reference. Oracle's follow-up
approved the walkthrough. Its known-limit views distinguish synthesis from hearing, invocation
snapshots from per-frame persistence, scheduled bounds from total memory, and completed-call
restoration from the blocked GOL-799 active replay path.

The Golem-hosted Chrome test checks all 23 steps, boundary selection, invalid/prototype-key hashes,
deep links, previous/next, keyboard/history, responsive mobile layout, CSP and no call requests.
Desktop screenshots for every scenario and mobile screenshots were inspected, including the
corrected Flux/barge-in views and the console link. All 76 unit tests and typecheck pass; ordinary
console smoke and microphone/playback/ACK/interrupt/hangup regression tests also pass.
The bounded walkthrough bug-finder review found no bugs. The deployed OpenAPI link returns 200.

## Remark — potential raw-byte audio transport (not implemented)

Durable Streams does not always require base64. Source inspection of the current Golem release
confirms that direct byte slots accept raw HTTP POST bodies and return raw bytes through ordinary
or long-poll HTTP GET. Binary SSE responses still use base64 because SSE is text. In the Effect
SDK, a direct byte slot is `WitTypes.AgentStream(WitTypes.Uint8)`, not
`AgentStream(Uint8ArraySchema)`, which is a stream of byte arrays. Golem uses packed byte payloads
on the direct-byte path.

The application's current mixed audio/control streams are JSON, with audio represented by
`Schema.Uint8ArrayFromBase64`. This is an application protocol choice; changing only the payload
field would not make these mixed streams raw binary slots. Two possible future designs are:

- Separate raw PCM input/output slots from JSON control, transcript and playback-ACK slots.
  Cross-stream ordering, response identity, interruption boundaries and ACK positions would need
  explicit coordination. Raw byte reads concatenate payloads and do not preserve original chunk
  boundaries, so consumers cannot use HTTP response boundaries as audio-message boundaries.
- Encode audio and control records together in a framed binary byte stream. This preserves their
  shared ordering but requires a framing protocol and matching browser/agent parsers that handle
  records split across or combined within HTTP reads.

Either design would need raw-byte reads rather than SSE to avoid base64 on the browser-facing
audio path. Raw bytes are approximately 25% smaller than base64 audio payloads, excluding JSON
metadata, and avoid base64 conversion work. ElevenLabs' current WebSocket protocol would still
deliver base64 audio to the agent, but the agent would not need to re-encode it for the browser.

Separate audio/control slots are worth exploring, but preserving the current interruption and
playback-ACK semantics makes this a protocol redesign rather than a drop-in optimization.
This remark records a future option only: no implementation, schema migration or deployment
change is authorized by it.

## Follow-up — Golem internals and recovery walkthrough (completed)

Extended the existing Golem-hosted interactive page to eight scenarios / 36 steps. New sections
explain the Effect/QuickJS/WASM/WIT host boundary, oplog replay and lazy WebSocket reconstruction;
map browser HTTP JSON, Durable Streams offsets/producer tuples, provider WSS and OpenAI SSE;
and illustrate the optional reconstruction policy requested in
[GOL-804](https://linear.app/golem-cloud/issue/GOL-804/add-an-opt-in-websocket-reconstruction-policy-that-reports-physical).
The policy and provider-aware recovery steps are explicitly proposals, not implemented code.
GOL-799 still blocks active replay verification; fixing it alone does not implement retained
utterance replay, synthesis-context reconstruction or OpenAI provider-cursor resumption.

Oracle reviewed the content against application/runtime sources and approved it with one
localhost wording correction: these URLs assume browser and server are on the same machine.
The bounded bug-finder found no bugs. All 36 steps pass in Chrome on the live local deployment,
including deep links, invalid hashes, keyboard/history, proposal labels, CSP and no call/provider
requests. The integration deployment also passed all steps before that wording correction.
Desktop default and new-section screenshots and mobile views for each new section were inspected.
All 76 unit tests and typecheck pass. README includes direct section links.
Only static explanation assets, their browser test and documentation changed. The local/live and
integration/mock deployments were updated without resetting or updating existing call agents;
no SDK implementation, provider protocol, cloud deployment or additional ticket changed.
