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

## Provider-aware recovery implementation — 2026-10-13

Continue in separately reviewed commits, using CLI `v1.6.0-rc2-25-gfd734d5670` and the
local SDK at `fd734d5670e587ed50b04278749caa771281ed66`. Testing uses the existing isolated
ports and a new `.amp/in/local-golem/data-recovery-rc2` directory; older data is preserved.

1. Require the reduced GOL-799 reproducer to pass and record the existing application recovery
   matrix as a baseline (not a prerequisite that all application recovery is already green).
   Opt Deepgram and ElevenLabs into `report-connection-loss`, preserving a typed
   session-loss classification through adapter errors. Ordinary network/provider errors remain
   distinct; do not silently retry malformed messages or account failures.
2. Implement provider-session ownership and recovery. One owner replaces each lost session;
   concurrent operations on the old session cannot publish obsolete results. Scope each physical
   session separately, and never hold a recovery lock across a blocking receive. Retain bounded
   uncommitted PCM for Deepgram, with a new recognition epoch and session-relative time mapping.
   If the beginning is unavailable, discard the partial utterance and explicitly ask for a repeat.
   Replace interrupted synthesis with a new response ID tied to the existing committed user turn.
   Reuse complete answer text when available; otherwise regenerate the foreground OpenAI answer.
   Never splice a new answer suffix onto old text/audio. Clear old playback before replacement audio.
3. Exercise deployed crash recovery during STT, LLM and TTS, repeated crashes, partial playback,
   interruption/hangup during reconstruction and utterance-buffer overflow. Strengthen fixtures to
   reject text sent to uninitialized TTS contexts and distinguish old/new connections. Verify
   continued conversation and output closure, not merely an absence of traps. Run browser checks.
4. Update documentation and walkthrough to describe tested behavior and remaining limitations.
   Inspect affected rendered states, obtain Oracle review, and commit the acceptance evidence.

Each step ends with targeted checks, typechecking, an Oracle review, and a separate commit.
Preserve existing snapshot and browser wire shapes. Existing agents keep their original
component; use fresh test call IDs. No reset, cloud deployment, push, or SDK edits are planned.
Recovery preserves the call and committed conversation, not provider sessions or identical audio.
An answer can be repeated, especially when an audible chunk's ACK was lost. Browser reload
recovery, exactly-once hearing, OpenAI background/cursor resume, and raw-byte transport remain
outside this implementation. Live-provider checks are reported separately from protocol fixtures.

Oracle design amendments: response restart allocates only a response attempt, never another
user turn, and preserves complete text before invalidation. A finished answer whose audio is
already durable is not restarted merely because an idle provider socket was lost. Interrupt,
new speech and hangup revoke restart eligibility. STT retains samples before send, uses absolute
sample ranges and a per-session origin (not capture wall-clock time), commits each epoch/turn
once, and trims only through the accepted end-of-turn boundary. Replay precedes the live tail.
Overflow suppresses the damaged fragment through its end-of-turn before re-arming; the repeat
request is a visible recoverable failure, not an invented user turn or a spoken system answer.

Loss reporters must not join themselves or wait on a recovery owner joining them. Control
ingestion stays independent of a bounded audio sender. Retired sessions have separate scopes;
no lock spans receive, fiber cancellation or queue-capacity waits. Replacement publication must
wait until its clear event is in the same output queue, not merely an intermediary control queue.
Recheck ownership after acquisitions so hangup/new speech wins. Tests must distinguish concurrent
send/receive loss, OpenAI/TTS loss observation order, completion races, blocked reconstruction,
output saturation, overflow followed by a healthy turn, and snapshot validity after replacement.

### Recovery progress and replay gate — 2026-10-13

The response-attempt foundation is committed: a restart can allocate a new response ID for the
same committed user turn without incrementing the turn counter, while preserving the old
attempt's emitted/acknowledged audio history. Deepgram/ElevenLabs adapters preserve the SDK's
typed physical-session-loss classification. The reconstruction policy is **not enabled yet**;
retained PCM replay, provider-session replacement and answer regeneration are still pending.
All 77 unit tests and typecheck pass; Oracle reviewed the foundation and the bounded bug-finder
reported no bugs.

The installed CLI is compatible after rebuilding the local SDK with a private wasm-rquickjs
generator at `15ab69c222fbd13cf9e80f3bb373bd0eeb524d10` (the global 0.4.6 generator emitted
the obsolete two-argument WebSocket ABI). The private executable is under
`.amp/in/recovery-tools/bin`; no global tool or SDK source was modified. Use that directory
on PATH and retain `GOLEM_EFFECT_GOLEM_PATH` when rebuilding against this CLI.

On fresh isolated data, the original reduced duplex echo **passes crash continuation**. A
temporary variant with forked input ingestion and a bounded output queue also passed, then
was removed. Normal fixture calls, provider-failure checks and all three non-crash interruption
checks pass. The full pipeline still stalls after crash at STT, LLM and TTS gates, before
physical WebSocket reconstruction. Server logs verify actual Restart and replay startup;
CLI success alone is not considered recovery evidence.

An additional `replay-stt` scenario separates runtime liveness from provider reconstruction:
hold recognition output, simulate a crash, then append a fresh interrupt without expecting any
old provider output. It fails to confirm the interrupt within 20 seconds; subsequently persisted
hangup/input closure does not close output within another 10 seconds. The no-crash
`interrupt-stt` control confirms interruption and closes output. The harness now verifies
cleanup completion rather than treating an accepted hangup append as successful cleanup.
Fallback cleanup checks output termination after fixture teardown, not graceful hangup or
complete resource disposal; its success cannot erase a prior scenario failure.

Run the diagnostic in Nushell with
`with-env { GOLEM_TEST_SCENARIO: replay-stt } { node test/recovery-smoke.mjs }`.
This is a **known failing integration test**, not a passing acceptance check.
Logs/oplogs are retained under `.amp/in/`.
The older `crash-*` scenarios remain provider-recovery acceptance goals, not proof that
releasing held fixture callbacks can restore a lost provider socket.

Oracle traced the earlier STT stall to the replay interval containing pending WebSocket
receives, a completed zero-duration timer and a completed send. The precise cause remains
unproven: runtime call admission/delivery and guest/SDK scheduling have not been distinguished.
Duplicate checking found GOL-799 as the closest existing report; follow-up evidence was added
there instead of opening another issue. GOL-709's in-progress call-claim ordering work may
be relevant, but is not a verified fix. GOL-799 remains marked Done in Linear despite this
broader failure. No ticket state was changed.

Provider-session recovery remains paused at this replay gate. Do not deploy partially wired
reconstruction or claim end-to-end crash recovery; resume steps 2–4 after the fresh-control
replay test can make progress. The non-crash application remains unchanged by this diagnostic.

### Separate replay report and standalone reproduction — 2026-10-13

The broader remaining failure is now tracked as
[GOL-862](https://linear.app/golem-cloud/issue/GOL-862), not as the original GOL-799 defect.
Checked release issues and active Golem Amp threads: GOL-709/#4131 and GOL-839/#4115
may be relevant, but neither has verified coverage of this exact duplex scenario.
GOL-799 remains Done and its reduced echo recovery passes.

Attached a fixture-only source archive to GOL-862, with a standalone manifest, full handler,
Nushell setup/runner, scoped host logs and oplog. It is not yet a minimal reduced agent.
Built/typechecked it and tested a separate local deployment without Web assets or real keys.
The standalone failure is intermittent: five crash runs produced four failures and one pass;
non-crash controls passed. Both outcomes are retained, not just failures. One successful
replay is therefore insufficient acceptance evidence. Oracle reviewed the reproducer and
evidence; its packaging/provenance corrections were applied. No runtime fix is claimed.

### STT provider reconstruction — 2026-10-13

The user authorized continuing despite intermittent GOL-862, separating application recovery
tests from runtime replay liveness. The earlier pause is superseded; the defect remains open.
Do not classify every timeout as GOL-862 or retry until green. Preserve bounded fresh-agent
attempts and use logs proving replay stalled before application reconstruction for that diagnosis.

Deepgram now opts into physical-session-loss reporting. One serialized sender replaces scoped
recognition sockets; a retirement signal releases old receives independently of transport cleanup.
Atomic ownership prevents hangup from being overwritten by late reconstruction. Acceptance of a
final user turn and trimming its PCM share a recognition-epoch fence. A retired final is rejected
and reconstructed, not committed twice. Relative provider times map to absolute sample boundaries.
Up to ten seconds of uncommitted PCM are retained before send. Reconstruction restores the last
configuration and replays it before live audio, capped at 1.25× including the queued live tail.
Three consecutive session losses during one restoration terminate that restoration; ordinary
provider/account/protocol failures are never silently retried. The SDK openTimeout is a no-op,
so this step makes no promise of a timed provider-open deadline.

If overflow removed the utterance beginning, reconstruction reports a visible repeat request and
suppresses the damaged fragment through its first final before rearming. Recognition writes run
in an independent bounded sender (128 frames / 320,000 queued bytes), allowing interrupt, playback
ACK and hangup ingestion while reconstruction is pending. Exhausting that sender is terminal.
Browser wire and snapshot shapes are unchanged; retained PCM is rebuilt by invocation replay,
not copied into snapshots.

85 unit tests, typecheck, local-SDK build and the isolated integration/mock deployment smoke pass.
Tests cover receive/send loss, retained-tail boundaries, repeated loss during replay, overflow then
a healthy turn, ordinary-error propagation, stale-final rejection, non-waking old receives,
hangup during acquisition, and control responsiveness while sending is blocked. Oracle's races
were fixed and its final STT review found no blockers; the bounded bug-finder review is clean.
The smoke verifies transcript/audio/completion/ACK/hangup/output closure, not live-provider or
crash-recovery compatibility. ElevenLabs/OpenAI replacement and the deployed crash matrix remain
the next separate steps. Only the integration deployment was updated; existing agents were not
reset or updated and the normal live deployment remains unchanged.

### TTS replacement and answer reconstruction — 2026-10-13

ElevenLabs now opts into explicit physical-session-loss reporting. One owner arbitrates
send/receive loss and hangup, retires the old epoch, and hands reconstruction to a dedicated
worker. An unfinished response is interrupted and its browser playback cleared before a new
attempt ID for the same committed user turn is launched. Complete generated text is reused;
incomplete text is regenerated through the foreground OpenAI request, never suffix-spliced.
Old audio/ACK records remain intact and retired socket events cannot complete the new attempt.
Idle completed playback is not discarded. Three consecutive reconstruction losses terminate
recovery; ordinary provider errors remain errors rather than reconnect triggers.

Review/testing exposed blocked-flush teardown races. Hangup now retires before joining,
invalidation cleanup runs in a dedicated generation scope outside control ingestion, and
implicit adapter teardown locally retires rather than depending on a graceful provider write.
Output teardown revokes ownership before closing that generation scope, including cancellation
inside a downstream consumer callback. Explicit graceful socket closure remains available.

99 unit tests and typecheck pass, including real-adapter blocked flush/close and unpublished
replacement cases. Oracle reviewed the fixes and found no remaining focused blockers. The
local-SDK build, isolated mock deployment and transcript/audio/ACK/hangup smoke pass. This is
not proof of host-crash or live-provider reconstruction: strict fixture and bounded deployed
crash-matrix validation remain next. The normal live deployment has not been changed.

### Strict fixture and deployed recovery matrix — 2026-10-13

The fixture now rejects text/flush/close for contexts not initialized on that physical
synthesis socket, records connection epochs and protocol events, and advances recognition
timestamps by received PCM bytes. A distinct second-input sample marker produces a distinct
transcript. The recovery runner preserves every fresh-agent attempt in a unique directory under
`.amp/in/recovery-runs/`, including provider journal, output events, CLI crash output and failure
details. Attempts are fixed in advance (1–3), not repeated until green; failed cleanup remains
failed. Controls can be selected as a comma-separated `GOLEM_TEST_SCENARIO` value.

After Oracle's acceptance review, both answers must contain matching text, nonempty audio and
completion for their own response ID. The first ACK is mandatory. The next answer requires a
new committed transcript after the second-input boundary; the call must stay open until explicit
hangup. Negative synthetic controls reject missing audio, a premature extra completion,
completion accompanied by early closure, and mixed old/new response IDs. The deployed no-crash
continuation control passes, as do slow reader, three provider-failure and three interruption
controls. These are harness and normal-call checks, not crash acceptance.

A fixed batch of three fresh calls per crash stage produced one STT continuation and eight
stalls (STT 1/3, OpenAI 0/3, ElevenLabs 0/3). The STT continuation showed fresh recognition PCM,
two synthesized answers, ACKs and hangup closure, but ran before the strengthened second-turn
assertions: it is not a strict-check acceptance pass. Failed attempts showed no replacement
provider connections. The independent replay-control probe also stalled; persisted hangup/input
closure did not close its output. Scoped logs and an STT oplog are retained beside those attempts.
This matches the observed pre-reconstruction replay-liveness pattern tracked by GOL-862; the
underlying runtime/guest scheduling cause remains unisolated. Never relabel arbitrary failures
as that issue or count a timeout as successful recovery.

Strict crash acceptance, repeated crashes on one continuing call, deployed partial-playback/ACK
races and deployed utterance-overflow recovery remain unverified because failed attempts cannot
reach those phases. Unit tests cover the corresponding application boundaries; they do not
substitute for host-crash evidence. Live-provider crash compatibility remains unverified too.
Stop this fixed batch rather than chase a successful certificate. Oracle approved the fixture
changes and the final harness corrections; commit this as partial acceptance evidence.

### Walkthrough and fresh-server browser verification — 2026-10-13

The walkthrough now explains implemented optional session-loss reporting, bounded recognition
replay, new synthesis attempts and foreground answer regeneration. It distinguishes these
application mechanisms from pending strict host-crash/live-provider acceptance, and replaces
the obsolete GOL-799 blocker with GOL-862. All 36 deployed walkthrough steps, deep links,
navigation, CSP, no-provider-request checks and mobile layout pass; desktop/default, all four
recovery steps and mobile captures were inspected. The older Amp portal remains an older static
copy; the current walkthrough is served by the integration Golem deployment.

Automated Chrome's native microphone request remained pending before any backend connection.
Using a controlled Web Audio MediaStream instead exercises the real AudioWorklet, browser
capture/transport, playback and ACK path, but does not verify physical microphone permission.
The first long-reply run timed out during hangup. Oracle traced this to a recorded memory-growth
admission failure followed by automatic agent restart/replay, not a failure-free input stall:
oplog entry 758 records Out of memory after 53 PCM frames. Browser logs after that point reflected
already-published audio paced through playback, not continued guest progress. No application
teardown change was justified by that evidence.

A fresh isolated server process, new data directory and explicit 8GiB admission budget were used
without deleting old data. All six unchanged browser buffering scenarios then passed: long
12.25-second audio, 100-chunk burst, interrupt, barge-in, provider failure and hangup. Scheduled
look-ahead stayed below four seconds; completed chunks were ACKed only after natural playback.
The fresh log contains no memory-admission failure. Logs are retained in
`.amp/in/browser-buffering-fresh.log` and `.amp/in/local-golem/browser-fresh-server.log`.

One predeclared crash attempt per provider stage on that fresh server still failed before a
replacement connection or answer, with persisted hangup unable to close output. No OOM was
recorded in this batch. Each failure remains a failure, with its provider journal/output/crash
and cleanup evidence under `.amp/in/recovery-runs/`; the aggregate log is
`.amp/in/recovery-crash-fresh.log`. This separates the observed replay-liveness problem from the
earlier browser memory-pressure restart, without claiming a proven underlying cause. Strict
crash acceptance remains pending; do not keep retrying for a green result. Typecheck and all
99 unit tests pass. Only the isolated integration deployment was updated; no live deployment,
agent reset, push or cloud deployment was performed.

Oracle approved this documentation/walkthrough checkpoint with no blockers, explicitly not
crash acceptance. Additional fresh-server evidence and the memory-pressure distinction were
commented on the existing GOL-862 (still Todo), rather than opening a duplicate issue.

### Updated runtime verification — 2026-10-15

Verified CLI `v1.6.0-rc2-68-g3b7efa521e`, with the matching rebuilt local Effect SDK symlinked
from `~/projects/golem/golem/sdks/effect`. Forced a fresh application build using
`GOLEM_EFFECT_GOLEM_PATH` and the previously verified compatible wasm-rquickjs injector.
Stopped only this thread's old server process and started the updated binary with a new data
directory `.amp/in/local-golem/data-rc2-68`, ports19881/19006/19007 and explicit8GiB admission
budget. Old data was preserved. Only the integration/mock environment was deployed.

A predeclared batch of three fresh agents per scenario now passes `continue-call`, `replay-stt`,
`crash-stt` and `crash-elevenlabs` (12 passes). Recognition and synthesis recovery each complete
two distinct committed turns with matching text/audio/completion, mandatory playback ACK and
explicit hangup/output closure. Post-crash interrupt controls are responsive again. GOL-862 is
Done, and its old stall was not reproduced in these bounded tests. Seven additional slow-reader,
provider-failure and interruption controls pass; typecheck and all99 unit tests pass unchanged.

All three `crash-openai` attempts fail, and the aggregate runner correctly exits nonzero.
This is not GOL-862: the agent reaches terminal Failed with `guest call settlement task failed:
Non-idempotent remote write operation was not completed, cannot retry`. Only the first transcript
is returned and the fixture observes one HTTP request, not a replacement. Representative server
replay-buffer history has batched scope147, HTTP send149 under147 and foreign WebSocket receive150
(WriteRemote, no parent). The unfinished HTTP scope's `no_concurrent_side_effect` scan rejects
that foreign write despite default assume_idempotence=true. Oracle reviewed the source/evidence
and confirmed no justified application fix: setting idempotence=true again cannot bypass that
guard, and serializing away recognition or globally weakening side-effect safety is not acceptable.

After checking all166 release-project issue titles, related completed HTTP/replay reports and
unarchived Golem threads, filed general reports in Golem1.6 finalization:
- [GOL-906](https://linear.app/golem-cloud/issue/GOL-906): P3 POST recovery with concurrent
  WebSocket receive; runtime defect versus intended restriction remains a maintainer decision.
- [GOL-907](https://linear.app/golem-cloud/issue/GOL-907): CLI oplog JSON decoding fails with
  missing `onRight`, truncating export after entry150. Server logs supplied the missing evidence.

GOL-906 has a fixture-only full-handler source/evidence archive with Nushell instructions,
112887 bytes, SHA256 `4997d2b24b8c399837718be0cfaf8dc01fc0da5e96ffd388d987eebd7213c36f`.
GOL-862 was commented with its passing verification and the separate remaining failure.
Aggregate logs are `.amp/in/rc2-68-recovery.log`, `.amp/in/rc2-68-controls.log` and
`.amp/in/local-golem/rc2-68-server.log`; per-agent evidence remains under recovery-runs.

No production code or acceptance assertion was changed. Full-server restart, repeated crashes
on one call, deployed partial-ACK/overflow races and real-provider crash compatibility remain
unverified. Browser rendering was not changed or rerun in this verification step; existing
walkthrough GOL-862 labels predate these results. The README is the current acceptance summary.

### Local crash-demo controller — 2026-10-15

Added `demo-server.mjs`, a loopback-only controller/proxy at localhost:19008. It forwards the
console and streaming traffic to the selected local or integration Golem hostname, preserving
the deployed CSP and avoiding browser management credentials or cross-origin crash calls.
Its same-origin JSON endpoint validates a bounded call ID and invokes only
`golem agent simulate-crash CallAgent(...)` in an allowlisted local environment, without a shell.
One command runs at a time, with a 30-second timeout and no automatic retry. The response
confirms command completion, not successful recovery. It never kills the Golem server.

Real HTTP verification through the proxy created a fresh fixture call, completed its first
answer, invoked the actual CLI through the controller, observed a distinct post-crash transcript
and confirmed hangup/output closure. Missing-origin, malformed JSON and invalid-ID requests
were rejected. Oracle reviewed this backend step and found no blockers. Frontend wiring is
the next separate commit. A fresh HTTP-stage crash recheck still failed on rc2-68; GOL-906
was checked live and remains Backlog, so arbitrary-time clicks must retain that warning.

### Crash-demo console and browser acceptance — 2026-10-15

Added controller-discovered demo controls to the existing Golem-hosted console. The active call
identity owns the crash request; pending requests disable duplicate clicks, never automatically
retry, and report command completion separately from recovery. Feedback survives hangup but
cannot overwrite a replacement call's feedback. The ordinary Golem URL hides these controls.
The controller now accepts bounded nonblank IDs including periods, preserving the console's
existing identity contract; JSON.stringify and execFile keep CLI arguments shell-free.

Deployed the frontend to the isolated integration server and passed the real Chrome/controller/
CLI test: one crash request, a new assistant response identity with expected fixture text,
matching responseFinished and Played events on the same call, then clean hangup. Validation
also covers rejected requests, disabled/pending/error states, terminal feedback after hangup,
stale replies after replacement, and 390px layout. Request gating and error injection exercise
frontend lifecycle only; they are not counted as runtime recovery. Controlled Web Audio capture
avoids native microphone permission. Typecheck and all99 unit tests pass. Inspected default,
pending, completed, error and mobile screenshots in `.amp/in/artifacts/demo-*.png`.

Oracle identified and then approved the disposal/feedback correction; the dotted-ID regression
was reproduced before correction and is covered by the real crash test. README documents live
and fixture controller launch modes. GOL-906 remains Backlog and a fresh HTTP-stage crash failed;
neither arbitrary-time clicks nor live-provider/full-server recovery are acceptance claims.

### Buffered inference isolation experiment — implementation checkpoint, 2026-10-15

Committed all existing workspace changes before starting this experiment. Reproduced the
unchanged direct-HTTP failure once on matching rc2-68 CLI/server and locally rebuilt SDK:
`crash-openai-1e38cbdf-3bf5-4a5e-96c5-b512948f1e62`, retained in recovery-runs with aggregate
`.amp/in/inference-baseline.log`. No server data was reset.

Added an opt-in `bufferedInference` config (false by default). A separate durable
`InferenceAgent(callId)` sequentially consumes the complete Effect AI response and returns text
through durable RPC. Its oplog contains no recognition/synthesis WebSocket operations. The caller
continues consuming both WebSockets concurrently and never receives an incomplete inference
prefix. Buffering is bounded to 65536 UTF-16 code units. Model/API URL config and OpenAI secrets
belong to the inference agent independently; the caller still supplies its system prompt/history.
SavedCallState's schema is unchanged; constructor identity is recovered from snapshot context.

This is deliberately not a streaming replacement: first speech waits for complete inference.
Interrupt/hangup suppresses late caller output but may leave the remote operation running,
incurring billing and delaying subsequent work queued on the same per-call inference owner.
No custom durability scope, idempotence override, manual RPC retries or recognition serialization
was introduced. Default local/live and integration streaming deployments are not switched.

Created a separate local `inference-recovery` environment, mock+buffered presets and
`call-center-recovery.localhost:19006` domain on the existing isolated server. Its first strict
two-turn/ACK/hangup/output-closure control passed. Oracle reviewed implementation and found no
blockers for the experiment, requested environment/origin validation and collector boundary
tests, and checked scope and snapshot ownership. Applied those changes. Crash acceptance is
the next checkpoint, including caller/helper/both pre-header crashes and response-body coverage.

### Buffered inference isolation — fixture recovery acceptance, 2026-10-15

The predeclared matrix passed three fresh attempts per scenario: caller-only, helper-only and
sequential caller-then-helper crashes before response headers, and the same three cases after
a partial SSE body. All 18 pass two distinct committed turns with expected text, nonempty
response-tagged audio, matching completion, mandatory ACK submission and hangup/output closure.
The body scenarios first assert no text/audio/completion escapes the caller while incomplete.
Six additional STT/TTS crash attempts, three continuation controls, seven failure/interruption/
slow-reader controls, and all six real Chrome buffering scenarios also pass. There is one extra
earlier implementation preflight control, not counted in the declared matrix. Typecheck and
100 unit tests pass, including collector completion/error and 65536/65537 boundary behavior.

Representative read-only oplog verification confirms the helper consumed the prefix: End63 for
consume-body-chunk62 contains `Hello `, followed by Jump66 cutting61–65. Its 168-entry history
has no WebSocket Starts. The separately sampled 422-entry caller history has zero HTTP Starts,
31 WebSocket Starts,12 RPC Starts and no jumps/errors. This corroborates isolation; it is not
a claim that HTTP no longer rolls back. The helper's rollback cannot discard caller history.
Fixture-side prefix emission plus a bounded delay is the harness trigger; guest consumption
was additionally established from the representative oplog, not automatically for every run.

Two-turn scenarios observe three HTTP requests after either caller-only or helper-only crashes,
four after both, versus two without crashes. Inference repetition/billing is intentionally accepted.
Interrupt controls demonstrate caller-visible suppression after provider release, not cancellation
of remote inference. Full-server restart, real providers, repeated crashes within one continuing
call, and nondeterministic replacement-body compatibility remain unverified. The existing local
live demo and default streaming deployments are unchanged.

Evidence: `.amp/in/inference-crash-batch.log`, `inference-body-batch.log`,
`inference-other-crashes.log`, `inference-controls.log`, `inference-browser-buffering.log`,
per-call recovery-runs, and `.amp/in/artifacts/inference-recovery-results.json` plus
`inference-oplog-ownership.json`. Raw fixture oplogs are under `.amp/in/inference-*-oplog-complete.json`;
they may contain mock credentials and should not be published unreviewed. Oracle audited all
35 retained experiment outcomes and raw ownership evidence, approved this validation step with
no blockers, and kept the acceptance limits explicit. A separate bounded bug-finding pass found
no bugs. No runtime/SDK changes, server resets, cloud deployment or push were performed.

Final controls also passed: crash an ended experimental call, then invoke it again to verify
restored ended-state rejection and output closure; deploy the new code with bufferedInference
false to integration and complete a fresh direct-streaming two-turn call. Hardened the runner
to require exactly two completions after final drain (also checked by the evidence audit).
The live-provider controller remains on its original local deployment at localhost:19008.

### Streaming inference isolation — implementation checkpoint, 2026-10-15

The original implementation forwarded inference deltas to synthesis immediately. At the user's
request, replaced the experimental scalar/buffered RPC with `WitTypes.AgentStream` carrying
TextDelta, Completed and sanitized Failed items. The shared response-event schema owns the
successful cases; the wire union flattens its members because the SDK rejects nested unions.
Each remote stream owns its RPC scope, and provider errors do not become successful EOF.
Renamed the opt-in config/preset to isolatedInference/isolated; defaults remain direct streaming.
This changes the experimental RPC/config contract: fresh identities only, no old-agent updates.

Deployed only inference-recovery revision2 on the isolated rc2-68 server. All99 unit tests and
typecheck pass. Seven fresh preflights passed: continuation plus caller/helper/both crashes before
headers and after an exposed prefix. Body tests require audio and a synthesis text write before
releasing provider completion, then two distinct turns, matching text/audio/completion, ACKs and
hangup/output closure. Evidence: `.amp/in/streaming-inference-preflight.log`, per-call recovery-runs
including streamed-prefix.json. These results are separate from the buffered acceptance above.

Oracle reviewed the implementation and scope with no blocker to fixture experiments. It identified
two acceptance checks for the next step: changed prefix/chunk segmentation on retry (durable stream
deduplication compares payloads, not just sequence numbers), and helper availability after interrupt
or hangup while the obsolete provider request stays held. No general nondeterministic inference
recovery claim is made. Existing live demo remains unchanged; no runtime/SDK changes or pushes.

### Streaming isolation — adversarial validation checkpoint, 2026-10-15

All18 predeclared caller/helper/both header/body crash cases passed (three fresh attempts each),
plus the seven earlier preflights. The aggregate is
`.amp/in/artifacts/streaming-inference-recovery-results.json`. First-response synthesis journals
contain exactly the expected text, including helper-only crashes after published audio.

Added public contract probes for early audio, cancellation without releasing the obsolete request,
hangup without release, and persistent changed text/chunk segmentation after helper recovery.
Initial fixture resets contaminated later cases by destroying held responses and provoking retries
into the next fixture scenario. Corrected cleanup releases requests only after recording outcomes,
then waits for the helper to settle before resetting. Do not count those contaminated runs.

The corrected bounded run (`.amp/in/streaming-inference-adversarial.log`) passes prefix and both
hangup cases but fails both continuation-after-cancel cases: interrupt is confirmed, yet the next
generation queues on the per-call helper behind the obsolete HTTP request. The next change will
key the helper by the existing stable callId/responseId, preserving replay identity while allowing
a fresh response to proceed independently. Oracle reviewed and recommended this boundary.

Persistent changed prefixes/chunks fail EventConflict after runtime retries; no mixed answer or
duplicate synthesis is committed. However, the remote error-context terminal traps the consuming
CallAgent itself, closing output without an application failure item. Oracle traced this to the
host stream receive/Wasmtime producer boundary, not an Effect failure/defect that catchCause could
contain. This needs a distinct runtime/SDK error-propagation contract; do not weaken replay safety
or pretend a TypeScript catch solves it. Duplicate research found no dedicated ticket; existing
Scala fatal-stream tests explicitly expect a fatal consumer invocation. As this may be a feature
contract decision, discuss it with the user before filing a feature request.

The conflict probes remain explicit failing acceptance scenarios, not silently green controls.
They and their retained journals are the reproducer. No runtime implementation was changed.

### Per-response streaming isolation — implementation and verification, 2026-10-15

Changed the durable helper identity to (callId, responseId). ResponseGenerator.stream now requires
the coordinator's existing response ID; no new counter or generated identity is introduced. Replay
of the same response selects the same helper; accepted turns and synthesis replacement responses
select independent helpers. The direct OpenAI implementation ignores this additional argument.
No caller snapshot schema change, cancellation workaround, idempotence override or runtime edit.
Helpers/history remain per response; automatic deletion and exactly-once billing are not promised.

Deployed fresh identities only to inference-recovery revision3 on the same isolated rc2-68 server.
All18 header/body crash cases passed again with this identity (three fixed fresh attempts each).
Both combined-crash scenarios crash the caller then the original response-1 helper, which may no
longer be the active replacement helper; they do not demonstrate simultaneous crashes. The five
stream-contract cases now pass: early audio, cancellation before headers/after prefix followed by
a new completed turn without releasing obsolete HTTP, and hangup before headers/after prefix
without release. Repeated those five after strengthening stale text/audio/completion checks through
final drain; all passed. First-response synthesis journals again contain exactly the expected text.

Ten control cases pass (continuation, slow reader, three provider failures, three interruptions,
recognition/synthesis crash), plus six real Chrome buffering/playback cases. Deployed the same code
with isolatedInference=false to integration revision4 and completed a fresh two-turn direct call.
The live-provider deployment/controller is unchanged. Typecheck and all99 unit tests pass.

Evidence logs: `.amp/in/streaming-response-isolation-{deploy,contract,final-contract,crashes,
controls,browser,direct-deploy,direct-control}.log`; retained per-call runs; aggregate38 passing
fixture outcomes in `.amp/in/artifacts/streaming-response-isolation-results.json` (18 crashes,
10 controls and two batches of5 contract cases). The six browser cases and direct-mode control
are separate from that aggregate. The earlier per-call helper results remain separate above.

Explicitly reran the two persistent divergence probes on the final per-response implementation:
both still fail desired containment, with no mixed completed response, in
`.amp/in/streaming-response-isolation-conflicts.log`. Producer EventConflict remains fatal to the
consumer. This is an intentional existing P3 error-context boundary, not solved by response-key
isolation. Do not present arbitrary real-provider crash recovery as accepted or suppress the failed
probes. A catchable remote-terminal option/contract is a discussion item, not an opened feature.

Oracle reviewed the completed step and inspected the logs/journals, approved it with no blockers
within this fixture-experiment scope, and requested the combined-crash wording clarification.
Applied that clarification and its optional final-drain stale-output check. The bounded independent
bug-finding pass found no application bugs. No server reset, live/cloud deployment or push.

### Next changeset — prefix-safe recovery design consultation, 2026-10-15

The streaming implementation is committed and the worktree was clean before this consultation.
Oracle confirmed that preserving published RPC items while rolling back their incomplete HTTP
input scope is the conflicting boundary. Do not remove EventConflict checks, discard published
stream history, skip regenerated chunks, or return to complete-response buffering.

Two materially different implementation directions remain:

1. Provider-specific OpenAI recovery: create a stored background response with stream=true, acquire
   its provider response ID before publishing text, then retrieve incremental SSE windows using
   stream=true and starting_after=<provider sequence_number>. Settle each HTTP window before
   publishing its normalized events. No new create after published output. This adds storage/
   retention requirements and potentially a network round trip per delta in the first prototype;
   model/account support and latency must be verified. It is not generic HTTP recovery.
2. Runtime recovery: preserve/replay the delivered HTTP prefix and expose loss of its continuation
   as an ordinary guest-visible HTTP error unless a provider-specific continuation is validated.
   The application could then resume by cursor or abandon the response explicitly. This is a
   broader executor recovery-policy change, not a one-line adjustment to request reissue; parent/
   child recovery and error delivery must remain consistent. Fatal remote-stream error containment
   is a separate boundary and must not be disguised as an Effect catch.

Oracle's correctness-first provider design has a prerequisite: prove that awaited cancellation of
an HTTP body durably settles its consume-body scope before publication. The generated native SDK
template inspection did not establish that barrier. This is an unverified concern, not a confirmed
bridge bug. Investigate/test the authoritative wasm-rquickjs source and matching SDK build before
depending on it; do not hand-edit generated templates. Custom Durability.wrap around a live Effect
stream is also not a verified fiber-safe substitute.

Choose the provider/storage/latency trade-off versus runtime-policy scope with the user before
implementation. Keep the deterministic crash matrix and explicit failing divergence probes as
baseline evidence. No implementation change or feature ticket was made during this consultation.

### Scope clarification — 2026-10-15

Build and test the call-center application on the planned Golem release, using the matching
local Effect SDK. Report runtime defects; do not modify Golem to make application tests pass.
The runtime option above was misunderstood as authorization to change Golem. The resulting
uncommitted experiment is isolated in `/Users/vigoo/projects/golem/golem-http-prefix-recovery`
and is not part of this application's implementation or release validation. Its passing tests
do not establish recovery support in the planned release. Continue with application-level
recovery mechanisms supported by that release, and explicitly report remaining runtime blockers.
