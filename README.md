# Call Center

A durable Effect Golem call agent: Deepgram Flux → Effect AI / OpenAI Responses → ElevenLabs
streaming TTS. A static-only ephemeral Golem HTTP router hosts the same-origin browser console.
See [`DEVELOPMENT_PLAN.md`](./DEVELOPMENT_PLAN.md) for the design and commit-by-commit history.

**Status:** provider-aware reconstruction is implemented and unit-tested; the fixture pipeline
and browser work locally. On CLI `v1.6.0-rc2-68-g3b7efa521e` with the rebuilt local SDK,
post-crash controls, recognition recovery and synthesis recovery each pass three fixed attempts.
[GOL-862](https://linear.app/golem-cloud/issue/GOL-862) is Done and its old stall was not reproduced.
Default-path whole-call crash acceptance remains pending: an unfinished HTTP POST interleaved
with a WebSocket receive fails recovery ([GOL-906](https://linear.app/golem-cloud/issue/GOL-906)).
The opt-in streaming inference-isolation experiment below passes its deterministic fixture crash
matrix; changed inference prefixes on replay remain a separate, fatal limitation.
Live recognition, LLM responses and audio playback were user-tested. Long-response buffering is
also tested against protocol fixtures using real Chrome playback and Golem streams. Acoustic
latency remains unmeasured. This is an unauthenticated development prototype, not a production
telephony service.

## Interactive walkthrough

Open [Inside the pipeline](http://call-center.localhost:19006/walkthrough) after local deployment,
or `/walkthrough` on the integration hostname. The console footer links to it too.
Eight scenarios and 36 steps cover a spoken turn, paced playback, interruption, delivery/retry,
state/recovery, Golem replay internals, endpoints/transports, and provider recovery.
Select a boundary to inspect its ownership/source files, or use step links such as
[`#runtime/3`](http://call-center.localhost:19006/walkthrough#runtime/3) for current lazy reconnect,
[`#protocols/0`](http://call-center.localhost:19006/walkthrough#protocols/0) for the endpoint map, and
[`#recoveryPolicy/0`](http://call-center.localhost:19006/walkthrough#recoveryPolicy/0) for GOL-804.
The transports opt into `report-connection-loss`; default automatic reconnect remains available
in Golem. Recognition restores bounded uncommitted PCM; unfinished synthesis gets a new response
attempt, reusing complete text or regenerating incomplete text. The walkthrough distinguishes
implemented application logic from pending crash/live-provider acceptance. These localhost URLs
require the browser and Golem to run on the same machine. The earlier Amp portal is a static
copy of the older walkthrough, not an automatically updated deployment.
Its GOL-862 acceptance labels, and those in the deployed walkthrough, predate the latest runtime
verification; use this README and the plan's 2026-10-15 checkpoint for current acceptance status.
The walkthrough is a static interactive explanation with representative protocol excerpts—not
live telemetry. It requires no microphone or provider credentials. Run `node test/browser-walkthrough.mjs`
against the integration deployment to check navigation, examples, CSP, mobile layout and no call requests.

## Prerequisites and isolated server

- Node.js 22.12 or newer, Golem CLI 1.6, and Google Chrome for browser tests.
- Golem checkout at `../../golem`, with its Effect SDK built at `../../golem/sdks/effect`.
- Exact Effect/OpenAI 4.0.0 dependencies; the SDK dependency is a local `file:` link.

Commands below are Nushell, from this repository root. Set the override in **each terminal** that
runs Golem so the CLI and npm resolve the same SDK:

```nu
$env.GOLEM_EFFECT_GOLEM_PATH = ($env.HOME | path join projects/golem/golem/sdks/effect)
npm install
npm test
npm run typecheck
golem build -Y
golem server run
```

`golem.yaml` selects management **19881**, HTTP **19006**, MCP **19007**, a 4GiB agent-admission
memory override, and `.amp/in/local-golem/data-current`. Readiness is recorded in
`.amp/in/local-golem/ports.json`. These are independent of default Golem ports and data. The memory
override is not a hard RSS limit. Do not start a second server on these ports or clean its data.
HTTP/MCP listeners bind all interfaces; use a trusted development machine/network.

Before writing test artifacts, ensure `/.amp/in/` is in this repository's local Git exclude file
(locate it with `git rev-parse --git-path info/exclude`); it is already configured in this checkout.

## Try the console without provider credentials

In another terminal, start the deterministic protocol fixture (port **19110**):

```nu
node test/provider-fixture.mjs
```

With the server and fixture running:

```nu
$env.GOLEM_EFFECT_GOLEM_PATH = ($env.HOME | path join projects/golem/golem/sdks/effect)
golem deploy -Y -E integration -P mock
node test/golem-smoke.mjs
node test/browser-smoke.mjs
with-env { GOLEM_TEST_DURATION_MS: "30000" } { node test/browser-call.mjs }
```

Open [the local console](http://call-center-test.localhost:19006). Start a call, grant microphone
access, interrupt, then hang up. Use a **new call ID** for each call; ended agents reject reuse.
The fixture ignores speech content, produces “Hello fixture” at most once per second, replies
“Hello from the fixture.” and plays a short tone. Browser tests use Chrome's fake microphone,
but real AudioWorklet capture, Golem streams, WebSockets, playback and acknowledgements.
Default/mobile/active/interrupted/failed screenshots are saved in `.amp/in/artifacts/`.

## Live providers

In a private terminal, set `DEEPGRAM_API_KEY`, `OPENAI_API_KEY` and `ELEVENLABS_API_KEY` without
putting values in committed files or shell history. The manifest's `local` secret defaults resolve
these variables into Golem secret configuration; agents receive opaque redacted handles.
Configure your ElevenLabs voice/model and other non-secret settings under `agents.CallAgent.config`
in `golem.yaml`, then:

```nu
$env.GOLEM_EFFECT_GOLEM_PATH = ($env.HOME | path join projects/golem/golem/sdks/effect)
golem deploy -Y -E local -P live
```

Open [the live-provider console](http://call-center.localhost:19006). Do **not** use `-P mock` for
this deployment. `live` is the explicit default agent preset; `integration` selects `mock` through
its environment configuration. Passing `-P live` also overrides a shell's `GOLEM_PRESET` setting.
Golem implicitly selects the first custom preset if no default is marked, so a lone `mock` preset
is not opt-in. After switching presets, use a fresh call ID: existing agents keep their deployed
revision/configuration. Verify provider URLs with `golem component get -E local` before a call.
Secret defaults seed missing values, not rotate existing values; use Golem's
secret-management commands if credentials already exist. Nothing here requires cloud deployment.
Use headphones to limit feedback. `*.localhost` is a browser secure context; remote hosting needs
HTTPS and appropriate authentication before exposing transcripts or audio.

Defaults: Flux `flux-general-en`, OpenAI `gpt-4o-mini`, ElevenLabs `eleven_flash_v2_5` with
`pcm_16000`. Microphone frames are mono PCM16 little-endian, 16 kHz, approximately 80 ms.
Resampling/low-pass filtering happens in the browser; the agent does no transcoding.

The default voice is George (`JBFqnCBsd6RMkjVDRZzb`), verified with the current free-plan key.
The previous Rachel ID returned HTTP 402 `paid_plan_required`: free users cannot use library
voices through the API. Starter should remove that restriction; higher tiers are not needed for
our 16 kHz PCM format. Account/voice availability can change: ElevenLabs documents that legacy
default voices expire December 31, 2026 and are limited to accounts created before March 2026.
See [voice availability](https://elevenlabs.io/docs/help-center/product/voices/my-voices/what-are-default-voices)
and [library restrictions](https://elevenlabs.io/docs/eleven-creative/voices/voice-library).
Empty synthesis reports a recoverable error instead of committing an unspoken assistant answer;
premature synthesis termination cancels generation. Use a fresh call ID after redeploying.

## Local recovery demo button

After deploying the live environment above, start the loopback-only controller:

```nu
node demo-server.mjs
```

Open [the recovery demo](http://localhost:19008), not the direct Golem URL. It proxies the
Golem-hosted console and Durable Streams and adds a same-origin crash endpoint. The ordinary
Golem console hides the demo controls. No management credentials are sent to the browser.
The controller requires the local `golem` executable and uses `GOLEM_EFFECT_GOLEM_PATH`, falling
back to `~/projects/golem/golem/sdks/effect`. It binds only loopback; do not expose it publicly.

Start a call and click **Crash agent** while keeping the page open. This invokes
`golem agent simulate-crash` for that call only; it does not kill the server. The button disables
while a request is pending and never automatically retries. Command completion is not proof
of recovery: continue speaking and check for another audible answer on the same call.
Speech may repeat. Recognition and synthesis crash scenarios pass fixture tests, but arbitrary
click timing can still hit GOL-906 during an OpenAI request. Start a fresh call if recovery fails.
Real-provider crash compatibility and whole-server restart are not verified.

For deterministic rehearsal, with the integration deployment and fixture running, use instead:

```nu
with-env { GOLEM_DEMO_ENV: "integration" } { node demo-server.mjs }
node test/browser-demo.mjs
```

Stop the existing controller before switching environments; both modes use port19008. The browser
test requires integration mode, exercises a real CLI crash and verifies a new completed/played
response, plus request rejection, pending/error feedback, hangup races and mobile layout.

## Streaming inference recovery experiment

The default path still streams OpenAI text in the call agent and can hit GOL-906. An opt-in
`isolatedInference` mode isolates HTTP in a separate durable `InferenceAgent(callId, responseId)`, returning
text deltas and explicit completion/failure items through streaming durable RPC. Recognition and
synthesis WebSockets remain in the call agent. Speech starts before inference completes, as in
the original direct path. The helper's model/API endpoint config is independent of the caller's
config. The coordinator supplies its existing stable response ID: replay uses the same helper,
but a new response never queues behind an abandoned one. This replaces the earlier buffered and
per-call streaming experiments; use fresh call IDs rather than updating their existing agents.
The saved call-state schema is unchanged.

The separate fixture-only deployment leaves the existing live demo unchanged. With the same
isolated server and protocol fixture running:

```nu
$env.GOLEM_EFFECT_GOLEM_PATH = ($env.HOME | path join projects/golem/golem/sdks/effect)
golem deploy -Y -E inference-recovery
with-env {
  GOLEM_TEST_ENV: "inference-recovery"
  GOLEM_TEST_ORIGIN: "http://call-center-recovery.localhost:19006"
  GOLEM_TEST_SCENARIO: "crash-openai,crash-inference,crash-both,crash-body,crash-helperbody,crash-bothbody"
  GOLEM_TEST_ATTEMPTS: "3"
} { node test/recovery-smoke.mjs }
```

`crash-openai` crashes the caller before HTTP headers; `crash-inference` crashes only the helper;
`crash-both` crashes caller then the original response-1 helper sequentially. Caller recovery may
already have started a replacement helper; this is not a simultaneous crash or a crash of that
replacement. The three body scenarios do the same after
the first delta has reached synthesis and browser-facing audio while the provider is still held.
All18 fresh crash attempts passed on rc2-68 with per-response inference isolation. Checks require
two distinct turns, expected text, nonempty audio with matching response IDs/completion, ACK
submission and hangup/output closure. `node test/inference-streaming.mjs` additionally checks early
audio, continuation after interrupt and hangup with HTTP still held (five cases, all passed).
The buffered and per-call streaming experiments have separate historical results in the plan.

Two explicit acceptance probes remain **failing**:

```nu
with-env { GOLEM_TEST_SCENARIO: "conflict-text,conflict-chunks" } { node test/inference-streaming.mjs }
```

These change the replayed prefix or its chunk segmentation after speech has begun. Golem rejects
the conflicting publication with EventConflict rather than splicing output. Its fatal remote
stream terminal also traps the consuming call, so the application cannot clear playback and
continue that same call through an Effect error handler. This is not transparent recovery of
arbitrary nondeterministic inference; a catchable remote-terminal contract needs discussion.

Open [the experimental console](http://call-center-recovery.localhost:19006) for fixture rehearsal.
The local crash-button controller remains configured for the original live demo, not this domain.
Remote inference may survive an interrupt/hangup and incur cost; cancellation is cooperative.
Helpers and their histories are retained per response, with no automatic deletion policy yet.
Exactly-once inference/billing is **not** promised.
Recovery of already published deltas requires a compatible event prefix: changing text or chunk
segmentation on HTTP retry may conflict with durable stream history. Do not splice a regenerated
suffix onto exposed speech. Real-provider crashes, full-server restart and repeated crashes within
one ongoing call remain unverified.

## Protocol and limits

`CallAgent(callId).run` exposes `input` and `output` Durable Stream slots under
`/calls/{callId}/run/invocations/{invocationId}`. The browser creates a session with PUT, POSTs
input batches with stable `Producer-Id`/`Producer-Epoch`/`Producer-Seq`, and follows output GETs
using the opaque `Stream-Next-Offset` cursor. Failed body reads retry the same cursor. Retries are
bounded; prolonged outages stop the call rather than promise transparent reconnection.

Effect schemas in `src/call-protocol.ts` own the contract. HTTP uses **WIT variant JSON**, not
the in-process `_tag` representation: `[{"audio":{"sequence":0,"capturedAt":0,"payload":"AAA="}}]`,
`[{"playbackAck":{"responseId":"response-1","playedThrough":0}}]`, `["interrupt"]`, `["hangup"]`.
Audio payloads are base64. Output carries transcripts, response-tagged audio, assistant text,
clear-playback controls, interrupt confirmation, response completion and sanitized stage failures.

Only committed Flux `EndOfTurn` starts generation. `StartOfTurn` invalidates the active response;
explicit interrupt clears local playback immediately and suppresses audio until agent confirmation.
Only naturally completed, contiguous audio advances heard progress—download/scheduling is not
playback. Assistant text appears after synthesis finalization, not token by token.

Agent queues are bounded; browser append backlog is capped at 64 requests. Playback schedules at
most 64 sources / 4 seconds, splitting provider chunks into at most one-second slices. Long replies
and bursts wait for scheduling capacity instead of failing. One ordered output cursor paces audio
and handles UI/confirmation; a second cursor immediately handles clear-playback, fatal failure and
terminal closure. Confirmation only unmutes on the ordered cursor. This downloads output twice;
HTTP response batches are still materialized, so the scheduling limit is **not** an end-to-end
memory bound. Each decoded provider audio frame is limited to 8 MiB. Input overload, oversized or
malformed audio still fails the call. EOF cancels playback, rather than draining obsolete speech.
Output retention is handled by Golem. Conversation history grows with committed turns; there is
no production retention/context-trimming policy.
Scoped cleanup handles hangup/failure; closing the tab makes only a best-effort hangup request.

Run `node test/browser-buffering.mjs` serially with the integration deployment and fixture. It
checks a 12.25-second chunk, a 100-chunk burst, whole-chunk playback ACKs, local interrupt,
server barge-in, fatal provider failure and hangup while scheduling is blocked.

The metrics “Final transcript received → first audio scheduled” and “Local playback stop dispatch”
are browser-side proxies. They do not measure microphone end-of-turn, acoustic first sound, or
interruption-to-silence. No production latency or speech-quality result is claimed.

## Recovery acceptance and release findings

Run serially against the integration deployment and fixture (its control endpoint is shared):

```nu
node test/recovery-smoke.mjs
```

Delayed reading (1.5 seconds), provider failures and explicit interruption during STT/OpenAI/TTS
are tested separately from simulated crashes. The script asserts failures rather than hiding them.
The older CLI's fixed crash batch (three fresh agents per stage) produced one STT continuation under earlier,
weaker checks and eight stalls. That continuation is not a strict-check acceptance pass. The
runner now requires fresh-session evidence, matching text/audio/completion, a mandatory ACK,
a distinct second committed turn and explicit hangup closure. It exits nonzero on any failure,
including failed cleanup; evidence goes to unique `.amp/in/recovery-runs/<call-id>/` directories.
Select a scenario with `with-env { GOLEM_TEST_SCENARIO: "interrupt-openai" } { node test/recovery-smoke.mjs }`.
Use a fixed `GOLEM_TEST_ATTEMPTS` from 1–3, never retry until green. Comma-separated scenario names
are accepted. `continue-call` is the non-crash control for two distinct answers, ACKs and closure.

Latest verification on `v1.6.0-rc2-68-g3b7efa521e`: typecheck and 99 unit tests pass. The
fixed batch passes `continue-call`, `replay-stt`, `crash-stt` and `crash-elevenlabs` three times
each, with strict two-turn/ACK/closure assertions for the continuing calls. All seven slow-reader,
provider-failure and interruption controls pass. All three `crash-openai` attempts fail with
`Non-idempotent remote write operation was not completed, cannot retry`, reaching terminal Failed
rather than the old stall. GOL-906 tracks the P3 HTTP batched-scope rejection of a concurrent
WebSocket receive. No application idempotence override or manual retry was added. CLI oplog
inspection also fails with a separate `missing field onRight` decoder error (GOL-907).

Previously, on CLI `v1.6.0-rc2-25-gfd734d5670` with the rebuilt local SDK, 99 unit tests, typecheck,
normal deployment smoke, provider-failure/interruption and strict non-crash continuation pass.
All six browser buffering scenarios also pass on a fresh isolated server using controlled
Web Audio capture; automated native microphone permission remains unverified. An earlier
browser hangup timeout followed a recorded memory-pressure restart, not a failure-free stall.
One fixed fresh-server crash attempt per provider stage still timed out without an OOM;
these failures and their unconfirmed hangup cleanup are retained, not counted as acceptance.
Deepgram uses ten seconds of retained PCM and at most 1.25× replay; overflow asks for a repeat
and suppresses the damaged fragment. TTS recovery fences physical epochs, clears old playback,
and preserves old ACK/audio records. Complete text is reused; incomplete foreground OpenAI text
is regenerated, not suffix-spliced. Ordinary provider errors are not unlimited reconnect triggers.
Repeated host crashes, deployed partial-playback/ACK races, deployed overflow and real-provider
crash compatibility remain unverified. An answer may repeat; browser reload and exactly-once
hearing are not supported. See the plan's dated checkpoints for retained evidence and reviews.

Historical observations on CLI/server `v1.6.0-rc1-28-g5948c58e5f` and local SDK at Golem revision
`5948c58e5f682823f34f53083db12f9f0b1e456d`:

- GOL-742's missing-span trap and GOL-750's live-input stall are fixed. Repeated fixture-backed calls and
  ten fresh reduced echo probes pass, including output closure.
- Completed call state survives same-version server restart; an explicit SDK restore marker and
  ended-call rejection verify snapshot restoration. This is **not** active-call recovery evidence.
- **GOL-799:** simulate-crash during STT, OpenAI or TTS stalls replay. A reduced duplex string-echo
  agent reproduces the failure without AI, custom snapshots, or a coordinator. Its post-crash input
  append succeeds but no second echo/output closure arrives. Root cause remains unestablished.
  See [`test/stream-stall-repro/README.md`](./test/stream-stall-repro/README.md) for commands.
- GOL-739/GOL-736 static-contract extraction limitations have localized config/validation
  workarounds. Snapshot shape stays structural; save/restore validates cross-record invariants.
- Older unreleased server data caused a compressed-oplog panic (GOL-797). That data directory is
  preserved separately; compatibility across development commits is not assumed.

Reports were checked for duplicates in **Golem 1.6 finalization** and phrased as general Golem
issues. No SDK changes, cloud deployment, push or merge were performed. Active-call replay and
live-provider testing remain acceptance gates; transparent provider-session continuation is not
a v1 guarantee even after the runtime blocker is fixed.
