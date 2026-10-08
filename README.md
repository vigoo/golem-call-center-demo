# Call Center

A durable Effect Golem call agent: Deepgram Flux → Effect AI / OpenAI Responses → ElevenLabs
streaming TTS. A static-only ephemeral Golem HTTP router hosts the same-origin browser console.
See [`DEVELOPMENT_PLAN.md`](./DEVELOPMENT_PLAN.md) for the design and commit-by-commit history.

**Status:** the protocol-fixture pipeline and browser work locally. Active-call crash recovery is
blocked by [GOL-799](https://linear.app/golem-cloud/issue/GOL-799/effect-duplex-agentstream-replay-stalls-after-simulate-crash-with).
Live recognition, LLM responses and audio playback were user-tested. Long-response buffering is
also tested against protocol fixtures using real Chrome playback and Golem streams. Acoustic
latency remains unmeasured. This is an unauthenticated development prototype, not a production
telephony service.

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
Currently all three crash scenarios stall; the command exits nonzero. Logs/events go to `.amp/in/`.
Select a scenario with `with-env { GOLEM_TEST_SCENARIO: "interrupt-openai" } { node test/recovery-smoke.mjs }`.

Observed on CLI/server `v1.6.0-rc1-28-g5948c58e5f` and local SDK at Golem revision
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
