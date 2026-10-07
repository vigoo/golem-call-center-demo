# Live stream delivery stall reproduction

Reduced reproduction for [GOL-750](https://linear.app/golem-cloud/issue/GOL-750/concurrent-websocket-receives-can-stall-live-durable-stream-input).
It has no AI provider, credentials, snapshots, application state machine, or output queue.
A durable string-echo agent opens two WebSockets, forks their receive loops, then echoes each
Durable Stream input item after a binary socket write. All resources belong to the output stream scope.

Run from the repository root with dependencies installed and the isolated local server already
listening on management port 19881 / HTTP port 19006. This manifest intentionally shares that
server but uses a separate app and `stream-stall-repro.localhost` deployment. Do not start a second
server or clean its data. Start the protocol fixture in another terminal if it is not already running:

```nu
node test/provider-fixture.mjs
```

Then:

```nu
npm install --prefix test/stream-stall-repro --package-lock=false
with-env { GOLEM_EFFECT_GOLEM_PATH: ($env.HOME | path join projects/golem/golem/sdks/effect) } {
  golem deploy -A test/stream-stall-repro/golem.yaml -Y
}
node test/stream-stall-repro/probe.mjs
```

Use the explicit manifest path: normal discovery selects the parent application instead.
The fixture is only a local WebSocket peer here. The probe uses `/v2/listen` (one initial text
frame and binary-write support) and `/tts` (accept and wait); its OpenAI/ElevenLabs behavior is unused.

Expected: ten fresh invocations echo `hello`, with normal completion. Actual on
`golem v1.6.0-rc1-16-g17a2a589f8` and the matching local SDK: first invocation passed; second
remained Running with retry count 0 and no echo within the ten-second probe deadline. The failing
oplog recorded `ConsumerItemValue` but no `Input handler entered` log and no binary send.
This does not prove a particular lost wakeup or host scheduling cause.

Inspect the failed ID printed by the probe (replace the UUID):

```nu
with-env { GOLEM_EFFECT_GOLEM_PATH: ($env.HOME | path join projects/golem/golem/sdks/effect) } {
  golem agent get -A test/stream-stall-repro/golem.yaml 'StreamProbe("UUID")'
  golem agent oplog -A test/stream-stall-repro/golem.yaml 'StreamProbe("UUID")'
}
```

The probe deliberately leaves the failed invocation intact for oplog inspection. Do not report
an intermittent passing rerun as a fix. Bound the runs; accumulated open test agents consume
resources, and admission-blocked Idle agents are a different failure from the Running stall.
