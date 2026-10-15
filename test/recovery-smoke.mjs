// Destructive only to the fresh test agents created here; never clears server data.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setDefaultResultOrder } from "node:dns";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, writeFile } from "node:fs/promises";
setDefaultResultOrder("ipv4first");
const environment = process.env.GOLEM_TEST_ENV ?? "integration";
assert.ok(environment === "integration" || process.env.GOLEM_TEST_ORIGIN, "Non-default environments require an explicit matching GOLEM_TEST_ORIGIN");
const origin = process.env.GOLEM_TEST_ORIGIN ?? "http://call-center-test.localhost:19006";
const fixture = "http://127.0.0.1:19110/test/control";
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const fixtureRequest = async (settings) => {
  const response = await fetch(fixture, { signal: AbortSignal.timeout(10000),
    ...(settings === undefined ? {} : { method: "POST", body: JSON.stringify(settings) }),
  });
  assert.ok(response.ok, `Fixture control: ${response.status}`);
  return response.json();
};
const run = promisify(execFile);
const failures = [];
const attempts = Number(process.env.GOLEM_TEST_ATTEMPTS ?? 1);
assert.ok(Number.isInteger(attempts) && attempts >= 1 && attempts <= 3, "Use one to three fixed fresh-agent attempts, never retry until green");

const inferenceScenarios = ["crash-inference", "crash-both", "crash-body", "crash-helperbody", "crash-bothbody"];
const scenarios = ["continue-call", "slow-reader", "failure-stt", "failure-openai", "failure-elevenlabs", "interrupt-stt", "interrupt-openai", "interrupt-elevenlabs", "replay-stt", "crash-stt", "crash-openai", "crash-elevenlabs", ...inferenceScenarios];
const selected = process.env.GOLEM_TEST_SCENARIO?.split(",") ?? scenarios.filter((s) => !inferenceScenarios.includes(s));
assert.ok(selected.every((scenario) => scenarios.includes(scenario)), "Unknown recovery scenario");
assert.ok(environment === "inference-recovery" || !selected.some((s) => inferenceScenarios.includes(s)), "Inference-agent crash scenarios require the buffered inference-recovery deployment");
for (const scenario of selected) {
  for (let attempt = 1; attempt <= attempts; attempt++) {
  const [kind, targetStage] = scenario.split("-");
  const bodyCrash = ["body", "helperbody", "bothbody"].includes(targetStage);
  const stage = bodyCrash ? "openai-body" : targetStage === "inference" || targetStage === "both" ? "openai" : targetStage;
  const callId = `${scenario}-${randomUUID()}`;
  const evidence = `.amp/in/recovery-runs/${callId}`;
  await mkdir(evidence, { recursive: true });
  await writeFile(`${evidence}/metadata.json`, JSON.stringify({ callId, scenario, attempt, environment, origin, startedAt: new Date().toISOString() }, null, 2));
  const base = `${origin}/calls/${callId}/run/invocations/${randomUUID()}`;
  const request = async (path, options = {}) => {
    const response = await fetch(base + path, { ...options, headers: { "Content-Type": "application/json", ...options.headers }, signal: AbortSignal.timeout(10000) });
    assert.ok(response.ok, `${scenario} ${path}: ${response.status}`); return response;
  };
  let sequence = 0;
  const append = (events, close = false) => request("/streams/input", {
    method: "POST", body: JSON.stringify(events), headers: { "Producer-Id": "recovery", "Producer-Epoch": "0", "Producer-Seq": String(sequence++), ...(close ? { "Stream-Closed": "true" } : {}) },
  });
  const events = [];
  let outcome = "failed";
  let acknowledgedAudio = null;
  let offset = "-1", closed = false;
  const poll = async () => {
    const response = await request(`/streams/output?offset=${encodeURIComponent(offset)}`);
    offset = response.headers.get("Stream-Next-Offset"); assert.ok(offset);
    events.push(...await response.json()); closed = response.headers.get("Stream-Closed") === "true";
  };
  try {
    await fixtureRequest(kind === "failure" ? { failStage: stage } : kind === "crash" || kind === "interrupt" || kind === "replay" ? { holdStage: stage } : {});
    await request("", { method: "PUT", body: "{}" });
    await append([{ audio: { sequence: 0, capturedAt: Date.now(), payload: Buffer.alloc(2560).toString("base64") } }]);
    if (kind === "crash" || kind === "interrupt" || kind === "replay") {
      const deadline = Date.now() + 15000;
      while ((await fixtureRequest()).pending[stage] === 0) { assert.ok(Date.now() < deadline, `${stage} not held`); await pause(50); }
    }
    if (kind === "crash" || kind === "replay") {
      if (bodyCrash) {
        await pause(500);
        await poll();
        assert.ok(!events.some((event) => event.audio || event.assistantText || event.responseFinished), "Buffered inference exposed an incomplete prefix");
      }
      const targets = targetStage === "both" || targetStage === "bothbody" ? ["CallAgent", "InferenceAgent"]
        : targetStage === "inference" || targetStage === "helperbody" ? ["InferenceAgent"] : ["CallAgent"];
      for (const target of targets) {
        const { stdout, stderr } = await run("golem", ["agent", "simulate-crash", `${target}(${JSON.stringify(callId)})`, "-E", environment, "-Y"], {
          env: { ...process.env, GOLEM_EFFECT_GOLEM_PATH: `${process.env.HOME}/projects/golem/golem/sdks/effect` }, timeout: 30000,
        });
        await writeFile(`${evidence}/crash-${target}.log`, stdout + stderr);
      }
      if (kind === "crash") await fixtureRequest({ release: true });
    }
    if (kind === "slow") await pause(1500);
    const deadline = Date.now() + 20000;
    if (kind === "replay") {
      // Runtime liveness is independent of restoring an old provider session: fresh controls
      // must be processed even if recognition has no new audio and no transcript to return.
      await append(["interrupt"]);
      while (!closed && !events.includes("interruptConfirmed")) { assert.ok(Date.now() < deadline, "Replay did not process a fresh interrupt"); await poll(); await pause(50); }
      assert.ok(events.includes("interruptConfirmed"));
      assert.ok(!closed, "Replay liveness check prematurely closed the call");
    } else if (kind === "interrupt") {
      await append(["interrupt"]);
      while (!closed && !events.includes("interruptConfirmed")) { assert.ok(Date.now() < deadline, `${scenario} interrupt not confirmed`); await poll(); await pause(50); }
      assert.ok(events.includes("interruptConfirmed"));
      assert.ok(!closed, "Interrupt prematurely closed the call");
      if (stage === "stt") assert.ok(!events.some((e) => e.transcript), "STT turn arrived before interruption");
      // STT has not committed a turn yet; hang up before releasing future speech.
      // For LLM/TTS, deliberately release obsolete provider output after confirmation.
      if (stage !== "stt") {
        await fixtureRequest({ release: true });
        await pause(2500); await poll();
        assert.ok(!closed, "Interrupt destroyed the provider session");
        assert.ok(!events.some((e) => e.audio || e.assistantText || e.responseFinished), "Interrupted response emitted late output");
        assert.ok(events.some((e) => e.clearPlayback), "Active response was not cleared");
      }
    } else {
      while (!closed && !events.some((e) => e.responseFinished || e.failure)) { assert.ok(Date.now() < deadline, `${scenario} no response/failure after recovery`); await poll(); await pause(100); }
    }
    if (kind === "failure") assert.ok(events.some((e) => e.failure?.stage === (stage === "stt" ? "deepgram" : stage)), "Expected provider failure did not surface");
    if (kind === "crash" || kind === "continue") {
      assert.ok(events.some((e) => e.responseFinished) && !events.some((e) => e.failure), "Crash did not recover a completed response");
      assert.ok(!closed, "Recovered call closed before hangup");
      const provider = await fixtureRequest();
      await writeFile(`${evidence}/reconstructed-provider.json`, JSON.stringify(provider, null, 2));
      assert.ok(!provider.journal.some((entry) => entry.operation === "invalid-context"), "Fresh socket received uninitialized TTS context");
      if (stage === "stt") assert.ok(provider.journal.some((entry) => entry.stage === "stt" && entry.operation === "audio" && entry.connection > 1), "No PCM replay on a fresh recognition socket");
      if (stage === "elevenlabs") {
        const finished = events.find((event) => event.responseFinished).responseFinished.responseId;
        assert.notEqual(finished, "response-1", "Lost synthesis attempt was not replaced");
        assert.ok(provider.journal.some((entry) => entry.operation === "initialize" && entry.connection > 1 && entry.responseId === finished), "Replacement synthesis context was not initialized on a fresh socket");
      }
      // Continued conversation proves the recovered call is usable, not just one callback.
      const first = events.find((event) => event.responseFinished).responseFinished.responseId;
      const firstAudio = events.filter((event) => event.audio?.responseId === first).at(-1)?.audio;
      assert.ok(firstAudio && Buffer.from(firstAudio.payload, "base64").length > 0, "Recovered answer has no playable audio");
      assert.ok(events.some((event) => event.assistantText?.responseId === first && event.assistantText.text === "Hello from the fixture."), "Recovered answer has incorrect text");
      assert.equal(events.filter((event) => event.responseFinished).length, 1, "Recovery committed extra answers before the next input");
      const firstTurn = events.find((event) => event.transcript?.final)?.transcript.turnId;
      assert.ok(firstTurn, "Recovered answer has no committed user transcript");
      await append([{ playbackAck: { responseId: first, playedThrough: firstAudio.sequence } }]);
      await pause(1100);
      const boundary = events.length;
      const nextAudio = Buffer.alloc(2560); nextAudio.writeInt16LE(1234, 0);
      await append([{ audio: { sequence: 1, capturedAt: Date.now(), payload: nextAudio.toString("base64") } }]);
      while (!events.slice(boundary).some((event) => event.responseFinished?.responseId !== undefined && event.responseFinished.responseId !== first)) {
        assert.ok(!closed && Date.now() < deadline, "Recovered call did not complete a second turn");
        await poll(); await pause(100);
      }
      assert.ok(!closed, "Second turn closed the call before hangup");
      const continued = events.slice(boundary);
      assert.ok(continued.some((event) => event.transcript?.final && event.transcript.turnId !== firstTurn && event.transcript.text === "Second fixture turn"), "Fresh PCM did not produce a distinct committed second turn");
      const second = continued.find((event) => event.responseFinished?.responseId !== undefined && event.responseFinished.responseId !== first).responseFinished.responseId;
      assert.ok(continued.some((event) => event.assistantText?.responseId === second && event.assistantText.text === "Hello from the fixture."), "Second answer has incorrect text");
      acknowledgedAudio = continued.filter((event) => event.audio?.responseId === second).at(-1)?.audio;
      assert.ok(acknowledgedAudio && Buffer.from(acknowledgedAudio.payload, "base64").length > 0, "Second answer has no playable audio");
    }
    if (kind === "slow") assert.ok(events.some((e) => e.audio) && events.some((e) => e.assistantText), "Slow reader lost completed response");
    if (kind === "slow") assert.ok(!closed, "Slow reader call closed before hangup");
    if (!closed) {
      const audio = acknowledgedAudio ?? events.filter((e) => e.audio).at(-1)?.audio;
      await append([...(audio ? [{ playbackAck: { responseId: audio.responseId, playedThrough: audio.sequence } }] : []), "hangup"], true);
      while (!closed) { assert.ok(Date.now() < deadline, `${scenario} output did not close`); await poll(); await pause(100); }
    }
    if (kind === "slow" || kind === "interrupt" || kind === "crash" || kind === "continue") assert.ok(!events.some((e) => e.failure), "Successful scenario emitted a failure during final drain");
    if (kind === "interrupt") assert.ok(!events.some((e) => e.audio || e.assistantText || e.responseFinished), "Interrupted scenario emitted response output during final drain");
    if (kind === "crash" || kind === "continue") assert.equal(events.filter((e) => e.responseFinished).length, 2, "Continuation committed extra answers during final drain");
    console.log(`${scenario}: closed; events=${events.map((e) => typeof e === "string" ? e : Object.keys(e)[0]).join(",")}`);
    outcome = "passed";
  } catch (error) {
    failures.push(callId); console.error(`${scenario}: FAILED ${error.message}; ${callId}`);
    await writeFile(`${evidence}/error.txt`, error.stack ?? String(error));
  } finally {
    await writeFile(`${evidence}/events.json`, JSON.stringify(events, null, 2));
    try {
      await writeFile(`${evidence}/provider.json`, JSON.stringify(await fixtureRequest(), null, 2));
    } catch (error) {
      outcome = "failed";
      if (!failures.includes(callId)) failures.push(callId);
      await writeFile(`${evidence}/fixture-error.txt`, error.stack ?? String(error));
    }
    if (!closed) {
      try {
        await append(["hangup"], true);
        const deadline = Date.now() + 10000;
        while (!closed) { assert.ok(Date.now() < deadline, "Hangup persisted but output did not close"); await poll(); await pause(100); }
      } catch (error) {
        console.error(`Unconfirmed cleanup for ${callId}: ${error.message}`);
        outcome = "failed";
        if (!failures.includes(callId)) failures.push(callId);
        await writeFile(`${evidence}/cleanup-error.txt`, error.stack ?? String(error));
      }
    }
    await writeFile(`${evidence}/events.json`, JSON.stringify(events, null, 2));
    await writeFile(`${evidence}/outcome.json`, JSON.stringify({ outcome, closed }, null, 2));
    await fixtureRequest({});
  }
  }
}
assert.deepEqual(failures, [], "Recovery scenarios failed; inspect the retained logs");
