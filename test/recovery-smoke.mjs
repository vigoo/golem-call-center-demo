// Destructive only to the fresh test agents created here; never clears server data.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setDefaultResultOrder } from "node:dns";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { writeFile } from "node:fs/promises";
setDefaultResultOrder("ipv4first");
const origin = process.env.GOLEM_TEST_ORIGIN ?? "http://call-center-test.localhost:19006";
const fixture = "http://127.0.0.1:19110/test/control";
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const run = promisify(execFile);
const failures = [];

for (const scenario of ["slow-reader", "failure-stt", "failure-openai", "failure-elevenlabs", "interrupt-stt", "interrupt-openai", "interrupt-elevenlabs", "crash-stt", "crash-openai", "crash-elevenlabs"]) {
  if (process.env.GOLEM_TEST_SCENARIO && scenario !== process.env.GOLEM_TEST_SCENARIO) continue;
  const [kind, stage] = scenario.split("-");
  await fetch(fixture, { method: "POST", body: JSON.stringify(kind === "failure" ? { failStage: stage } : kind === "crash" || kind === "interrupt" ? { holdStage: stage } : {}) });
  const callId = `${scenario}-${randomUUID()}`;
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
  let offset = "-1", closed = false;
  const poll = async () => {
    const response = await request(`/streams/output?offset=${encodeURIComponent(offset)}`);
    offset = response.headers.get("Stream-Next-Offset"); assert.ok(offset);
    events.push(...await response.json()); closed = response.headers.get("Stream-Closed") === "true";
  };
  try {
    await request("", { method: "PUT", body: "{}" });
    await append([{ audio: { sequence: 0, capturedAt: Date.now(), payload: "AAA=" } }]);
    if (kind === "crash" || kind === "interrupt") {
      const deadline = Date.now() + 15000;
      while ((await (await fetch(fixture)).json()).pending[stage] === 0) { assert.ok(Date.now() < deadline, `${stage} not held`); await pause(50); }
    }
    if (kind === "crash") {
      const { stdout, stderr } = await run("golem", ["agent", "simulate-crash", `CallAgent(${JSON.stringify(callId)})`, "-E", "integration", "-Y"], {
        env: { ...process.env, GOLEM_EFFECT_GOLEM_PATH: `${process.env.HOME}/projects/golem/golem/sdks/effect` }, timeout: 30000,
      });
      await writeFile(`.amp/in/${scenario}.log`, stdout + stderr);
      await fetch(fixture, { method: "POST", body: '{"release":true}' });
    }
    if (kind === "slow") await pause(1500);
    const deadline = Date.now() + 20000;
    if (kind === "interrupt") {
      await append(["interrupt"]);
      while (!closed && !events.includes("interruptConfirmed")) { assert.ok(Date.now() < deadline, `${scenario} interrupt not confirmed`); await poll(); await pause(50); }
      assert.ok(events.includes("interruptConfirmed"));
      assert.ok(!closed, "Interrupt prematurely closed the call");
      if (stage === "stt") assert.ok(!events.some((e) => e.transcript), "STT turn arrived before interruption");
      // STT has not committed a turn yet; hang up before releasing future speech.
      // For LLM/TTS, deliberately release obsolete provider output after confirmation.
      if (stage !== "stt") {
        await fetch(fixture, { method: "POST", body: '{"release":true}' });
        await pause(2500); await poll();
        assert.ok(!closed, "Interrupt destroyed the provider session");
        assert.ok(!events.some((e) => e.audio || e.assistantText || e.responseFinished), "Interrupted response emitted late output");
        assert.ok(events.some((e) => e.clearPlayback), "Active response was not cleared");
      }
    } else {
      while (!closed && !events.some((e) => e.responseFinished || e.failure)) { assert.ok(Date.now() < deadline, `${scenario} no response/failure after recovery`); await poll(); await pause(100); }
    }
    if (kind === "failure") assert.ok(events.some((e) => e.failure?.stage === (stage === "stt" ? "deepgram" : stage)), "Expected provider failure did not surface");
    if (kind === "crash") assert.ok(events.some((e) => e.responseFinished) && !events.some((e) => e.failure), "Crash did not recover a completed response");
    if (kind === "slow") assert.ok(events.some((e) => e.audio) && events.some((e) => e.assistantText), "Slow reader lost completed response");
    if (kind === "slow") assert.ok(!closed, "Slow reader call closed before hangup");
    if (!closed) {
      const audio = events.filter((e) => e.audio).at(-1)?.audio;
      await append([...(audio ? [{ playbackAck: { responseId: audio.responseId, playedThrough: audio.sequence } }] : []), "hangup"], true);
      while (!closed) { assert.ok(Date.now() < deadline, `${scenario} output did not close`); await poll(); await pause(100); }
    }
    if (kind === "slow" || kind === "interrupt" || kind === "crash") assert.ok(!events.some((e) => e.failure), "Successful scenario emitted a failure during final drain");
    if (kind === "interrupt") assert.ok(!events.some((e) => e.audio || e.assistantText || e.responseFinished), "Interrupted scenario emitted response output during final drain");
    console.log(`${scenario}: closed; events=${events.map((e) => typeof e === "string" ? e : Object.keys(e)[0]).join(",")}`);
    await writeFile(`.amp/in/${scenario}-events.json`, JSON.stringify(events, null, 2));
  } catch (error) {
    failures.push(scenario); console.error(`${scenario}: FAILED ${error.message}; ${callId}`);
  } finally {
    await writeFile(`.amp/in/${scenario}-events.json`, JSON.stringify(events, null, 2));
    await fetch(fixture, { method: "POST", body: "{}" });
    if (!closed) await request("/streams/input", { method: "POST", headers: { "Stream-Closed": "true" }, body: '["hangup"]' }).catch((error) => console.error(`Unconfirmed cleanup: ${error.message}`));
  }
}
assert.deepEqual(failures, [], "Recovery scenarios failed; inspect the retained logs");
