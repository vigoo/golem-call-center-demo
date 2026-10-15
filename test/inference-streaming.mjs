// Public streaming-RPC lifecycle checks against the isolated fixture deployment.
// Use fresh identities only. Never release the obsolete HTTP request in cancel/hangup checks.
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { setDefaultResultOrder } from "node:dns";
import { mkdir, writeFile } from "node:fs/promises";
import { promisify } from "node:util";

setDefaultResultOrder("ipv4first");
const origin = process.env.GOLEM_TEST_ORIGIN ?? "http://call-center-recovery.localhost:19006";
const environment = process.env.GOLEM_TEST_ENV ?? "inference-recovery";
assert.equal(environment, "inference-recovery", "These checks require the isolated streaming deployment");
const selected = process.env.GOLEM_TEST_SCENARIO?.split(",") ?? ["prefix", "cancel-headers", "cancel-body", "hangup-headers", "hangup-body"];
assert.ok(selected.every((s) => ["prefix", "cancel-headers", "cancel-body", "hangup-headers", "hangup-body", "conflict-text", "conflict-chunks"].includes(s)));
const run = promisify(execFile);
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const control = async (settings) => {
  const response = await fetch("http://127.0.0.1:19110/test/control", {
    signal: AbortSignal.timeout(10000),
    ...(settings === undefined ? {} : { method: "POST", body: JSON.stringify(settings) }),
  });
  assert.ok(response.ok); return response.json();
};
const failures = [];
for (const scenario of selected) {
  const callId = `stream-${scenario}-${randomUUID()}`;
  const evidence = `.amp/in/streaming-contract-runs/${callId}`;
  await mkdir(evidence, { recursive: true });
  await writeFile(`${evidence}/metadata.json`, JSON.stringify({ callId, scenario, environment, origin, startedAt: new Date().toISOString() }, null, 2));
  const base = `${origin}/calls/${callId}/run/invocations/${randomUUID()}`;
  const events = [];
  let offset = "-1", sequence = 0, closed = false, outcome = "failed";
  const request = async (path, options = {}) => {
    const response = await fetch(base + path, { ...options, signal: AbortSignal.timeout(10000) });
    assert.ok(response.ok, `${path}: ${response.status}`); return response;
  };
  const append = (input, close = false) => request("/streams/input", {
    method: "POST", body: JSON.stringify(input), headers: { "Content-Type": "application/json",
      "Producer-Id": "stream-contract", "Producer-Epoch": "0", "Producer-Seq": String(sequence++),
      ...(close ? { "Stream-Closed": "true" } : {}),
    },
  });
  const poll = async () => {
    const response = await request(`/streams/output?offset=${encodeURIComponent(offset)}`);
    offset = response.headers.get("Stream-Next-Offset"); assert.ok(offset);
    events.push(...await response.json()); closed = response.headers.get("Stream-Closed") === "true";
  };
  const waitFor = async (condition, description) => {
    const deadline = Date.now() + 20000;
    while (!condition()) {
      assert.ok(Date.now() < deadline, description);
      await poll(); await pause(50);
    }
  };
  const audio = (n) => {
    const pcm = Buffer.alloc(2560); if (n) pcm.writeInt16LE(1234, 0);
    return { audio: { sequence: n, capturedAt: Date.now(), payload: pcm.toString("base64") } };
  };
  try {
    const conflict = scenario.startsWith("conflict-");
    const body = !scenario.endsWith("headers");
    const stage = body ? "openai-body" : "openai";
    await control({ holdStage: stage, holdFirstOnly: !conflict, ...(conflict ? { retryVariant: scenario.split("-")[1] } : {}) });
    await request("", { method: "PUT", body: "{}", headers: { "Content-Type": "application/json" } });
    await append([audio(0)]);
    const heldDeadline = Date.now() + 15000;
    while (!(await control()).pending[stage]) { assert.ok(Date.now() < heldDeadline, "HTTP was not held"); await pause(50); }
    if (body) {
      await waitFor(() => events.some((event) => event.audio), "No audio before inference completion");
      const provider = await control();
      assert.ok(provider.pending[stage] > 0);
      assert.ok(provider.journal.some((entry) => entry.operation === "text" && entry.text === "Hello "));
      assert.ok(!events.some((event) => event.assistantText || event.responseFinished), "Incomplete answer committed");
      await writeFile(`${evidence}/prefix.json`, JSON.stringify({ events, provider }, null, 2));
    }
    if (scenario.startsWith("hangup-")) {
      await append(["hangup"], true);
      await waitFor(() => closed, "Hangup did not close output with HTTP held");
      assert.ok(!events.some((event) => event.failure || event.responseFinished));
    } else {
      if (scenario.startsWith("cancel-")) {
        await append(["interrupt"]);
        await waitFor(() => events.includes("interruptConfirmed"), "Interrupt was not confirmed");
        assert.ok(!closed && events.some((event) => event.clearPlayback));
      } else if (conflict) {
        const { stdout, stderr } = await run("golem", ["agent", "simulate-crash", `InferenceAgent(${JSON.stringify(callId)})`, "-E", environment, "-Y"], {
          env: { ...process.env, GOLEM_EFFECT_GOLEM_PATH: `${process.env.HOME}/projects/golem/golem/sdks/effect` }, timeout: 30000,
        });
        await writeFile(`${evidence}/crash.log`, stdout + stderr);
        await control({ release: true });
        await waitFor(() => closed || events.some((event) => event.failure || event.responseFinished), "Divergent replay never became a terminal result");
        assert.ok(!closed && events.some((event) => event.failure?.stage === "openai"), "Divergent prefix was not a recoverable generation failure");
        assert.ok(!events.some((event) => event.assistantText || event.responseFinished), "Conflicting response was committed");
        assert.ok(events.some((event) => event.clearPlayback));
      } else {
        await control({ release: true });
        await waitFor(() => events.some((event) => event.responseFinished), "First response did not complete");
      }
      const boundary = events.length;
      const obsolete = events.find((event) => event.clearPlayback)?.clearPlayback.responseId;
      const firstTurn = events.find((event) => event.transcript?.final)?.transcript.turnId;
      assert.ok(firstTurn);
      const played = events.filter((event) => event.audio).at(-1)?.audio;
      if (played) await append([{ playbackAck: { responseId: played.responseId, playedThrough: played.sequence } }]);
      await pause(1100);
      await append([audio(1)]);
      await waitFor(() => closed || events.slice(boundary).some((event) => event.responseFinished), "Second turn blocked behind obsolete inference");
      assert.ok(!closed);
      const continued = events.slice(boundary);
      const finished = continued.find((event) => event.responseFinished)?.responseFinished.responseId;
      assert.ok(finished && finished !== obsolete);
      assert.ok(continued.some((event) => event.transcript?.final && event.transcript.turnId !== firstTurn && event.transcript.text === "Second fixture turn"));
      assert.ok(continued.some((event) => event.assistantText?.responseId === finished && event.assistantText.text === "Hello from the fixture."));
      const lastAudio = continued.filter((event) => event.audio?.responseId === finished).at(-1)?.audio;
      assert.ok(lastAudio && Buffer.from(lastAudio.payload, "base64").length > 0);
      assert.ok(!continued.some((event) => event.failure || (obsolete && (event.audio?.responseId === obsolete || event.responseFinished?.responseId === obsolete))));
      await append([{ playbackAck: { responseId: finished, playedThrough: lastAudio.sequence } }, "hangup"], true);
      await waitFor(() => closed, "Hangup did not close output");
      assert.equal(events.filter((event) => event.responseFinished).length, scenario === "prefix" ? 2 : 1);
      assert.equal(events.filter((event) => event.failure).length, conflict ? 1 : 0);
    }
    outcome = "passed";
    console.log(`${scenario}: passed; ${callId}`);
  } catch (error) {
    failures.push(callId); console.error(`${scenario}: FAILED ${error.message}; ${callId}`);
    await writeFile(`${evidence}/error.txt`, error.stack ?? String(error));
  } finally {
    await writeFile(`${evidence}/provider.json`, JSON.stringify(await control(), null, 2));
    if (!closed) {
      try {
        await append(["hangup"], true);
        await waitFor(() => closed, "Cleanup output did not close");
      } catch (error) {
        console.error(`Unconfirmed cleanup: ${callId}: ${error.message}`);
        await writeFile(`${evidence}/cleanup-error.txt`, error.stack ?? String(error));
      }
    }
    await writeFile(`${evidence}/events.json`, JSON.stringify(events, null, 2));
    await writeFile(`${evidence}/outcome.json`, JSON.stringify({ outcome, closed }, null, 2));
    // Release only after recording the scenario outcome. Let the abandoned producer finish
    // before reset: destroying its held HTTP response can otherwise retry into the next test.
    await control({ release: true });
    const cleanupDeadline = Date.now() + 15000;
    while (true) {
      const { stdout } = await run("golem", ["agent", "get", `InferenceAgent(${JSON.stringify(callId)})`, "-E", environment, "-F", "json"], {
        env: { ...process.env, GOLEM_EFFECT_GOLEM_PATH: `${process.env.HOME}/projects/golem/golem/sdks/effect` }, timeout: 10000,
      });
      const metadata = JSON.parse(stdout).metadata;
      await writeFile(`${evidence}/helper-cleanup.json`, JSON.stringify(metadata, null, 2));
      if (["Idle", "Failed", "Interrupted", "Exited"].includes(metadata.status) && metadata.pendingInvocationCount === 0) break;
      assert.ok(Date.now() < cleanupDeadline, "Abandoned helper did not settle after cleanup release");
      await pause(100);
    }
    await control({});
  }
}
assert.deepEqual(failures, [], "Streaming contract scenarios failed; inspect retained evidence");
