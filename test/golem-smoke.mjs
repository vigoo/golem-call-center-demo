import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setDefaultResultOrder } from "node:dns";

setDefaultResultOrder("ipv4first");
const origin = process.env.GOLEM_TEST_ORIGIN ?? "http://call-center-test.localhost:19006";
const callId = process.env.GOLEM_TEST_CALL_ID ?? `smoke-${randomUUID()}`;
const base = `${origin}/calls/${callId}/run/invocations/${randomUUID()}`;
const request = async (path, options = {}) => {
  const response = await fetch(base + path, {
    ...options,
    headers: { "Content-Type": "application/json", ...options.headers },
    signal: AbortSignal.timeout(10000),
  });
  assert.ok(response.ok, `${options.method ?? "GET"} ${path}: ${response.status} ${await response.clone().text()}`);
  return response;
};
let producerSequence = 0;
const append = (events, close = false) => request("/streams/input", {
  method: "POST",
  headers: {
    "Producer-Id": "smoke", "Producer-Epoch": "0",
    "Producer-Seq": String(producerSequence++),
    ...(close ? { "Stream-Closed": "true" } : {}),
  },
  body: JSON.stringify(events),
});
console.log(`Testing ${callId}`);
await request("", { method: "PUT", body: "{}" });
if (process.env.GOLEM_TEST_EXPECT_ENDED) {
  const deadline = performance.now() + 15000;
  let rejected = false;
  while (performance.now() < deadline) {
    const response = await request("/streams/output?offset=-1");
    const events = await response.json();
    if (events.length) {
      assert.equal(events[0].failure?.reason, "Call has ended; start a new call ID");
      rejected = true;
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.ok(rejected, "Restored ended call did not reject a new run");
  console.log("Passed: restored ended state rejects reuse");
  process.exit(0);
}
await append([{ audio: { sequence: 0, capturedAt: 0, payload: "AAA=" } }]);
let offset = "-1";
const events = [];
const pollUntil = async (predicate) => {
  const deadline = performance.now() + 15000;
  while (performance.now() < deadline) {
    const response = await request(`/streams/output?offset=${encodeURIComponent(offset)}`);
    offset = response.headers.get("Stream-Next-Offset");
    assert.ok(offset, "Missing opaque output offset");
    events.push(...await response.json());
    if (events.some((event) => event.failure)) {
      throw new Error(`Provider failure: ${JSON.stringify(events.filter((event) => event.failure))}`);
    }
    if (predicate(response)) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out. Received event types: ${events.map((event) => Object.keys(event)[0]).join(", ")}`);
};
await pollUntil(() => events.some((event) => event.responseFinished));
assert.equal(events.find((event) => event.transcript)?.transcript.text, "Hello fixture");
const audio = events.filter((event) => event.audio).map((event) => event.audio);
assert.ok(audio.length > 0, "No synthesized audio");
assert.ok(Buffer.from(audio[0].payload, "base64").length > 0, "Empty audio payload");
assert.deepEqual(audio.map((chunk) => chunk.sequence), audio.map((_, index) => index));
const finished = events.find((event) => event.responseFinished).responseFinished;
await append([{ playbackAck: { responseId: finished.responseId, playedThrough: audio.at(-1).sequence } }, "hangup"], true);
await pollUntil((response) => response.headers.get("Stream-Closed") === "true");
console.log(`Passed: transcript, ${audio.length} audio chunks, response completion, acknowledgement, hangup, output closure`);
