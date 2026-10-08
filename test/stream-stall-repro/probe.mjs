import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setDefaultResultOrder } from "node:dns";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
setDefaultResultOrder("ipv4first");
const crash = process.env.GOLEM_PROBE_CRASH === "1";
for (let run = 0; run < (crash ? 1 : 10); run++) {
  const name = randomUUID();
  const base = `http://stream-stall-repro.localhost:19006/probe/${name}/run/invocations/test`;
  const request = async (path, init) => {
    const response = await fetch(base + path, { ...init, signal: AbortSignal.timeout(10000),
      headers: { "Content-Type": "application/json", ...init?.headers } });
    assert.ok(response.ok, `${response.status}: ${await response.clone().text()}`);
    return response;
  };
  console.log(`Probe ${run + 1}: StreamProbe("${name}")`);
  await request("", { method: "PUT", body: "{}" });
  await request("/streams/input", { method: "POST", body: '["hello"]',
    headers: { "Producer-Id": "probe", "Producer-Epoch": "0", "Producer-Seq": "0", ...(!crash ? { "Stream-Closed": "true" } : {}) } });
  let deadline = performance.now() + 10000;
  let passed = false;
  let restarted = false;
  let offset = "-1";
  const received = [];
  while (performance.now() < deadline) {
    const response = await request(`/streams/output?offset=${encodeURIComponent(offset)}`);
    offset = response.headers.get("Stream-Next-Offset");
    assert.ok(offset, "Missing output cursor");
    received.push(...await response.json());
    if (crash && !restarted && received.length === 1) {
      await promisify(execFile)("golem", ["agent", "simulate-crash", `StreamProbe(${JSON.stringify(name)})`, "-A", "test/stream-stall-repro/golem.yaml", "-Y"], {
        env: { ...process.env, GOLEM_EFFECT_GOLEM_PATH: `${process.env.HOME}/projects/golem/golem/sdks/effect` }, timeout: 30000,
      });
      await request("/streams/input", { method: "POST", body: '["after"]',
        headers: { "Producer-Id": "probe", "Producer-Epoch": "0", "Producer-Seq": "1", "Stream-Closed": "true" } });
      restarted = true;
      deadline = performance.now() + 10000;
    }
    if (response.headers.get("Stream-Closed") === "true") {
      assert.deepEqual(received, crash ? ["hello", "after"] : ["hello"]);
      passed = true;
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.ok(passed, `StreamProbe("${name}") stalled before echo/output closure`);
}
