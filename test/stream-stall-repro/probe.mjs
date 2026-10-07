import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setDefaultResultOrder } from "node:dns";
setDefaultResultOrder("ipv4first");
for (let run = 0; run < 10; run++) {
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
    headers: { "Producer-Id": "probe", "Producer-Epoch": "0", "Producer-Seq": "0", "Stream-Closed": "true" } });
  const deadline = performance.now() + 10000;
  let passed = false;
  while (performance.now() < deadline) {
    const response = await request("/streams/output?offset=-1");
    const output = await response.json();
    if (output.length) {
      assert.deepEqual(output, ["hello"]);
      passed = true;
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.ok(passed, `StreamProbe("${name}") stalled without echo output`);
}
