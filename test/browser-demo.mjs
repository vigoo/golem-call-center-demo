// Run with the integration fixture, Golem server and GOLEM_DEMO_ENV=integration controller.
import assert from "node:assert/strict";
import { chromium } from "@playwright/test";

const origin = "http://127.0.0.1:19008";
const control = (settings) => fetch("http://127.0.0.1:19110/test/control", {
  method: "POST", body: JSON.stringify(settings),
}).then((response) => { assert.ok(response.ok); });
assert.equal((await fetch(`${origin}/__demo`).then((r) => r.json())).environment, "integration");
for (const [headers, body, status] of [
  [{ "Content-Type": "application/json" }, "{}", 403],
  [{ Origin: "https://untrusted.example", "Content-Type": "application/json" }, "{}", 403],
  [{ Origin: origin, "Content-Type": "application/json" }, "{", 400],
  [{ Origin: origin, "Content-Type": "application/json" }, '{"callId":""}', 400],
]) {
  assert.equal((await fetch(`${origin}/__demo/crash`, { method: "POST", headers, body })).status, status);
}
const browser = await chromium.launch({ channel: "chrome" });
try {
  const ordinary = await browser.newPage();
  await ordinary.goto("http://call-center-test.localhost:19006");
  await ordinary.waitForFunction(() => !document.getElementById("start").disabled);
  assert.equal(await ordinary.locator("#demo-controls").isVisible(), false);
  await ordinary.close();
  await control({ singleTurn: true, audioSeconds: .1 });
  const page = await browser.newPage({ viewport: { width: 1120, height: 1100 } });
  const errors = [], requests = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.addInitScript(() => {
    // Exercise real AudioWorklet capture/streaming; do not depend on OS microphone permission.
    navigator.mediaDevices.getUserMedia = async () => {
      const context = new AudioContext();
      const oscillator = context.createOscillator(), destination = context.createMediaStreamDestination();
      oscillator.frequency.value = 440;
      oscillator.connect(destination); oscillator.start(); await context.resume();
      destination.stream.getTracks()[0].addEventListener("ended", () => context.close());
      return destination.stream;
    };
  });
  await page.goto(origin);
  await page.locator("#demo-controls").waitFor();
  assert.ok(await page.locator("#crash").isDisabled());
  await page.screenshot({ path: ".amp/in/artifacts/demo-ready.png", fullPage: true });
  await page.locator("#call-id").fill(`demo.call.${Date.now()}`);
  await page.locator("#start").click();
  await page.waitForFunction(() => document.getElementById("events").textContent.includes("Played ·"));
  const callId = await page.locator("#call-id").inputValue();
  const priorResponses = await page.locator("#transcripts .turn span").allTextContents();
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  await page.route("**/__demo/crash", async (route) => {
    requests.push(route.request().postDataJSON());
    const response = await route.fetch(); // Real backend invokes real simulate-crash.
    await gate;
    await route.fulfill({ response });
  });
  await page.locator("#crash").click();
  await page.waitForFunction(() => document.getElementById("crash-result").textContent.includes("Requesting agent crash"));
  assert.ok(await page.locator("#crash").isDisabled());
  await page.locator("#crash").evaluate((button) => button.click());
  await page.screenshot({ path: ".amp/in/artifacts/demo-pending.png", fullPage: true });
  release();
  await page.waitForFunction(() => document.getElementById("crash-result").textContent.includes("success is not yet confirmed"));
  assert.deepEqual(requests, [{ callId }]);
  await page.waitForFunction((prior) => [...document.querySelectorAll("#transcripts .turn span")].some((label) => {
    if (!label.textContent.startsWith("Assistant · ") || prior.includes(label.textContent)) return false;
    const id = label.textContent.slice("Assistant · ".length);
    const events = document.getElementById("events").textContent;
    return label.parentElement.textContent.endsWith("Hello from the fixture.") &&
      events.includes(`responseFinished · ${id}`) && events.includes(`Played · ${id} ·`);
  }), priorResponses);
  assert.equal(await page.locator("#call-id").inputValue(), callId);
  assert.equal(await page.locator("#error").isVisible(), false);
  await page.screenshot({ path: ".amp/in/artifacts/demo-completed.png", fullPage: true });
  await page.unroute("**/__demo/crash");
  await page.route("**/__demo/crash", (route) => route.fulfill({ status: 502,
    contentType: "application/json", body: JSON.stringify({ error: "Test controller unavailable" }),
  }));
  await page.locator("#crash").click();
  await page.waitForFunction(() => document.getElementById("crash-result").textContent.includes("Could not confirm crash"));
  assert.equal(await page.locator("#error").isVisible(), false);
  await page.screenshot({ path: ".amp/in/artifacts/demo-error.png", fullPage: true });
  await page.locator("#hangup").click();
  await page.waitForFunction(() => document.getElementById("status").textContent === "Ended");
  assert.ok(await page.locator("#crash").isDisabled());
  await page.unroute("**/__demo/crash");
  for (const newCall of [false, true]) {
    await page.locator("#call-id").fill(`feedback.${Date.now()}`);
    await page.locator("#start").click();
    await page.waitForFunction(() => !document.getElementById("crash").disabled);
    let finish;
    const pending = new Promise((resolve) => { finish = resolve; });
    await page.route("**/__demo/crash", async (route) => {
      await pending;
      await route.fulfill({ status: 200, contentType: "application/json", body: "{}" });
    });
    await page.locator("#crash").click();
    await page.waitForFunction(() => document.getElementById("crash-result").textContent.includes("Requesting agent crash"));
    await page.locator("#hangup").click();
    await page.waitForFunction(() => document.getElementById("status").textContent === "Ended");
    if (newCall) {
      await page.locator("#call-id").fill(`replacement.${Date.now()}`);
      await page.locator("#start").click();
      await page.waitForFunction(() => !document.getElementById("hangup").disabled);
    }
    finish();
    if (newCall) {
      await page.waitForFunction(() => !document.getElementById("crash").disabled);
      assert.equal(await page.locator("#crash-result").textContent(), "Crash control targets this call only. Recovery is experimental.");
      await page.locator("#hangup").click();
      await page.waitForFunction(() => document.getElementById("status").textContent === "Ended");
    } else {
      await page.waitForFunction(() => document.getElementById("crash-result").textContent.includes("Crash command completed for feedback."));
      assert.equal(await page.locator("#status").textContent(), "Ended");
    }
    await page.unroute("**/__demo/crash");
  }
  const mobile = await browser.newPage({ viewport: { width: 390, height: 1400 } });
  await mobile.goto(origin); await mobile.locator("#demo-controls").waitFor();
  assert.ok(await mobile.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
  await mobile.screenshot({ path: ".amp/in/artifacts/demo-mobile.png", fullPage: true });
  assert.deepEqual(errors, []);
  console.log("Passed: controller request validation, demo-only controls, real button/CLI crash, same-call post-crash answer, single request, pending/error states, hangup and mobile layout");
} finally { await control({}); await browser.close(); }
