// Serial fixture-backed checks: real Web Audio scheduling and Golem Durable Streams.
import assert from "node:assert/strict";
import { chromium } from "@playwright/test";

const origin = process.env.GOLEM_TEST_ORIGIN ?? "http://call-center-test.localhost:19006";
const control = (settings) => fetch("http://127.0.0.1:19110/test/control", {
  method: "POST", body: JSON.stringify(settings),
}).then((response) => { assert.ok(response.ok); });
const browser = await chromium.launch({ channel: "chrome", args: [
  "--use-fake-ui-for-media-stream", "--use-fake-device-for-media-stream",
] });
try {
  for (const scenario of ["long", "burst", "interrupt", "barge-in", "fatal", "hangup"]) {
    await control({ singleTurn: true, audioSeconds: scenario === "burst" ? .1 : 12.25,
      audioChunks: scenario === "burst" ? 100 : 1,
    });
    const page = await browser.newPage();
    const errors = [], acks = [];
    page.on("pageerror", (error) => errors.push(error.message));
    await page.addInitScript(() => {
      window.audioStats = { maxSources: 0, maxAhead: 0, ended: 0, stopped: 0 };
      const original = AudioContext.prototype.createBufferSource;
      AudioContext.prototype.createBufferSource = function () {
        const context = this, source = original.call(this);
        const live = context.testSources ??= new Map();
        const start = source.start.bind(source), stop = source.stop.bind(source);
        source.start = (at) => {
          live.set(source, at + source.buffer.duration);
          window.audioStats.maxSources = Math.max(window.audioStats.maxSources, live.size);
          window.audioStats.maxAhead = Math.max(window.audioStats.maxAhead, ...[...live.values()].map((end) => end - context.currentTime));
          start(at);
        };
        source.stop = () => { live.delete(source); window.audioStats.stopped++; stop(); };
        source.addEventListener("ended", () => { live.delete(source); window.audioStats.ended++; });
        return source;
      };
    });
    await page.route("**/streams/input", async (route) => {
      const body = JSON.parse(route.request().postData() ?? "[]");
      const response = await route.fetch();
      if (response.ok()) for (const event of body) if (event.playbackAck) acks.push(event.playbackAck);
      await route.fulfill({ response });
    });
    await page.goto(origin); await page.locator("#start").click();
    await page.waitForFunction(() => document.getElementById("status").textContent === "Speaking");
    const started = Date.now();
    if (scenario === "long" || scenario === "burst") {
      const last = scenario === "burst" ? 99 : 0;
      await page.waitForFunction((last) => document.getElementById("events").textContent.includes(`Played · response-1 · chunk ${last}`), last, { timeout: 22000 });
      assert.ok(Date.now() - started > (scenario === "long" ? 11000 : 8500), "Audio was acknowledged before it could have played");
      for (let i = 0; !acks.some((ack) => ack.playedThrough === last) && i < 30; i++) await page.waitForTimeout(100);
      assert.ok(acks.some((ack) => ack.playedThrough === last), "Golem did not accept the final acknowledgement");
      assert.equal(await page.locator("#error").isVisible(), false);
    } else if (scenario === "interrupt") {
      await page.locator("#interrupt").click();
      await page.waitForFunction(() => document.getElementById("events").textContent.includes("Agent confirmed interrupt"), null, { timeout: 3000 });
      assert.deepEqual(acks, []);
    } else if (scenario === "barge-in") {
      await control({ event: "barge-in" });
      await page.waitForFunction(() => document.getElementById("events").textContent.includes("clearPlayback · response-1"), null, { timeout: 4000 });
      assert.ok(Date.now() - started < 4000, "Server barge-in waited behind a long audio frame");
      assert.deepEqual(acks, []);
    } else if (scenario === "fatal") {
      await control({ event: "fatal" });
      await page.locator("#error").waitFor({ timeout: 4000 });
      assert.equal(await page.locator("#status").textContent(), "Failed");
      assert.deepEqual(acks, []);
    }
    const stats = await page.evaluate(() => window.audioStats);
    assert.ok(stats.maxAhead <= 4.001, JSON.stringify(stats));
    assert.ok(stats.maxSources <= 64, JSON.stringify(stats));
    if (scenario !== "fatal") {
      await page.locator("#hangup").click();
      await page.waitForFunction(() => document.getElementById("status").textContent === "Ended", null, { timeout: 15000 });
    }
    if (scenario === "hangup") assert.deepEqual(acks, []);
    assert.deepEqual(errors, []);
    console.log(`Passed: ${scenario} (${JSON.stringify(stats)})`);
    await page.close();
  }
} finally { await control({}); await browser.close(); }
