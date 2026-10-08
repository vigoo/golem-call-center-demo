import assert from "node:assert/strict";
import { chromium } from "@playwright/test";

const origin = process.env.GOLEM_TEST_ORIGIN ?? "http://call-center-test.localhost:19006";
const browser = await chromium.launch({ channel: process.env.PLAYWRIGHT_CHANNEL ?? "chrome",
  args: ["--use-fake-ui-for-media-stream", "--use-fake-device-for-media-stream"] });
try {
  const page = await browser.newPage({ viewport: { width: 1120, height: 900 } });
  const errors = []; page.on("pageerror", (error) => errors.push(error.message));
  let retried = false, producer = null;
  await page.route("**/streams/input", async (route) => {
    const request = route.request();
    const headers = request.headers();
    if (headers["stream-closed"] === "true") {
      const response = await route.fetch(); await new Promise((resolve) => setTimeout(resolve, 1000));
      await route.fulfill({ response }); return;
    }
    if (request.method() === "POST" && headers["producer-seq"] === "0") {
      if (!producer) { producer = request.postData(); await route.fetch(); await route.abort("failed"); return; }
      assert.equal(request.postData(), producer); retried = true;
    }
    await route.continue();
  });
  await page.goto(origin);
  await page.getByRole("button", { name: "Start call" }).click();
  await page.waitForFunction(() => document.getElementById("status").textContent === "Listening", { timeout: 20000 });
  await page.waitForFunction(() => document.getElementById("transcripts").textContent.includes("Hello fixture"), { timeout: 20000 });
  for (let attempt = 0; !retried && attempt < 50; attempt++) await page.waitForTimeout(100);
  assert.ok(retried, "uncertain append was not retried");
  await page.waitForFunction(() => document.getElementById("events").textContent.includes("Played ·"));
  await page.waitForFunction(() => document.getElementById("transcripts").textContent.includes("Assistant ·"));
  await page.screenshot({ path: ".amp/in/artifacts/console-active.png", fullPage: true });
  await page.getByRole("button", { name: "Interrupt", exact: true }).click();
  await page.waitForFunction(() => document.getElementById("events").textContent.includes("Agent confirmed interrupt"));
  assert.match(await page.locator("#silence").textContent(), /ms$/);
  await page.screenshot({ path: ".amp/in/artifacts/console-interrupted.png", fullPage: true });
  await page.getByRole("button", { name: "Hang up" }).click();
  try {
    await page.waitForFunction(() => document.getElementById("status").textContent === "Ended", null, { timeout: 20000 });
  } catch (error) { console.error(await page.locator("body").innerText()); throw error; }
  await page.waitForTimeout(1500);
  assert.equal(await page.locator("#status").textContent(), "Ended", "late hangup reply overwrote terminal state");
  assert.deepEqual(errors, []);
  await page.addInitScript(() => {
    navigator.mediaDevices.getUserMedia = async () => { throw new DOMException("Permission denied", "NotAllowedError"); };
  });
  await page.reload(); await page.getByRole("button", { name: "Start call" }).click();
  await page.getByRole("alert").waitFor();
  assert.match(await page.getByRole("alert").textContent(), /Permission denied/);
  assert.equal(await page.locator("#status").textContent(), "Failed");
  await page.screenshot({ path: ".amp/in/artifacts/console-failed.png", fullPage: true });
  const late = await browser.newPage(); let created = 0;
  late.on("request", (request) => { if (request.method() === "PUT") created++; });
  await late.addInitScript(() => {
    navigator.mediaDevices.getUserMedia = () => new Promise((resolve) => {
      window.finishPermission = () => resolve({ getTracks: () => [{ stop: () => { window.trackStopped = true; } }] });
    });
  });
  await late.goto(origin); await late.locator("#start").click();
  await late.evaluate(() => { window.dispatchEvent(new Event("pagehide")); window.finishPermission(); });
  await late.waitForFunction(() => window.trackStopped);
  assert.equal(created, 0, "late permission created a call after disposal");
  console.log("Passed: real AudioWorklet capture, uncertain append retry, transcript, interrupt, hangup, denied permission");
} finally { await browser.close(); }
