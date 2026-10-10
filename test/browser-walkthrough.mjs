import assert from "node:assert/strict";
import { chromium } from "@playwright/test";

const origin = process.env.GOLEM_TEST_ORIGIN ?? "http://call-center-test.localhost:19006";
const browser = await chromium.launch({ channel: "chrome" });
try {
  const page = await browser.newPage({ viewport: { width: 1280, height: 1000 } });
  const errors = [], requests = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("request", (request) => requests.push(request.url()));
  const response = await page.goto(`${origin}/walkthrough`);
  assert.equal(response.status(), 200);
  assert.match(response.headers()["content-type"], /text\/html/);
  assert.match(response.headers()["content-security-policy"], /script-src 'self'/);
  await page.getByRole("heading", { name: "Inside the pipeline" }).waitFor();
  await page.waitForFunction(() => document.getElementById("step-title").textContent === "Golem hosts the console");
  assert.equal(await page.locator("#previous").isDisabled(), true);
  await page.screenshot({ path: ".amp/in/artifacts/walkthrough-default.png", fullPage: true });
  for (const [id, title, length] of [
    ["turn", "A spoken turn", 9], ["buffering", "Long reply", 3],
    ["interruption", "Barge-in", 4], ["delivery", "Delivery & retry", 3],
    ["durability", "State & recovery", 4],
    ["runtime", "Golem replay internals", 5], ["protocols", "Endpoints & transports", 4],
    ["recoveryPolicy", "Proposed session recovery", 4],
  ]) {
    await page.getByRole("button", { name: title, exact: true }).click();
    await page.waitForFunction(([id, title]) => location.hash === `#${id}/0` && document.getElementById("scenario-title").textContent === title, [id, title]);
    assert.equal(await page.locator("#steps button").count(), length);
    for (let index = 0; index < length; index++) {
      await page.waitForFunction(([index, length]) => document.getElementById("step-count").textContent === `STEP ${index + 1} / ${length}`, [index, length]);
      assert.equal(await page.locator("#steps [aria-current=step]").count(), 1);
      assert.ok((await page.locator("#payload").textContent()).length > 30);
      assert.ok((await page.locator("#route").textContent()).includes("→"));
      assert.ok(await page.locator('.boundary[data-active="true"]').count() >= 2);
      assert.equal(await page.locator("#next").isDisabled(), index === length - 1);
      if (id === "turn" && index === 3) {
        assert.match(await page.locator("#invariant").textContent(), /TurnResumed.*no coordinator action/);
        await page.screenshot({ path: ".amp/in/artifacts/walkthrough-flux.png", fullPage: true });
      }
      if (id === "interruption" && index === 1) {
        assert.match(await page.locator("#description").textContent(), /TurnResumed.*no coordinator action/);
        await page.screenshot({ path: ".amp/in/artifacts/walkthrough-barge-in.png", fullPage: true });
      }
      if (id === "recoveryPolicy") assert.match(await page.locator("#state").textContent(), /PROPOSAL/);
      if (id === "runtime" && index === 2) {
        assert.match(await page.locator("#invariant").textContent(), /not reconstruct provider state/);
        assert.match(await page.locator("#payload").textContent(), /no resend/);
      }
      if (id === "protocols" && index === 1) {
        assert.match(await page.locator("#description").textContent(), /does not use SSE or a WebSocket to Golem/);
      }
      if ((id === "runtime" && index === 3) || (id === "protocols" && index === 2) || (id === "recoveryPolicy" && index === 0)) {
        await page.screenshot({ path: `.amp/in/artifacts/walkthrough-${id}-detail.png`, fullPage: true });
      }
      if (index < length - 1) await page.locator("#next").click();
    }
    await page.screenshot({ path: `.amp/in/artifacts/walkthrough-${id}.png`, fullPage: true });
    await page.locator("#previous").click();
    await page.waitForFunction(([id, length]) => location.hash === `#${id}/${length - 2}`, [id, length]);
  }
  await page.goto(`${origin}/walkthrough#turn/7`);
  await page.waitForFunction(() => document.getElementById("step-title").textContent === "Play, then acknowledge");
  assert.match(await page.locator("#payload").textContent(), /"playbackAck".*"playedThrough":0/);
  await page.getByRole("button", { name: /State & recovery Snapshots/ }).click();
  assert.equal(await page.locator("#owner-title").textContent(), "State & recovery");
  assert.match(await page.locator("#owner-detail").textContent(), /GOL-799/);
  await page.locator("#steps button").nth(2).click();
  await page.waitForFunction(() => document.getElementById("step-title").textContent === "Capture and append PCM");
  assert.match(await page.locator("#payload").textContent(), /Producer-Seq: 0/);
  assert.match(await page.locator("#payload").textContent(), /"audio":.*"capturedAt":0/);
  for (const hash of ["#unknown/1", "#__proto__/0", "#turn/-1", "#turn/99", "#turn/1/extra"]) {
    await page.goto(`${origin}/walkthrough${hash}`);
    await page.waitForFunction(() => document.getElementById("step-title").textContent === "Golem hosts the console");
    assert.equal(new URL(page.url()).hash, "#turn/0");
  }
  // Keyboard navigation and browser history use ordinary controls and hash links.
  await page.locator("#next").focus(); await page.keyboard.press("Enter");
  await page.waitForFunction(() => document.getElementById("step-title").textContent === "Create the stream session");
  await page.goBack();
  await page.waitForFunction(() => document.getElementById("step-title").textContent === "Golem hosts the console");
  assert.ok(requests.every((url) => new URL(url).origin === origin && !url.includes("/calls/")), "Walkthrough made a call/provider request");
  const mobile = await browser.newPage({ viewport: { width: 390, height: 1100 } });
  mobile.on("pageerror", (error) => errors.push(error.message));
  await mobile.goto(`${origin}/walkthrough#buffering/2`);
  await mobile.waitForFunction(() => document.getElementById("step-title").textContent === "Cancellation takes a fast lane");
  assert.ok(await mobile.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
  await mobile.screenshot({ path: ".amp/in/artifacts/walkthrough-mobile.png", fullPage: true });
  for (const [id, step] of [["runtime", 3], ["protocols", 2], ["recoveryPolicy", 0]]) {
    await mobile.goto(`${origin}/walkthrough#${id}/${step}`);
    await mobile.waitForFunction(([id, step]) => document.querySelector("#scenarios [aria-pressed=true]") && document.getElementById("step-count").textContent.startsWith(`STEP ${step + 1} /`) && location.hash === `#${id}/${step}`, [id, step]);
    assert.ok(await mobile.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    await mobile.screenshot({ path: `.amp/in/artifacts/walkthrough-${id}-mobile.png`, fullPage: true });
  }
  await page.goto(origin);
  await page.getByRole("link", { name: /interactive pipeline walkthrough/ }).waitFor();
  await page.screenshot({ path: ".amp/in/artifacts/console-walkthrough-link.png", fullPage: true });
  assert.deepEqual(errors, []);
  console.log("Passed: all 36 walkthrough steps, proposal labels, boundary inspector, protocol examples, hash validation, deep links, keyboard/history, mobile layout, CSP and no call requests");
} finally { await browser.close(); }
