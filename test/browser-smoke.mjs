import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import { chromium } from "@playwright/test";

const origin = process.env.GOLEM_TEST_ORIGIN ?? "http://call-center-test.localhost:19006";
await mkdir(".amp/in/artifacts", { recursive: true });
const browser = await chromium.launch({ channel: process.env.PLAYWRIGHT_CHANNEL ?? "chrome" });
try {
  const page = await browser.newPage({ viewport: { width: 1120, height: 900 } });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const response = await page.goto(origin);
  assert.equal(response.status(), 200);
  assert.match(response.headers()["content-type"], /text\/html/);
  await page.getByRole("heading", { name: "Call console" }).waitFor();
  assert.ok(await page.locator("#call-id").inputValue());
  assert.equal(await page.locator("#status").textContent(), "Ready");
  for (const [asset, mime] of [["console.css", /text\/css/], ["console.js", /javascript/]]) {
    const fetched = await page.request.get(`${origin}/assets/${asset}`);
    assert.equal(fetched.status(), 200);
    assert.match(fetched.headers()["content-type"], mime);
  }
  assert.equal((await page.request.get(`${origin}/assets/missing.js`)).status(), 404);
  await page.screenshot({ path: ".amp/in/artifacts/console-default.png", fullPage: true });
  const mobile = await browser.newPage({ viewport: { width: 390, height: 1400 } });
  await mobile.goto(origin);
  assert.ok(await mobile.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
  await mobile.screenshot({ path: ".amp/in/artifacts/console-mobile.png", fullPage: true });
  assert.deepEqual(errors, []);
  console.log("Passed: Golem-hosted console, MIME types, missing asset, responsive layout, no browser errors");
} finally {
  await browser.close();
}
