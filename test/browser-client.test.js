import { describe, it, expect } from "vitest";
import { PcmEncoder } from "../web/assets/pcm.js";
import { CallTransport } from "../web/assets/transport.js";

describe("browser PCM", () => {
  it("preserves fractional phase across asymmetric block boundaries and encodes little endian", () => {
    const frames = [];
    const encoder = new PcmEncoder(48000, 4);
    const samples = Float32Array.from([-1, .1, .2, -.5, .3, .4, .5, .2, 0, 1, 0, 0, 0]);
    encoder.push(samples.slice(0, 2), (bytes) => frames.push(bytes));
    encoder.push(samples.slice(2, 7), (bytes) => frames.push(bytes));
    encoder.push(samples.slice(7), (bytes) => frames.push(bytes));
    expect([...frames[0]]).toEqual([0, 128, 0, 192, 0, 64, 255, 127]);
  });
  it("44.1 kHz chunked interpolation matches whole-stream interpolation", () => {
    const samples = Float32Array.from({ length: 10001 }, (_, i) => Math.sin(i / 31));
    const encode = (sizes) => {
      const encoder = new PcmEncoder(44100, 80), frames = [];
      let offset = 0;
      for (const size of sizes) { encoder.push(samples.slice(offset, offset + size), (b) => frames.push(...b)); offset += size; }
      return frames;
    };
    expect(encode([13, 127, 1, 901, 8959])).toEqual(encode([10001]));
    expect(encode([10001]).length).toBe(7200);
  });
});

describe("browser Durable Streams", () => {
  it("retries a disconnected output body at the same cursor but not malformed JSON", async () => {
    const urls = [], seen = [];
    const transport = new CallTransport("id", async (url) => {
      urls.push(url);
      if (urls.length === 1) return new Response(new ReadableStream({ start(c) { c.error(new TypeError("lost body")); } }));
      return new Response('[{"transcript":{"text":"once"}}]', { headers: { "Stream-Next-Offset": "next", "Stream-Closed": "true" } });
    });
    await transport.read((e) => seen.push(e.transcript.text));
    expect(urls[0]).toBe(urls[1]); expect(seen).toEqual(["once"]);
    let requests = 0;
    const malformed = new CallTransport("id", async () => { requests++; return new Response("bad JSON", { headers: { "Stream-Next-Offset": "next" } }); });
    await expect(malformed.read(() => {})).rejects.toBeInstanceOf(SyntaxError);
    expect(requests).toBe(1); transport.dispose(); malformed.dispose();
  });
  it("retries uncertain append with identical body and identity, then advances sequence", async () => {
    const requests = [];
    const transport = new CallTransport("a/b", async (url, options) => {
      requests.push({ url, ...options });
      if (requests.length === 1) throw new TypeError("lost reply");
      return new Response("[]");
    });
    await transport.append([{ audio: { sequence: 0 } }]);
    await transport.append(["hangup"], true);
    expect(requests[0].url).toContain("/a%2Fb/");
    expect(requests[0].body).toBe(requests[1].body);
    expect(requests[0].headers).toEqual(requests[1].headers);
    expect(requests.map((r) => r.headers["Producer-Seq"])).toEqual(["0", "0", "1"]);
    expect(requests[2].headers["Stream-Closed"]).toBe("true");
    await expect(transport.append(["interrupt"])).rejects.toThrow("closed");
    transport.dispose();
  });
  it("bounds queued frames and does not continue a producer after permanent rejection", async () => {
    let reject;
    let calls = 0;
    const transport = new CallTransport("id", () => { calls++; return new Promise((_, r) => { reject = r; }); });
    const pending = Array.from({ length: 64 }, () => transport.append([]).catch((e) => e));
    await expect(transport.append([])).rejects.toThrow("64 frames");
    await Promise.resolve();
    transport.dispose(); reject(new Error("aborted"));
    await Promise.all(pending);
    expect(calls).toBe(1);
  });
  it("follows opaque output cursors without replay and stops at closure", async () => {
    const urls = [], seen = [];
    const transport = new CallTransport("id", async (url) => {
      urls.push(url);
      return new Response(JSON.stringify([{ transcript: { text: String(urls.length) } }]), {
        headers: { "Stream-Next-Offset": "opaque+next/=", "Stream-Closed": String(urls.length === 2) },
      });
    });
    await transport.read((event) => seen.push(event.transcript.text));
    expect(urls[1]).toContain("offset=opaque%2Bnext%2F%3D");
    expect(seen).toEqual(["1", "2"]);
    transport.dispose();
  });
});
