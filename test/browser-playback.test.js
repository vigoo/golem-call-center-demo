import { describe, it, expect } from "vitest";
import { PcmPlayback } from "../web/assets/playback.js";

const fakeContext = () => ({
  currentTime: 1, destination: {}, nodes: [],
  createBuffer(_, size, rate) { return { rate, samples: new Float32Array(size), getChannelData() { return this.samples; } }; },
  createBufferSource() {
    const node = { connect() {}, disconnect() {}, start(at) { this.at = at; }, stop() { this.stopped = true; }, end() { this.onended?.(); } };
    this.nodes.push(node); return node;
  },
});
const audio = (id, sequence, bytes = [0, 128, 255, 127]) => ({ responseId: `response-${id}`, sequence, payload: btoa(String.fromCharCode(...bytes)) });

describe("browser playback", () => {
  it("schedules gap-free PCM but acknowledges only contiguous completed audio, never scheduled audio", async () => {
    const context = fakeContext(), heard = [];
    const playback = new PcmPlayback(context, (...ack) => heard.push(ack));
    await playback.push(audio(4, 0)); await playback.push(audio(4, 1));
    expect(heard).toEqual([]);
    expect([...context.nodes[0].buffer.samples]).toEqual([-1, 32767 / 32768]);
    expect(context.nodes[1].at).toBe(context.nodes[0].at + 2 / 16000);
    context.nodes[1].end(); expect(heard).toEqual([]);
    context.nodes[0].end(); expect(heard).toEqual([["response-4", 1]]);
  });
  it("stops all scheduled sources without acknowledgements and gates in-flight audio until confirmation", async () => {
    const context = fakeContext(), heard = [];
    const playback = new PcmPlayback(context, (...ack) => heard.push(ack));
    await playback.push(audio(3, 0)); playback.interrupt();
    expect(context.nodes[0].stopped).toBe(true);
    context.nodes[0].end(); expect(heard).toEqual([]);
    expect(await playback.push(audio(3, 1))).toBe(false);
    expect(await playback.push(audio(5, 0))).toBe(false);
    playback.confirmInterrupt();
    expect(await playback.push(audio(5, 1))).toBe(false);
    expect(await playback.push(audio(6, 0))).toBe(true);
    playback.clear("response-3"); expect(context.nodes[1].stopped).not.toBe(true);
    expect(await playback.push(audio(3, 2))).toBe(false);
  });
  it("rejects sequence gaps, odd PCM and huge frames, drops duplicates and waits at the source bound", async () => {
    const playback = new PcmPlayback(fakeContext(), () => {});
    await expect(playback.push(audio(1, 1))).rejects.toThrow("gap");
    await expect(playback.push(audio(1, 0, [0]))).rejects.toThrow("length");
    await expect(playback.push({ ...audio(1, 0), payload: "A".repeat(12 * 1024 * 1024) })).rejects.toThrow("8 MiB");
    await playback.push(audio(1, 0)); expect(await playback.push(audio(1, 0))).toBe(false);
    for (let i = 1; i < 64; i++) await playback.push(audio(1, i));
    const pending = playback.push(audio(1, 64));
    expect(playback.sources.size).toBe(64); expect(playback.waiters.size).toBe(1);
    playback.context.nodes[0].end();
    expect(await pending).toBe(true); expect(playback.sources.size).toBe(64);
    playback.stop();
  });
  it("paces a 12.25-second single chunk within four seconds and ACKs only the whole played chunk", async () => {
    const context = fakeContext(), heard = [];
    const playback = new PcmPlayback(context, (...ack) => heard.push(ack));
    let scheduled = false;
    const pending = playback.push({ ...audio(2, 0), payload: btoa("\0".repeat(392000)) }).then(() => { scheduled = true; });
    let ended = 0;
    while (!scheduled) {
      expect(playback.sources.size).toBeLessThanOrEqual(4);
      expect(playback.nextTime - context.currentTime).toBeLessThanOrEqual(4);
      expect(heard).toEqual([]);
      const source = context.nodes[ended++];
      context.currentTime = source.at + source.buffer.samples.length / 16000;
      source.end(); await Promise.resolve(); await Promise.resolve();
    }
    await pending;
    expect(heard).toEqual([]);
    for (; ended < context.nodes.length; ended++) context.nodes[ended].end();
    expect(context.nodes).toHaveLength(13);
    expect(context.nodes.at(-1).buffer.samples).toHaveLength(4000);
    expect(heard).toEqual([["response-2", 0]]);
  });
  it("fast clear cancels a blocked chunk and confirmation cannot revive it", async () => {
    const context = fakeContext(), heard = [];
    const playback = new PcmPlayback(context, (...ack) => heard.push(ack));
    const pending = playback.push({ ...audio(3, 0), payload: btoa("\0".repeat(320000)) });
    const scheduled = context.nodes.length;
    playback.clear("response-3"); playback.confirmInterrupt();
    expect(await pending).toBe(false);
    expect(context.nodes).toHaveLength(scheduled);
    for (const source of context.nodes) { expect(source.stopped).toBe(true); source.end(); }
    expect(heard).toEqual([]); expect(playback.waiters.size).toBe(0);
    expect(await playback.push(audio(4, 0))).toBe(true);
    playback.stop(); playback.confirmInterrupt();
    expect(await playback.push(audio(5, 0))).toBe(false);
  });
  it("local interrupt and terminal stop wake pending scheduling immediately", async () => {
    for (const action of ["interrupt", "stop"]) {
      const playback = new PcmPlayback(fakeContext(), () => {});
      const pending = playback.push({ ...audio(7, 0), payload: btoa("\0".repeat(320000)) });
      expect(playback.waiters.size).toBe(1);
      playback[action]();
      expect(await pending).toBe(false);
      expect(playback.sources.size).toBe(0); expect(playback.waiters.size).toBe(0);
    }
  });
});
