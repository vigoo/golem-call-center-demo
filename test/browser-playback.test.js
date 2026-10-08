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
  it("schedules gap-free PCM but acknowledges only contiguous completed audio, never scheduled audio", () => {
    const context = fakeContext(), heard = [];
    const playback = new PcmPlayback(context, (...ack) => heard.push(ack));
    playback.push(audio(4, 0)); playback.push(audio(4, 1));
    expect(heard).toEqual([]);
    expect([...context.nodes[0].buffer.samples]).toEqual([-1, 32767 / 32768]);
    expect(context.nodes[1].at).toBe(context.nodes[0].at + 2 / 16000);
    context.nodes[1].end(); expect(heard).toEqual([]);
    context.nodes[0].end(); expect(heard).toEqual([["response-4", 1]]);
  });
  it("stops all scheduled sources without acknowledgements and gates in-flight audio until confirmation", () => {
    const context = fakeContext(), heard = [];
    const playback = new PcmPlayback(context, (...ack) => heard.push(ack));
    playback.push(audio(3, 0)); playback.interrupt();
    expect(context.nodes[0].stopped).toBe(true);
    context.nodes[0].end(); expect(heard).toEqual([]);
    expect(playback.push(audio(3, 1))).toBe(false);
    expect(playback.push(audio(5, 0))).toBe(false);
    playback.confirmInterrupt();
    expect(playback.push(audio(5, 1))).toBe(false);
    expect(playback.push(audio(6, 0))).toBe(true);
    playback.clear("response-3"); expect(context.nodes[1].stopped).not.toBe(true);
    expect(playback.push(audio(3, 2))).toBe(false);
  });
  it("rejects sequence gaps and odd PCM, drops duplicates and enforces count and time bounds", () => {
    const playback = new PcmPlayback(fakeContext(), () => {});
    expect(() => playback.push(audio(1, 1))).toThrow("gap");
    expect(() => playback.push(audio(1, 0, [0]))).toThrow("length");
    playback.push(audio(1, 0)); expect(playback.push(audio(1, 0))).toBe(false);
    for (let i = 1; i < 64; i++) playback.push(audio(1, i));
    expect(() => playback.push(audio(1, 64))).toThrow("backlog");
    const timed = new PcmPlayback(fakeContext(), () => {});
    expect(() => timed.push({ responseId: "response-1", sequence: 0, payload: btoa("\0".repeat(16000 * 2 * 8)) })).toThrow("backlog");
  });
});
