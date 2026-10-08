import { PcmEncoder } from "./pcm.js";
class Capture extends AudioWorkletProcessor {
  constructor() { super(); this.encoder = new PcmEncoder(sampleRate); }
  process(inputs) {
    const channels = inputs[0];
    if (channels?.length) {
      const mono = new Float32Array(channels[0].length);
      for (const channel of channels) channel.forEach((sample, i) => { mono[i] += sample / channels.length; });
      this.encoder.push(mono, (bytes) => this.port.postMessage(bytes, [bytes.buffer]));
    }
    return true;
  }
}
registerProcessor("pcm-capture", Capture);
