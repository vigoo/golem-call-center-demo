// Continuous linear interpolation: retain fractional phase and a boundary sample across blocks.
export class PcmEncoder {
  constructor(sampleRate, frameSamples = 1280) {
    this.ratio = sampleRate / 16000;
    this.frame = new Int16Array(frameSamples);
    this.used = 0;
    this.position = 0;
    this.previous = new Float32Array(0);
  }
  push(samples, emit) {
    const data = new Float32Array(this.previous.length + samples.length);
    data.set(this.previous); data.set(samples, this.previous.length);
    while (this.position + 1 < data.length) {
      const i = Math.floor(this.position), fraction = this.position - i;
      const value = Math.max(-1, Math.min(1, data[i] * (1 - fraction) + data[i + 1] * fraction));
      this.frame[this.used++] = Math.round(value * (value < 0 ? 32768 : 32767));
      this.position += this.ratio;
      if (this.used === this.frame.length) {
        const bytes = new Uint8Array(this.frame.length * 2);
        const view = new DataView(bytes.buffer);
        this.frame.forEach((sample, index) => view.setInt16(index * 2, sample, true));
        emit(bytes); this.used = 0;
      }
    }
    const keep = Math.min(Math.floor(this.position), data.length - 1);
    this.previous = data.slice(keep);
    this.position -= keep;
  }
}

export const toBase64 = (bytes) => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};
