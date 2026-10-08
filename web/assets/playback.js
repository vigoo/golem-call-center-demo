// One paced push at a time. The Durable Stream holds the backlog, not Web Audio.
export class PcmPlayback {
  constructor(context, acknowledge) {
    this.context = context; this.acknowledge = acknowledge;
    this.sources = new Map(); this.responses = new Map();
    this.invalidThrough = 0; this.latest = 0; this.nextTime = 0; this.muted = false;
    this.stopped = false; this.waiters = new Set();
  }
  async push({ responseId, sequence, payload }, onScheduled = () => {}) {
    const number = Number(responseId.slice("response-".length));
    if (!Number.isSafeInteger(number) || number < 1 || responseId !== `response-${number}`) throw new Error("Invalid audio response ID");
    if (this.stopped) return false;
    if (this.muted) { this.invalidThrough = Math.max(this.invalidThrough, number); return false; }
    if (number <= this.invalidThrough || number < this.latest) return false;
    const progress = this.responses.get(responseId) ?? { next: 0, played: -1, completed: new Set() };
    if (sequence < progress.next) return false;
    if (sequence !== progress.next) throw new Error("Audio sequence gap");
    // Bound a single provider frame before decoding; scheduled buffers are much smaller.
    if (payload.length > Math.ceil(8 * 1024 * 1024 / 3) * 4) throw new Error("Audio frame exceeded 8 MiB");
    const binary = atob(payload);
    if (!binary.length || binary.length % 2) throw new Error("Invalid PCM16 payload length");
    if (number > this.latest) {
      for (const id of this.responses.keys()) {
        if (![...this.sources.values()].some((s) => s.responseId === id)) this.responses.delete(id);
      }
    }
    this.responses.set(responseId, progress); this.latest = number;
    progress.next++;
    let remaining = 0, fullyScheduled = false;
    const valid = () => !this.stopped && !this.muted && number > this.invalidThrough && this.responses.get(responseId) === progress;
    const completed = () => {
      if (!fullyScheduled || remaining || !valid()) return;
      progress.completed.add(sequence);
      const before = progress.played;
      while (progress.completed.delete(progress.played + 1)) progress.played++;
      if (progress.played > before) this.acknowledge(responseId, progress.played);
      if (number < this.latest && ![...this.sources.values()].some((s) => s.responseId === responseId)) this.responses.delete(responseId);
    };
    for (let offset = 0; offset < binary.length;) {
      if (!valid()) return false;
      const size = Math.min(16000, (binary.length - offset) / 2);
      const duration = size / 16000;
      const start = Math.max(this.context.currentTime + .015, this.nextTime);
      if (this.sources.size >= 64 || start + duration - this.context.currentTime > 4) {
        await new Promise((resolve) => this.waiters.add(resolve));
        continue;
      }
      const buffer = this.context.createBuffer(1, size, 16000);
      const samples = buffer.getChannelData(0);
      for (let i = 0; i < size; i++) {
        const value = binary.charCodeAt(offset + i * 2) | binary.charCodeAt(offset + i * 2 + 1) << 8;
        samples[i] = (value >= 32768 ? value - 65536 : value) / 32768;
      }
      const source = this.context.createBufferSource(); source.buffer = buffer; source.connect(this.context.destination);
      this.sources.set(source, { responseId, number, end: start + duration });
      remaining++; this.nextTime = start + duration;
      source.onended = () => {
        if (!this.sources.delete(source)) return;
        source.disconnect(); remaining--; completed(); this.wake();
      };
      source.start(start);
      if (offset === 0) onScheduled();
      offset += size * 2;
    }
    fullyScheduled = true; completed();
    return true;
  }
  wake() { for (const resolve of this.waiters) resolve(); this.waiters.clear(); }
  clear(responseId) {
    const number = Number(responseId.slice("response-".length));
    this.invalidThrough = Math.max(this.invalidThrough, number);
    for (const [source, info] of this.sources) {
      if (info.number > this.invalidThrough) continue;
      this.sources.delete(source); source.onended = null; source.stop(); source.disconnect();
    }
    for (const id of this.responses.keys()) if (Number(id.slice(9)) <= this.invalidThrough) this.responses.delete(id);
    this.nextTime = Math.max(this.context.currentTime, ...[...this.sources.values()].map((s) => s.end));
    this.wake();
  }
  interrupt() {
    this.muted = true;
    this.clear(`response-${Math.max(this.latest, this.invalidThrough)}`);
  }
  stop() { this.stopped = true; this.interrupt(); }
  confirmInterrupt() { if (!this.stopped) this.muted = false; }
}
