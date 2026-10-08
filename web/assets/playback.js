// Scheduled sources are the queue; both their count and audio duration are bounded.
export class PcmPlayback {
  constructor(context, acknowledge) {
    this.context = context; this.acknowledge = acknowledge;
    this.sources = new Map(); this.responses = new Map();
    this.invalidThrough = 0; this.latest = 0; this.nextTime = 0; this.muted = false;
  }
  push({ responseId, sequence, payload }) {
    const number = Number(responseId.slice("response-".length));
    if (!Number.isSafeInteger(number) || number < 1 || responseId !== `response-${number}`) throw new Error("Invalid audio response ID");
    if (this.muted) { this.invalidThrough = Math.max(this.invalidThrough, number); return false; }
    if (number <= this.invalidThrough || number < this.latest) return false;
    const progress = this.responses.get(responseId) ?? { next: 0, played: -1, completed: new Set() };
    if (sequence < progress.next) return false;
    if (sequence !== progress.next) throw new Error("Audio sequence gap");
    const binary = atob(payload);
    if (!binary.length || binary.length % 2) throw new Error("Invalid PCM16 payload length");
    const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0));
    const view = new DataView(bytes.buffer);
    const duration = bytes.length / 2 / 16000;
    const start = Math.max(this.context.currentTime + .015, this.nextTime);
    if (this.sources.size >= 64 || start + duration - this.context.currentTime > 8) throw new Error("Playback backlog exceeded 8 seconds or 64 chunks");
    const buffer = this.context.createBuffer(1, bytes.length / 2, 16000);
    const samples = buffer.getChannelData(0);
    for (let i = 0; i < samples.length; i++) samples[i] = view.getInt16(i * 2, true) / 32768;
    const source = this.context.createBufferSource(); source.buffer = buffer; source.connect(this.context.destination);
    if (number > this.latest) {
      for (const id of this.responses.keys()) {
        if (![...this.sources.values()].some((s) => s.responseId === id)) this.responses.delete(id);
      }
    }
    this.sources.set(source, { responseId, number, end: start + duration });
    this.responses.set(responseId, progress); this.latest = number;
    progress.next++; this.nextTime = start + duration;
    source.onended = () => {
      if (!this.sources.delete(source)) return;
      source.disconnect(); progress.completed.add(sequence);
      const before = progress.played;
      while (progress.completed.delete(progress.played + 1)) progress.played++;
      if (progress.played > before) this.acknowledge(responseId, progress.played);
      if (number < this.latest && ![...this.sources.values()].some((s) => s.responseId === responseId)) this.responses.delete(responseId);
    };
    source.start(start);
    return true;
  }
  clear(responseId) {
    const number = Number(responseId.slice("response-".length));
    this.invalidThrough = Math.max(this.invalidThrough, number);
    for (const [source, info] of this.sources) {
      if (info.number > this.invalidThrough) continue;
      this.sources.delete(source); source.onended = null; source.stop(); source.disconnect();
    }
    for (const id of this.responses.keys()) if (Number(id.slice(9)) <= this.invalidThrough) this.responses.delete(id);
    this.nextTime = Math.max(this.context.currentTime, ...[...this.sources.values()].map((s) => s.end));
  }
  interrupt() {
    this.muted = true;
    this.clear(`response-${Math.max(this.latest, this.invalidThrough)}`);
  }
  confirmInterrupt() { this.muted = false; }
}
