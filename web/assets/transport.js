const wait = (ms, signal) => new Promise((resolve, reject) => {
  signal.throwIfAborted();
  const abort = () => { clearTimeout(timer); reject(signal.reason); };
  const timer = setTimeout(() => { signal.removeEventListener("abort", abort); resolve(); }, ms);
  signal.addEventListener("abort", abort, { once: true });
});

// A session owns one ordered producer. Retry identities never change on uncertain delivery.
export class CallTransport {
  constructor(callId, fetcher = fetch) {
    this.base = `/calls/${encodeURIComponent(callId)}/run/invocations/${crypto.randomUUID()}`;
    this.fetcher = (...args) => fetcher(...args);
    this.controller = new AbortController();
    this.sequence = 0;
    this.pending = 0;
    this.tail = Promise.resolve();
    this.inputClosed = false;
  }
  async request(path, options = {}, readBody = false) {
    for (let attempt = 0; attempt < 3; attempt++) {
      this.controller.signal.throwIfAborted();
      let response;
      try {
        response = await this.fetcher(this.base + path, {
          ...options,
          signal: AbortSignal.any([this.controller.signal, AbortSignal.timeout(10000)]),
          headers: { "Content-Type": "application/json", ...options.headers },
        });
        if (response.ok) return readBody ? { response, text: await response.text() } : response;
      } catch (error) {
        if (this.controller.signal.aborted || attempt === 2) throw error;
      }
      if (response && !response.ok && response.status < 500 && response.status !== 429) {
        throw new Error(`Stream request ${options.method ?? "GET"} failed (${response.status})`);
      }
      if (attempt === 2) throw new Error(`Stream request failed (${response?.status ?? "network"})`);
      await wait(150 * 2 ** attempt + Math.random() * 100, this.controller.signal);
    }
  }
  create() { return this.request("", { method: "PUT", body: "{}" }); }
  append(events, close = false) {
    if (this.inputClosed) return Promise.reject(new Error("Call input is closed"));
    if (this.pending >= 64) return Promise.reject(new Error("Input backlog exceeded 64 frames; call stopped"));
    if (close) this.inputClosed = true;
    this.pending++;
    const body = JSON.stringify(events);
    const task = this.tail.then(async () => {
      await this.request("/streams/input", {
        method: "POST", body,
        headers: { "Producer-Id": "browser", "Producer-Epoch": "0", "Producer-Seq": String(this.sequence),
          ...(close ? { "Stream-Closed": "true" } : {}) },
      });
      this.sequence++;
    }).finally(() => { this.pending--; });
    // A failed append poisons this producer; later events cannot skip its sequence.
    this.tail = task;
    return task;
  }
  async read(onEvent) {
    let offset = "-1";
    while (!this.controller.signal.aborted) {
      const { response, text } = await this.request(`/streams/output?offset=${encodeURIComponent(offset)}`, {}, true);
      const next = response.headers.get("Stream-Next-Offset");
      if (!next) throw new Error("Output stream omitted its resume cursor");
      const events = JSON.parse(text);
      for (const event of events) await onEvent(event);
      offset = next;
      if (response.headers.get("Stream-Closed") === "true") return;
      await wait(events.length ? 10 : 100, this.controller.signal);
    }
  }
  dispose() { this.controller.abort(new DOMException("Call disposed", "AbortError")); }
}
