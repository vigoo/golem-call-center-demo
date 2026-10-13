// Local protocol fixtures only; never substitute these for live-provider verification.
import { createServer } from "node:http";
import { WebSocketServer } from "ws";

const newScenario = (settings = {}) => ({ settings, counts: { stt: 0, openai: 0, elevenlabs: 0 }, connections: { stt: 0, elevenlabs: 0 }, journal: [], held: new Map(), resources: new Set(), controls: new Set(), released: false });
let scenario = newScenario();
const deliver = (state, stage, owner, transmit) => {
  if (state.settings.holdStage === stage && !state.released) state.held.set(owner, { stage, transmit });
  else transmit();
};
const server = createServer(async (request, response) => {
  if (request.url === "/test/control") {
    if (request.method === "POST") {
      let body = ""; for await (const chunk of request) body += chunk;
      const settings = JSON.parse(body);
      if (settings.event) {
        for (const trigger of scenario.controls) trigger(settings.event);
      } else if (settings.release) {
        scenario.released = true;
        const held = [...scenario.held.values()]; scenario.held.clear();
        for (const item of held) item.transmit();
      } else {
        for (const dispose of scenario.resources) dispose();
        scenario.held.clear(); scenario = newScenario(settings);
      }
    }
    const pending = { stt: 0, openai: 0, elevenlabs: 0 };
    for (const item of scenario.held.values()) pending[item.stage]++;
    response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ ...scenario.counts, connections: scenario.connections, journal: scenario.journal, pending })); return;
  }
  if (request.url !== "/v1/responses") {
    response.writeHead(404).end();
    return;
  }
  const state = scenario;
  const dispose = () => response.destroy(); state.resources.add(dispose);
  response.on("close", () => { state.resources.delete(dispose); state.held.delete(response); });
  for await (const _ of request) { /* consume the request body */ }
  state.counts.openai++;
  state.journal.push({ stage: "openai", operation: "request", attempt: state.counts.openai });
  await new Promise((resolve) => {
    response.once("close", resolve);
    deliver(state, "openai", response, resolve);
  });
  if (response.destroyed) return;
  if (state.settings.failStage === "openai") { response.writeHead(400).end("Fixture failure"); return; }
  response.writeHead(200, { "content-type": "text/event-stream" });
  const send = (event) => response.write(`data: ${JSON.stringify(event)}\n\n`);
  send({ type: "response.output_text.delta", item_id: "message-1", output_index: 0,
    content_index: 0, sequence_number: 0, logprobs: [], delta: "Hello from the fixture." });
  send({ type: "response.completed", sequence_number: 1, response: {
    id: "fixture-response", status: "completed", model: "fixture",
    object: "response", created_at: 0, metadata: null, temperature: null, top_p: null,
    tools: [], tool_choice: "none", error: null, output: [], instructions: null,
    parallel_tool_calls: false,
    incomplete_details: null,
    usage: { input_tokens: 10, output_tokens: 6, total_tokens: 16,
      input_tokens_details: { cached_tokens: 0 }, output_tokens_details: { reasoning_tokens: 0 } },
  } });
  response.end();
});
const sockets = new WebSocketServer({ server });
sockets.on("connection", (socket, request) => {
  const deepgram = request.url.startsWith("/v2/listen");
  const state = scenario;
  const settings = state.settings;
  const stage = deepgram ? "stt" : "elevenlabs";
  const connection = ++state.connections[stage];
  state.journal.push({ stage, operation: "connect", connection });
  const contexts = new Set();
  let audioBytes = 0;
  const dispose = () => socket.terminate(); state.resources.add(dispose);
  const send = (stage, event) => {
    const transmit = () => { if (socket.readyState === 1) socket.send(JSON.stringify(event)); };
    // Keep all messages for one held socket, including audio followed by final.
    const previous = state.held.get(socket)?.transmit;
    deliver(state, stage, socket, previous ? () => { previous(); transmit(); } : transmit);
  };
  socket.on("close", () => { state.resources.delete(dispose); state.held.delete(socket); state.journal.push({ stage, operation: "close", connection }); });
  let sequence = 0;
  let lastTurn = 0;
  if (deepgram) socket.send(JSON.stringify({
    type: "Connected", request_id: "fixture", sequence_id: sequence++,
  }));
  if (deepgram) {
    const trigger = (event) => {
      if (event === "fatal") { socket.close(1011, "Fixture failure"); return; }
      if (event === "barge-in") send("stt", { type: "TurnInfo", request_id: "fixture", sequence_id: sequence++,
        event: "StartOfTurn", turn_index: sequence, transcript: "New speech",
        words: [], audio_window_start: 2, audio_window_end: 2.08, end_of_turn_confidence: 0.5 });
    };
    state.controls.add(trigger); socket.on("close", () => state.controls.delete(trigger));
  }
  socket.on("message", (bytes, binary) => {
    if (deepgram && binary) {
      const start = audioBytes / 32000;
      audioBytes += bytes.length;
      state.journal.push({ stage, operation: "audio", connection, bytes: bytes.length, audioBytes });
      // A continuous microphone should not invent a new turn for every 80 ms frame.
      if (settings.singleTurn && lastTurn) return;
      if (Date.now() - lastTurn < 1000) return;
      lastTurn = Date.now();
      state.counts.stt++;
      if (settings.failStage === "stt") { socket.close(1011, "Fixture failure"); return; }
      send("stt", {
        type: "TurnInfo", request_id: "fixture", sequence_id: sequence++,
        event: "EndOfTurn", turn_index: sequence,
        transcript: bytes.length >= 2 && bytes.readInt16LE(0) === 1234 ? "Second fixture turn" : "Hello fixture",
        words: [], audio_window_start: start, audio_window_end: audioBytes / 32000,
        end_of_turn_confidence: 0.99,
      });
      return;
    }
    const message = JSON.parse(bytes.toString());
    if (deepgram) {
      if (message.type === "CloseStream") socket.close();
      return;
    }
    if (message.close_socket) { socket.close(); return; }
    if (message.text === " " && message.voice_settings) {
      if (contexts.has(message.context_id)) { socket.close(1008, "Context already initialized"); return; }
      contexts.add(message.context_id);
      state.journal.push({ stage, operation: "initialize", connection, responseId: message.context_id });
      return;
    }
    if (!contexts.has(message.context_id)) {
      state.journal.push({ stage, operation: "invalid-context", connection, responseId: message.context_id });
      socket.close(1008, "Context not initialized on this socket"); return;
    }
    if (message.text?.trim()) {
      state.journal.push({ stage, operation: "text", connection, responseId: message.context_id, text: message.text });
      state.counts.elevenlabs++;
      if (settings.failStage === "elevenlabs") { socket.close(1011, "Fixture failure"); return; }
      const samples = Buffer.alloc(Math.round(32000 * (settings.audioSeconds ?? .1)));
      for (let i = 0; i < samples.length / 2; i++) {
        samples.writeInt16LE(Math.round(2000 * Math.sin(i * Math.PI * 2 * 440 / 16000)), i * 2);
      }
      for (let chunk = 0; chunk < (settings.audioChunks ?? 1); chunk++) {
        send("elevenlabs", { contextId: message.context_id, audio: samples.toString("base64"), isFinal: null });
      }
    }
    if (message.close_context) {
      contexts.delete(message.context_id);
      send("elevenlabs", { contextId: message.context_id, isFinal: true });
    }
  });
});
server.listen(19110, "127.0.0.1", () => console.log("Provider fixtures ready on 19110"));
