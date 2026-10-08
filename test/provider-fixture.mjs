// Local protocol fixtures only; never substitute these for live-provider verification.
import { createServer } from "node:http";
import { WebSocketServer } from "ws";

const server = createServer(async (request, response) => {
  if (request.url !== "/v1/responses") {
    response.writeHead(404).end();
    return;
  }
  for await (const _ of request) { /* consume the request body */ }
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
  let sequence = 0;
  let lastTurn = 0;
  if (deepgram) socket.send(JSON.stringify({
    type: "Connected", request_id: "fixture", sequence_id: sequence++,
  }));
  socket.on("message", (bytes, binary) => {
    if (deepgram && binary) {
      // A continuous microphone should not invent a new turn for every 80 ms frame.
      if (Date.now() - lastTurn < 1000) return;
      lastTurn = Date.now();
      socket.send(JSON.stringify({
        type: "TurnInfo", request_id: "fixture", sequence_id: sequence++,
        event: "EndOfTurn", turn_index: sequence, transcript: "Hello fixture",
        words: [], audio_window_start: 0, audio_window_end: 0.08,
        end_of_turn_confidence: 0.99,
      }));
      return;
    }
    const message = JSON.parse(bytes.toString());
    if (deepgram) {
      if (message.type === "CloseStream") socket.close();
      return;
    }
    if (message.text?.trim()) {
      const samples = Buffer.alloc(3200);
      for (let i = 0; i < 1600; i++) {
        samples.writeInt16LE(Math.round(2000 * Math.sin(i * Math.PI * 2 * 440 / 16000)), i * 2);
      }
      socket.send(JSON.stringify({ contextId: message.context_id, audio: samples.toString("base64") }));
    }
    if (message.close_context) socket.send(JSON.stringify({ contextId: message.context_id, isFinal: true }));
    if (message.close_socket) socket.close();
  });
});
server.listen(19110, "127.0.0.1", () => console.log("Provider fixtures ready on 19110"));
