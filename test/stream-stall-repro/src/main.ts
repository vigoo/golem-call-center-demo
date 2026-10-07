import { defineAgent, Http, method, Websocket, WitTypes } from "@golemcloud/effect-golem";
import { Effect, Schema, Scope, Stream } from "effect";
import { Socket } from "effect/socket";

defineAgent({
  name: "StreamProbe",
  mode: "durable",
  id: { name: Schema.String },
  http: Http.mount("/probe/{name}"),
  methods: {
    run: method({
      input: { input: WitTypes.AgentStream(Schema.String) },
      success: WitTypes.AgentStream(Schema.String),
      http: [Http.post("/run", {
        durableStreams: { slots: [
          { source: "input", slot: "input", name: "input" },
          { source: "output", slot: "$result", name: "output" },
        ] },
      })],
    }),
  },
}).implement({
  init: () => Effect.void,
  methods: () => ({
    run: ({ input }) => Effect.succeed(Stream.unwrap(Effect.gen(function* () {
      const connect = (path: string) => Websocket.connect(`ws://127.0.0.1:19110/${path}`) as
        Effect.Effect<Websocket.GolemSocket, Socket.SocketError, Scope.Scope>;
      const first = yield* connect("v2/listen");
      const firstPull = yield* Socket.readerString(first);
      const writer = yield* first.writer;
      const second = yield* connect("tts");
      const secondPull = yield* Socket.readerString(second);
      yield* firstPull.pipe(Effect.forever, Effect.forkScoped);
      yield* secondPull.pipe(Effect.forever, Effect.forkScoped);
      return input.pipe(Stream.mapEffect((text) => Effect.gen(function* () {
        yield* Effect.logInfo("Input handler entered");
        yield* writer.write(new TextEncoder().encode(text));
        return text;
      })));
    }))),
  }),
});
