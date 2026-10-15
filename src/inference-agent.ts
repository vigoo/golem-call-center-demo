import { defineAgent, defineConfig, method, WitTypes } from "@golemcloud/effect-golem";
import { Context, Effect, Layer, Schema, Scope, Stream } from "effect";
import { AiError } from "effect/ai";
import { CallId, ConversationTurn } from "./call-protocol.js";
import { openAiLanguageModelLayer, ResponseGenerationEvent, ResponseGenerator } from "./response-generation.js";

// Agent streams need an explicit application-error item, not a successful EOF.
const InferenceEvent = Schema.Union([
  ...ResponseGenerationEvent.members,
  Schema.TaggedStruct("Failed", {}),
]);

const InferenceAgentConfig = defineConfig("InferenceAgent.Config", {
  openAi: Schema.Struct({
    apiKey: Schema.Redacted(Schema.String),
    apiUrl: Schema.String,
    model: Schema.String,
  }),
});

// One sequential durable inference owner per call, with no sibling WebSocket writes.
export const InferenceAgent = defineAgent({
  name: "InferenceAgent",
  mode: "durable",
  id: { callId: CallId },
  config: InferenceAgentConfig,
  methods: {
    generate: method({
      input: { systemPrompt: Schema.String, turns: Schema.Array(ConversationTurn) },
      success: WitTypes.AgentStream(InferenceEvent),
      error: Schema.Struct({ reason: Schema.String }),
    }),
  },
}).implement({
  init: () => Effect.void,
  methods: () => ({
    generate: ({ systemPrompt, turns }) => Effect.gen(function* () {
      const config = yield* InferenceAgentConfig;
      const providers = ResponseGenerator.layer.pipe(Layer.provide(openAiLanguageModelLayer({
        apiKey: yield* config.openAi.apiKey.get,
        apiUrl: yield* config.openAi.apiUrl,
        model: yield* config.openAi.model,
      })));
      return Stream.unwrap(Effect.map(ResponseGenerator, (generator) => generator.stream(systemPrompt, turns))).pipe(
        Stream.provide(providers),
        Stream.catch(() => Stream.make({ _tag: "Failed" as const })),
      );
    }).pipe(Effect.mapError(() => ({ reason: "Language model generation failed" }))),
  }),
});

export const makeIsolatedInferenceLayer = (callId: typeof CallId.Type) => Effect.gen(function* () {
  // Do not retain the handler scope; each stream owns its RPC resources.
  const context = (yield* Effect.context<Exclude<Effect.Services<ReturnType<typeof InferenceAgent.client.get>>, Scope.Scope>>()).pipe(Context.omit(Scope.Scope));
  return Layer.succeed(ResponseGenerator, ResponseGenerator.of({
    stream: (systemPrompt, turns) => Stream.unwrap(Effect.gen(function* () {
      const remote = yield* InferenceAgent.client.get({ callId });
      const output = yield* remote.generate({ systemPrompt, turns });
      return output.pipe(Stream.mapEffect((event) => event._tag === "Failed"
        ? Effect.fail({ reason: "Language model generation failed" })
        : Effect.succeed(event)));
    })).pipe(
      Stream.mapError(() => AiError.make({
        module: "InferenceAgent", method: "generate",
        reason: new AiError.UnknownError({ description: "Remote inference failed" }),
      })),
      Stream.provideContext(context),
    ),
  }));
});
