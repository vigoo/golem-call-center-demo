import { defineAgent, defineConfig, method } from "@golemcloud/effect-golem";
import { Context, Effect, Layer, Schema, Scope, Stream } from "effect";
import { AiError } from "effect/ai";
import { CallId, ConversationTurn } from "./call-protocol.js";
import { collectResponseText, openAiLanguageModelLayer, ResponseGenerator } from "./response-generation.js";

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
      success: Schema.String,
      error: Schema.Struct({ reason: Schema.String }),
    }),
  },
}).implement({
  init: () => Effect.void,
  methods: () => ({
    generate: ({ systemPrompt, turns }) => Effect.gen(function* () {
      const config = yield* InferenceAgentConfig;
      return yield* collectResponseText(systemPrompt, turns).pipe(
        Effect.provide(ResponseGenerator.layer.pipe(Layer.provide(openAiLanguageModelLayer({
          apiKey: yield* config.openAi.apiKey.get,
          apiUrl: yield* config.openAi.apiUrl,
          model: yield* config.openAi.model,
        })))),
      );
    }).pipe(Effect.mapError(() => ({ reason: "Language model generation failed" }))),
  }),
});

export const makeBufferedInferenceLayer = (callId: typeof CallId.Type) => Effect.gen(function* () {
  // Do not retain the handler scope; each stream owns its RPC resources.
  const context = (yield* Effect.context<Exclude<Effect.Services<ReturnType<typeof InferenceAgent.client.get>>, Scope.Scope>>()).pipe(Context.omit(Scope.Scope));
  return Layer.succeed(ResponseGenerator, ResponseGenerator.of({
    stream: (systemPrompt, turns) => Stream.unwrap(Effect.gen(function* () {
      const remote = yield* InferenceAgent.client.get({ callId });
      const text = yield* remote.generate({ systemPrompt, turns });
      return Stream.make({ _tag: "TextDelta" as const, text }, { _tag: "Completed" as const });
    }).pipe(Effect.mapError(() => AiError.make({
      module: "InferenceAgent", method: "generate",
      reason: new AiError.UnknownError({ description: "Remote inference failed" }),
    })))).pipe(Stream.provideContext(context)),
  }));
});
