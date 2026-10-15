import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Context, Effect, Layer, type Redacted, Schema, Stream } from "effect";
import { AiError, LanguageModel, Prompt, type Response } from "effect/ai";
import { FetchHttpClient } from "effect/http";
import type { ConversationTurn, ResponseId } from "./call-protocol.js";

export const ResponseGenerationEvent = Schema.Union([
  Schema.TaggedStruct("TextDelta", { text: Schema.String }),
  Schema.TaggedStruct("Completed", {}),
]);
export type ResponseGenerationEvent = typeof ResponseGenerationEvent.Type;

export const buildConversationPrompt = (
  systemPrompt: string,
  turns: ReadonlyArray<ConversationTurn>,
): Prompt.Prompt =>
  Prompt.fromMessages([
    Prompt.systemMessage({ content: systemPrompt }),
    ...turns.map((turn) =>
      turn.role === "user"
        ? Prompt.userMessage({ content: [Prompt.textPart({ text: turn.text })] })
        : Prompt.assistantMessage({ content: [Prompt.textPart({ text: turn.text })] }),
    ),
  ]);

const inBandFailure = (part: Response.StreamPart<{}>): AiError.AiError | undefined => {
  if (part.type === "error") {
    return AiError.isAiError(part.error)
      ? part.error
      : AiError.make({
          module: "ResponseGenerator",
          method: "stream",
          reason: new AiError.UnknownError({
            description: "The language model reported a streaming error",
          }),
        });
  }

  if (part.type === "finish" && part.reason !== "stop") {
    return AiError.make({
      module: "ResponseGenerator",
      method: "stream",
      reason: new AiError.UnknownError({
        description: `The language model response ended with reason: ${part.reason}`,
      }),
    });
  }

  return undefined;
};

export class ResponseGenerator extends Context.Service<
  ResponseGenerator,
  {
    readonly stream: (
      systemPrompt: string,
      turns: ReadonlyArray<ConversationTurn>,
      responseId: ResponseId,
    ) => Stream.Stream<ResponseGenerationEvent, AiError.AiError>;
  }
>()("call-center/ResponseGenerator") {
  static readonly layer = Layer.effect(
    ResponseGenerator,
    Effect.gen(function* () {
      const model = yield* LanguageModel.LanguageModel;

      return ResponseGenerator.of({
        stream: (systemPrompt, turns) =>
          model
            .streamText({
              prompt: buildConversationPrompt(systemPrompt, turns),
              toolChoice: "none",
            })
            .pipe(
              Stream.mapEffect((part) => {
                const failure = inBandFailure(part);
                return failure === undefined ? Effect.succeed(part) : Effect.fail(failure);
              }),
              Stream.filter(
                (part): part is Response.TextDeltaPart | Response.FinishPart =>
                  part.type === "text-delta" || part.type === "finish",
              ),
              Stream.map(
                (part): ResponseGenerationEvent =>
                  part.type === "text-delta"
                    ? { _tag: "TextDelta", text: part.delta }
                    : { _tag: "Completed" },
              ),
              Stream.takeUntil((event) => event._tag === "Completed"),
            ),
      });
    }),
  );
}

export interface OpenAiLanguageModelOptions {
  readonly apiKey: Redacted.Redacted<string>;
  readonly apiUrl: string;
  readonly model: string;
}

export const openAiLanguageModelLayer = ({
  apiKey,
  apiUrl,
  model,
}: OpenAiLanguageModelOptions): Layer.Layer<LanguageModel.LanguageModel> => {
  const client = OpenAiClient.layer({ apiKey, apiUrl }).pipe(
    Layer.provide(FetchHttpClient.layer),
  );

  return OpenAiLanguageModel.layer({ model }).pipe(Layer.provide(client));
};
