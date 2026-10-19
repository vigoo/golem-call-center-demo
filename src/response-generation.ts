import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { readFile } from "node:fs/promises";
import { Context, Effect, Layer, type Redacted, Schema, Stream } from "effect";
import { AiError, Chat, LanguageModel, Prompt, type Response, Tool, Toolkit } from "effect/ai";
import { FetchHttpClient } from "effect/http";
import type { ConversationTurn, ResponseId } from "./call-protocol.js";

const walkthroughToolkit = Toolkit.make(Tool.make("read_walkthrough", {
  description: "Read the entire Inside the pipeline walkthrough: all architecture boundaries, scenarios, steps, invariants, protocol examples and source references. Historical test-status labels are not live telemetry; the system prompt has current deployment facts.",
  parameters: Schema.Record(Schema.String, Schema.Never),
  success: Schema.String,
  failure: Schema.String,
  failureMode: "return",
}));

const walkthroughHandlers = walkthroughToolkit.toLayer({
  read_walkthrough: () => Effect.tryPromise({
    try: () => readFile("/reference/walkthrough-data.js", "utf8"),
    catch: () => "The bundled walkthrough could not be read. Do not claim to have read it.",
  }),
});

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

const inBandFailure = (part: Response.AnyPart, allowTools: boolean): AiError.AiError | undefined => {
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

  if (part.type === "finish" && part.reason !== "stop" && !(allowTools && part.reason === "tool-calls")) {
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
      const toolkit = yield* walkthroughToolkit.pipe(Effect.provide(walkthroughHandlers));

      return ResponseGenerator.of({
        stream: (systemPrompt, turns) =>
          Stream.unwrap(Effect.gen(function* () {
            const chat = yield* Chat.fromPrompt(buildConversationPrompt(systemPrompt, turns));
            let calledTool = false;
            let finishedToolRound = false;
            const round = (allowTools: boolean) => chat.streamText({
              prompt: [],
              toolkit,
              toolChoice: allowTools ? "auto" : "none",
            }).pipe(
              Stream.mapEffect((part) => {
                if (part.type === "tool-call") calledTool = true;
                const failure = inBandFailure(part, allowTools);
                if (allowTools && part.type === "finish" && failure === undefined) finishedToolRound = true;
                return failure === undefined ? Effect.succeed(part) : Effect.fail(failure);
              }),
              Stream.filter(
                (part): part is Response.TextDeltaPart | Response.FinishPart =>
                  part.type === "text-delta" || (part.type === "finish" && part.reason === "stop" && !(allowTools && calledTool)),
              ),
              Stream.map(
                (part): ResponseGenerationEvent => part.type === "text-delta"
                  ? { _tag: "TextDelta", text: part.delta }
                  : { _tag: "Completed" },
              ),
            );
            // One read returns the full document; a second tool round cannot add information.
            return round(true).pipe(
              Stream.concat(Stream.suspend(() => calledTool && finishedToolRound ? round(false) : Stream.empty)),
              Stream.takeUntil((event) => event._tag === "Completed"),
              Stream.provideService(LanguageModel.LanguageModel, model),
              Stream.mapError((error) => AiError.isAiError(error) ? error : AiError.make({
                module: "ResponseGenerator",
                method: "stream",
                reason: new AiError.UnknownError({ description: "The walkthrough tool failed" }),
              })),
            );
          })),
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

  return OpenAiLanguageModel.layer({ model, config: { reasoning: { effort: "low" } } }).pipe(Layer.provide(client));
};
