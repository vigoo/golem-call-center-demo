import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Stream } from "effect";
import { AiError, LanguageModel, type Prompt } from "effect/ai";
import type { ConversationTurn, ResponseId } from "../src/call-protocol.js";
import {
  ResponseGenerator,
  type ResponseGenerationEvent,
} from "../src/response-generation.js";

const responseId = "response-test" as ResponseId;
const turns: ReadonlyArray<ConversationTurn> = [
  {
    turnId: "turn-3" as ConversationTurn["turnId"],
    role: "user",
    text: "First question",
    responseId: null,
  },
  {
    turnId: "turn-4" as ConversationTurn["turnId"],
    role: "assistant",
    text: "First answer",
    responseId: "response-7" as ConversationTurn["responseId"],
  },
  {
    turnId: "turn-5" as ConversationTurn["turnId"],
    role: "user",
    text: "Follow-up question",
    responseId: null,
  },
];

const fakeLanguageModel = (
  streamText: Parameters<typeof LanguageModel.make>[0]["streamText"],
) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText,
    }),
  );

const provideGenerator = <E>(
  stream: Stream.Stream<ResponseGenerationEvent, E, ResponseGenerator>,
  model: Layer.Layer<LanguageModel.LanguageModel>,
) => stream.pipe(Stream.provide(ResponseGenerator.layer), Stream.provide(model));

describe("response generation", () => {
  it.effect("preserves system and committed-turn ordering", () =>
    Effect.gen(function* () {
      let capturedPrompt: Prompt.Prompt | undefined;
      const model = fakeLanguageModel((options) => {
        capturedPrompt = options.prompt;
        return Stream.empty;
      });

      yield* provideGenerator(
        Stream.unwrap(
          Effect.map(ResponseGenerator, (generator) =>
            generator.stream("Keep answers concise", turns, responseId),
          ),
        ),
        model,
      ).pipe(Stream.runDrain);

      expect(
        capturedPrompt?.content.map((message) => ({
          role: message.role,
          text:
            message.role === "system"
              ? message.content
              : message.content
                  .filter((part) => part.type === "text")
                  .map((part) => part.text)
                  .join(""),
        })),
      ).toEqual([
        { role: "system", text: "Keep answers concise" },
        { role: "user", text: "First question" },
        { role: "assistant", text: "First answer" },
        { role: "user", text: "Follow-up question" },
      ]);
    }),
  );

  it.effect("emits only text deltas followed by completion", () =>
    Effect.gen(function* () {
      const model = fakeLanguageModel(() =>
        Stream.make(
          { type: "text-start", id: "answer" },
          { type: "text-delta", id: "answer", delta: "Hello" },
          { type: "text-end", id: "answer" },
          { type: "text-delta", id: "answer", delta: " there" },
          {
            type: "finish",
            reason: "stop",
            usage: { inputTokens: {}, outputTokens: {} },
          },
        ),
      );

      const events = yield* provideGenerator(
        Stream.unwrap(
          Effect.map(ResponseGenerator, (generator) => generator.stream("system", turns, responseId)),
        ),
        model,
      ).pipe(Stream.runCollect);

      expect(events).toEqual([
        { _tag: "TextDelta", text: "Hello" },
        { _tag: "TextDelta", text: " there" },
        { _tag: "Completed" },
      ]);
    }),
  );

  it.effect("preserves typed AI failures without emitting completion", () =>
    Effect.gen(function* () {
      const failure = AiError.make({
        module: "FakeLanguageModel",
        method: "streamText",
        reason: new AiError.UnknownError({ description: "provider unavailable" }),
      });
      const model = fakeLanguageModel(() =>
        Stream.make({ type: "text-delta", id: "answer", delta: "partial" }).pipe(
          Stream.concat(Stream.fail(failure)),
        ),
      );

      const result = yield* provideGenerator(
        Stream.unwrap(
          Effect.map(ResponseGenerator, (generator) => generator.stream("system", turns, responseId)),
        ),
        model,
      ).pipe(Stream.runCollect, Effect.result);

      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure).toBe(failure);
    }),
  );

  it.effect("fails safely on an in-band provider error without emitting completion", () =>
    Effect.gen(function* () {
      const emitted: Array<ResponseGenerationEvent> = [];
      const model = fakeLanguageModel(() =>
        Stream.make(
          { type: "text-delta", id: "answer", delta: "partial" },
          { type: "error", error: { privateProviderDetail: "must not leak" } },
        ),
      );

      const result = yield* provideGenerator(
        Stream.unwrap(
          Effect.map(ResponseGenerator, (generator) => generator.stream("system", turns, responseId)),
        ),
        model,
      ).pipe(
        Stream.tap((event) => Effect.sync(() => emitted.push(event))),
        Stream.runDrain,
        Effect.result,
      );

      expect(result._tag).toBe("Failure");
      expect(emitted).toEqual([{ _tag: "TextDelta", text: "partial" }]);
      expect(JSON.stringify(result)).not.toContain("must not leak");
    }),
  );

  it.effect("fails on an error finish without emitting completion", () =>
    Effect.gen(function* () {
      const emitted: Array<ResponseGenerationEvent> = [];
      const model = fakeLanguageModel(() =>
        Stream.make({
          type: "finish",
          reason: "error",
          usage: { inputTokens: {}, outputTokens: {} },
        }),
      );

      const result = yield* provideGenerator(
        Stream.unwrap(
          Effect.map(ResponseGenerator, (generator) => generator.stream("system", turns, responseId)),
        ),
        model,
      ).pipe(
        Stream.tap((event) => Effect.sync(() => emitted.push(event))),
        Stream.runDrain,
        Effect.result,
      );

      expect(result._tag).toBe("Failure");
      expect(emitted).toEqual([]);
    }),
  );

  it.effect("does not report completion when generation stops at the token limit", () =>
    Effect.gen(function* () {
      const emitted: Array<ResponseGenerationEvent> = [];
      const model = fakeLanguageModel(() =>
        Stream.make(
          { type: "text-delta", id: "answer", delta: "truncated" },
          {
            type: "finish",
            reason: "length",
            usage: { inputTokens: {}, outputTokens: {} },
          },
        ),
      );

      const result = yield* provideGenerator(
        Stream.unwrap(
          Effect.map(ResponseGenerator, (generator) => generator.stream("system", turns, responseId)),
        ),
        model,
      ).pipe(
        Stream.tap((event) => Effect.sync(() => emitted.push(event))),
        Stream.runDrain,
        Effect.result,
      );

      expect(result._tag).toBe("Failure");
      expect(emitted).toEqual([{ _tag: "TextDelta", text: "truncated" }]);
    }),
  );

  it.effect("does not report completion when the provider stream ends without a finish", () =>
    Effect.gen(function* () {
      const emitted: Array<ResponseGenerationEvent> = [];
      const model = fakeLanguageModel(() =>
        Stream.make({ type: "text-delta", id: "answer", delta: "truncated" }),
      );

      yield* provideGenerator(
        Stream.unwrap(
          Effect.map(ResponseGenerator, (generator) => generator.stream("system", turns, responseId)),
        ),
        model,
      ).pipe(
        Stream.tap((event) => Effect.sync(() => emitted.push(event))),
        Stream.runDrain,
        Effect.result,
      );

      expect(emitted).toEqual([{ _tag: "TextDelta", text: "truncated" }]);
    }),
  );

  it.effect("interrupts and finalizes the provider stream", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const finalized = yield* Deferred.make<void>();
      const model = fakeLanguageModel(() =>
        Stream.unwrap(
          Deferred.succeed(started, undefined).pipe(Effect.as(Stream.never)),
        ).pipe(Stream.ensuring(Deferred.succeed(finalized, undefined))),
      );
      const drain = provideGenerator(
        Stream.unwrap(
          Effect.map(ResponseGenerator, (generator) => generator.stream("system", turns, responseId)),
        ),
        model,
      ).pipe(Stream.runDrain);

      const fiber = yield* Effect.forkChild(drain);
      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);

      expect(yield* Deferred.isDone(finalized)).toBe(true);
    }),
  );
});
