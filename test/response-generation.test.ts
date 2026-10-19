import { describe, expect, it } from "@effect/vitest";
import { readFileSync } from "node:fs";
import { vi } from "vitest";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Deferred, Effect, Fiber, Layer, Redacted, Stream } from "effect";
import { AiError, LanguageModel, type Prompt } from "effect/ai";
import { HttpClient, HttpClientResponse } from "effect/http";
import type { ConversationTurn, ResponseId } from "../src/call-protocol.js";
import {
  ResponseGenerator,
  type ResponseGenerationEvent,
} from "../src/response-generation.js";

const walkthroughRead = vi.hoisted(() => vi.fn());
vi.mock("node:fs/promises", () => ({ readFile: walkthroughRead }));

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
  it.effect("offers the no-argument tool through the OpenAI Responses adapter", () =>
    Effect.gen(function* () {
      const sse = [
        { type: "response.output_text.delta", item_id: "message-1", output_index: 0,
          content_index: 0, sequence_number: 0, logprobs: [], delta: "Hello." },
        { type: "response.completed", sequence_number: 1, response: {
          id: "adapter-test", status: "completed", model: "gpt-6.1-sol", object: "response",
          created_at: 0, metadata: null, temperature: null, top_p: null, tools: [],
          tool_choice: "auto", error: null, output: [], instructions: null,
          parallel_tool_calls: false, incomplete_details: null,
          usage: { input_tokens: 10, output_tokens: 2, total_tokens: 12,
            input_tokens_details: { cached_tokens: 0 }, output_tokens_details: { reasoning_tokens: 0 } },
        } },
      ].map((event) => `data: ${JSON.stringify(event)}\n\n`).join("");
      const http = HttpClient.make((request) => Effect.succeed(HttpClientResponse.fromWeb(request,
        new globalThis.Response(sse, { headers: { "content-type": "text/event-stream" } }))));
      const client = OpenAiClient.layer({ apiKey: Redacted.make("test-key") }).pipe(
        Layer.provide(Layer.succeed(HttpClient.HttpClient, http)));
      const model = OpenAiLanguageModel.layer({ model: "gpt-6.1-sol" }).pipe(Layer.provide(client));
      const events = yield* provideGenerator(Stream.unwrap(Effect.map(ResponseGenerator, (generator) =>
        generator.stream("system", turns, responseId))), model).pipe(Stream.runCollect);
      expect(events).toEqual([{ _tag: "TextDelta", text: "Hello." }, { _tag: "Completed" }]);
    }),
  );

  it.effect("independent tool calls preserve second-round failures", () =>
    Effect.gen(function* () {
      walkthroughRead.mockResolvedValue("walkthrough");
      const failure = AiError.make({ module: "review", method: "stream", reason: new AiError.UnknownError({ description: "failed second round" }) });
      let requests = 0;
      const model = fakeLanguageModel((options) => {
        requests++;
        const hasResult = options.prompt.content.some((message) => message.role === "tool");
        return hasResult ? Stream.fail(failure) : Stream.make(
          { type: "tool-call", id: "read", name: "read_walkthrough", params: {} },
          { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
        );
      });
      yield* Effect.gen(function* () {
        const generator = yield* ResponseGenerator;
        for (let i = 0; i < 2; i++) {
          const result = yield* generator.stream("system", turns, responseId).pipe(Stream.runCollect, Effect.result);
          expect(result._tag).toBe("Failure");
          if (result._tag === "Failure") expect(result.failure).toBe(failure);
        }
      }).pipe(Effect.provide(ResponseGenerator.layer), Effect.provide(model));
      expect(requests).toBe(4);
    }),
  );

  it.effect("cancellation during tool read prevents second round", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      let requests = 0;
      walkthroughRead.mockImplementationOnce(() => {
        Effect.runSync(Deferred.succeed(started, undefined));
        return new Promise(() => {});
      });
      const model = fakeLanguageModel(() => {
        requests++;
        return Stream.make(
          { type: "tool-call", id: "read", name: "read_walkthrough", params: {} },
          { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
        );
      });
      const fiber = yield* provideGenerator(Stream.unwrap(Effect.map(ResponseGenerator, (generator) =>
        generator.stream("system", turns, responseId))), model).pipe(Stream.runDrain, Effect.forkChild);
      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      expect(requests).toBe(1);
    }),
  );

  it.effect("does not complete after a truncated tool round", () =>
    Effect.gen(function* () {
      walkthroughRead.mockResolvedValueOnce("walkthrough");
      let requests = 0;
      const model = fakeLanguageModel(() => ++requests === 1
        ? Stream.make({ type: "tool-call", id: "read-truncated", name: "read_walkthrough", params: {} })
        : Stream.make(
          { type: "text-delta", id: "answer", delta: "Finished" },
          { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
        ));
      const events: Array<ResponseGenerationEvent> = [];
      yield* provideGenerator(Stream.unwrap(Effect.map(ResponseGenerator, (generator) =>
        generator.stream("system", turns, responseId))), model).pipe(
          Stream.tap((event) => Effect.sync(() => events.push(event))),
          Stream.runDrain,
          Effect.result,
        );
      expect(events.some((event) => event._tag === "Completed")).toBe(false);
      expect(requests).toBe(1);
    }),
  );

  it.effect("reads the full walkthrough and streams an answer after the tool result", () =>
    Effect.gen(function* () {
      const walkthrough = readFileSync(new URL("../web/assets/walkthrough-data.js", import.meta.url), "utf8");
      walkthroughRead.mockResolvedValueOnce(walkthrough);
      const model = fakeLanguageModel((options) => {
        const result = options.prompt.content.flatMap((message) => message.role === "tool" ? message.content : [])
          .find((part) => part.type === "tool-result");
        if (!result) return Stream.make(
          { type: "tool-call", id: "read-1", name: "read_walkthrough", params: {} },
          { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
        );
        expect(result.isFailure).toBe(false);
        expect(result.result).toBe(walkthrough);
        expect(options.toolChoice).toBe("none");
        return Stream.make(
          { type: "text-delta", id: "answer", delta: "The walkthrough explains heard acknowledgements." },
          { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
        );
      });
      const events = yield* provideGenerator(Stream.unwrap(Effect.map(ResponseGenerator, (generator) =>
        generator.stream("Read the walkthrough", turns, responseId))), model).pipe(Stream.runCollect);
      expect(events).toEqual([
        { _tag: "TextDelta", text: "The walkthrough explains heard acknowledgements." },
        { _tag: "Completed" },
      ]);
    }),
  );

  it.effect("reports an unreadable walkthrough to the model without claiming a successful read", () =>
    Effect.gen(function* () {
      walkthroughRead.mockRejectedValueOnce(new Error("private filesystem detail"));
      const model = fakeLanguageModel((options) => {
        const result = options.prompt.content.flatMap((message) => message.role === "tool" ? message.content : [])
          .find((part) => part.type === "tool-result");
        if (!result) return Stream.make(
          { type: "tool-call", id: "read-1", name: "read_walkthrough", params: {} },
          { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
        );
        expect(result.isFailure).toBe(true);
        expect(result.result).toContain("could not be read");
        expect(result.result).not.toContain("private filesystem detail");
        return Stream.make(
          { type: "text-delta", id: "answer", delta: "I could not read the walkthrough." },
          { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
        );
      });
      const events = yield* provideGenerator(Stream.unwrap(Effect.map(ResponseGenerator, (generator) =>
        generator.stream("Read the walkthrough", turns, responseId))), model).pipe(Stream.runCollect);
      expect(events).toEqual([
        { _tag: "TextDelta", text: "I could not read the walkthrough." },
        { _tag: "Completed" },
      ]);
    }),
  );

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
