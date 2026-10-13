import { describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Queue,
  Redacted,
  Ref,
  Stream,
} from "effect";
import { AiError } from "effect/ai";
import { coordinateCall } from "../src/call-coordinator.js";
import type {
  CallInput,
  CallOutput,
  ResponseId,
  SavedCallState,
} from "../src/call-protocol.js";
import { initialCallState } from "../src/call-state.js";
import {
  DeepgramTransportError,
  DeepgramFlux,
  type FluxEvent,
  type FluxSession,
} from "../src/deepgram-flux.js";
import {
  type ElevenLabsEvent,
  type ElevenLabsSession,
  ElevenLabsTts,
  ElevenLabsTransport,
  ElevenLabsTransportError,
} from "../src/elevenlabs-tts.js";
import {
  type ResponseGenerationEvent,
  ResponseGenerator,
} from "../src/response-generation.js";

interface Harness {
  readonly layer: Layer.Layer<DeepgramFlux | ElevenLabsTts | ResponseGenerator>;
  readonly fluxEvents: Queue.Queue<FluxEvent, DeepgramTransportError>;
  readonly ttsEvents: Queue.Queue<ElevenLabsEvent, ElevenLabsTransportError>;
  readonly ttsConnections: Array<Queue.Queue<ElevenLabsEvent, ElevenLabsTransportError>>;
  readonly sentAudio: Array<Uint8Array>;
  readonly startedContexts: Array<ResponseId>;
  readonly text: Array<{ responseId: ResponseId; text: string }>;
  readonly finishedContexts: Array<ResponseId>;
  readonly closedContexts: Array<ResponseId>;
  readonly deepgramClosed: Ref.Ref<number>;
  readonly ttsClosed: Ref.Ref<number>;
}

const makeHarness = (
  responseStreams: ReadonlyArray<Stream.Stream<ResponseGenerationEvent, AiError.AiError>>,
  onTtsReceive: Effect.Effect<void> = Effect.void,
  onFluxReceived: (event: FluxEvent) => Effect.Effect<void> = () => Effect.void,
  onSendAudio: Effect.Effect<void> = Effect.void,
  onSendText: (id: ResponseId, text: string) => Effect.Effect<void, ElevenLabsTransportError> = () => Effect.void,
  onTtsOpen: Effect.Effect<void> = Effect.void,
) =>
  Effect.gen(function* () {
    const fluxEvents = yield* Queue.unbounded<FluxEvent, DeepgramTransportError>();
    const ttsEvents = yield* Queue.unbounded<ElevenLabsEvent, ElevenLabsTransportError>();
    const ttsConnections: Harness["ttsConnections"] = [];
    const sentAudio: Array<Uint8Array> = [];
    const startedContexts: Array<ResponseId> = [];
    const text: Array<{ responseId: ResponseId; text: string }> = [];
    const finishedContexts: Array<ResponseId> = [];
    const closedContexts: Array<ResponseId> = [];
    const deepgramClosed = yield* Ref.make(0);
    const ttsClosed = yield* Ref.make(0);
    let generation = 0;

    const fluxSession: FluxSession = {
      receive: Queue.take(fluxEvents).pipe(Effect.tap(onFluxReceived)),
      commitTurn: (_event, accept) => accept,
      sendAudio: (payload) => Effect.sync(() => sentAudio.push(payload)).pipe(Effect.andThen(onSendAudio)),
      forceEndTurn: Effect.void,
      configure: () => Effect.void,
      closeStream: Ref.update(deepgramClosed, (count) => count + 1).pipe(
        Effect.andThen(
          Queue.fail(
            fluxEvents,
            new DeepgramTransportError({
              operation: "receive",
              reason: "closed by test",
            }),
          ),
        ),
        Effect.andThen(Effect.yieldNow),
        Effect.asVoid,
      ),
    };
    const ttsSession: ElevenLabsSession = {
      startContext: (responseId) =>
        Effect.sync(() => startedContexts.push(responseId)),
      sendText: (responseId, value) =>
        Effect.sync(() => text.push({ responseId, text: value })).pipe(Effect.andThen(onSendText(responseId, value))),
      finishContext: (responseId) =>
        Effect.sync(() => finishedContexts.push(responseId)),
      closeContext: (responseId) =>
        Effect.sync(() => closedContexts.push(responseId)),
      receive: onTtsReceive.pipe(Effect.andThen(Queue.take(ttsEvents))),
      retireSocket: Ref.update(ttsClosed, (count) => count + 1).pipe(Effect.andThen(Effect.sync(() => {
        for (const id of startedContexts) if (!closedContexts.includes(id)) closedContexts.push(id);
      }))),
      closeSocket: Ref.update(ttsClosed, (count) => count + 1).pipe(
        Effect.andThen(
          Queue.fail(
            ttsEvents,
            new ElevenLabsTransportError({
              operation: "receive",
              reason: "closed by test",
            }),
          ),
        ),
        Effect.andThen(Effect.yieldNow),
        Effect.asVoid,
      ),
    };

    const layer = Layer.mergeAll(
      Layer.succeed(
        DeepgramFlux,
        DeepgramFlux.of({ open: () => Effect.succeed(fluxSession) }),
      ),
      Layer.succeed(
        ElevenLabsTts,
        ElevenLabsTts.of({ open: () => Effect.gen(function*() {
          const events = ttsConnections.length === 0 ? ttsEvents : yield* Queue.unbounded<ElevenLabsEvent, ElevenLabsTransportError>();
          ttsConnections.push(events);
          if (ttsConnections.length > 1) yield* onTtsOpen;
          return { ...ttsSession, receive: onTtsReceive.pipe(Effect.andThen(Queue.take(events))) };
        }) }),
      ),
      Layer.succeed(
        ResponseGenerator,
        ResponseGenerator.of({
          stream: () => responseStreams[generation++] ?? Stream.empty,
        }),
      ),
    );

    return {
      layer,
      fluxEvents,
      ttsEvents,
      ttsConnections,
      sentAudio,
      startedContexts,
      text,
      finishedContexts,
      closedContexts,
      deepgramClosed,
      ttsClosed,
    } satisfies Harness;
  });

const options = (
  states: Array<SavedCallState>,
  outputQueueCapacity = 16,
) => ({
  initialState: initialCallState(),
  deepgramApiKey: Redacted.make("deepgram-secret"),
  elevenLabsApiKey: Redacted.make("elevenlabs-secret"),
  elevenLabs: {
    voiceId: "voice",
    modelId: "model",
    outputFormat: "pcm_16000",
  },
  systemPrompt: "Be helpful",
  outputQueueCapacity,
  onStateChange: (state: SavedCallState) =>
    Effect.sync(() => states.push(state)),
});

const turn = (
  kind: Extract<FluxEvent, { _tag: "Turn" }>["kind"],
  transcript: string,
  sequence: number,
): FluxEvent => ({
  _tag: "Turn",
  kind,
  requestId: "request",
  sequence,
  turnIndex: sequence,
  transcript,
  endOfTurnConfidence: 0.9,
});

const waitUntil = (predicate: () => boolean): Effect.Effect<void> =>
  Effect.suspend(() =>
    predicate()
      ? Effect.void
      : Effect.yieldNow.pipe(Effect.andThen(waitUntil(predicate))),
  );

const waitUntilEffect = (predicate: Effect.Effect<boolean>): Effect.Effect<void> =>
  predicate.pipe(
    Effect.flatMap((done) =>
      done
        ? Effect.void
        : Effect.yieldNow.pipe(Effect.andThen(waitUntilEffect(predicate))),
    ),
  );

const runCall = (
  input: Queue.Queue<CallInput, Cause.Done>,
  harness: Harness,
  states: Array<SavedCallState>,
  outputQueueCapacity = 16,
): Effect.Effect<Array<CallOutput>> =>
  (coordinateCall(Stream.fromQueue(input), options(states, outputQueueCapacity)).pipe(
    Stream.provide(harness.layer),
  ) as Stream.Stream<CallOutput>).pipe(
    Stream.runCollect,
    Effect.map((events) => Array.from(events)),
  );

describe("call coordinator", () => {
  it.live.each([false, true])("cancelling collection retires a blocked flush (downstream callback: %s)", (downstream) =>
    Effect.gen(function* () {
      const flushing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const callback = yield* Deferred.make<void>();
      const harness = yield* makeHarness([Stream.make(
        { _tag: "TextDelta", text: "Complete." }, { _tag: "Completed" },
      )]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const transport = Layer.succeed(ElevenLabsTransport, ElevenLabsTransport.of({
        connect: () => Effect.succeed({
          receiveText: Effect.never,
          sendText: (text: string) => JSON.parse(text).flush
            ? Deferred.succeed(flushing, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void,
        }),
      }));
      const fiber = yield* coordinateCall(Stream.fromQueue(input), options([])).pipe(
        Stream.provide(Layer.merge(harness.layer, ElevenLabsTts.layer.pipe(Layer.provide(transport)))),
        (stream) => downstream
          ? stream.pipe(Stream.runForEach(() => Deferred.succeed(callback, undefined).pipe(Effect.andThen(Effect.never))))
          : stream.pipe(Stream.runDrain),
        Effect.forkChild,
      );
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "Question", 1));
      if (downstream) yield* Deferred.await(callback);
      yield* Deferred.await(flushing);
      const cancelling = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild);
      const result = yield* Fiber.join(cancelling).pipe(Effect.timeout("2 seconds"), Effect.result);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(cancelling);
      expect(result._tag).toBe("Success");
    }),
  );

  it.live("hangup after interrupt retires a real adapter's blocked context close", () =>
    Effect.gen(function* () {
      const closing = yield* Deferred.make<void>();
      const sockets: Array<Queue.Queue<string, ElevenLabsTransportError>> = [];
      let textSent = false;
      const transport = Layer.succeed(ElevenLabsTransport, ElevenLabsTransport.of({ connect: () => Effect.gen(function*() {
        const inbound = yield* Queue.unbounded<string, ElevenLabsTransportError>();
        sockets.push(inbound);
        return {
          receiveText: Queue.take(inbound),
          sendText: (text: string) => Effect.suspend(() => {
            const message = JSON.parse(text) as { text?: string; context_id?: string; close_context?: boolean };
            if (message.text === "Pending answer.") textSent = true;
            return message.close_context && message.context_id === "response-1"
              ? Deferred.succeed(closing, undefined).pipe(Effect.andThen(Effect.never)) : Effect.void;
          }),
        };
      }) }));
      const harness = yield* makeHarness([Stream.concat(Stream.make({ _tag: "TextDelta", text: "Pending answer." }), Stream.never)]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const realTts = ElevenLabsTts.layer.pipe(Layer.provide(transport));
      const fiber = yield* coordinateCall(Stream.fromQueue(input), options(states)).pipe(
        Stream.provide(Layer.merge(harness.layer, realTts)), Stream.runCollect, Effect.forkChild,
      );
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "Question", 1));
      yield* waitUntil(() => textSent);
      yield* Queue.offer(input, { _tag: "interrupt" });
      yield* Deferred.await(closing);
      yield* Queue.offer(input, { _tag: "hangup" });
      expect(Array.from(yield* Fiber.join(fiber))).toContainEqual({ _tag: "interruptConfirmed" });
      expect(states.at(-1)?.phase).toBe("ended");
    }).pipe(Effect.timeout("3 seconds")),
  );

  it.live("hangup disposes an acquired replacement before its ownership is published", () =>
    Effect.gen(function*() {
      const acquired = yield* Deferred.make<void>();
      const sockets: Array<Queue.Queue<string, ElevenLabsTransportError>> = [];
      const wire: Array<string> = [];
      const transport = Layer.succeed(ElevenLabsTransport, ElevenLabsTransport.of({
        connect: () => Effect.gen(function*() {
          const incoming = yield* Queue.unbounded<string, ElevenLabsTransportError>();
          sockets.push(incoming);
          return { receiveText: Queue.take(incoming), sendText: (text: string) => {
            wire.push(text);
            return JSON.parse(text).close_socket ? Effect.never : Effect.void;
          } };
        }),
      }));
      const gatedTts = Layer.effect(ElevenLabsTts, Effect.gen(function*() {
        const real = yield* ElevenLabsTts;
        return ElevenLabsTts.of({ open: Effect.fnUntraced(function*(key, settings) {
          const session = yield* real.open(key, settings);
          if (sockets.length === 2) {
            yield* Deferred.succeed(acquired, undefined);
            yield* Effect.never;
          }
          return session;
        }) });
      })).pipe(Layer.provide(ElevenLabsTts.layer.pipe(Layer.provide(transport))));
      const harness = yield* makeHarness([]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* coordinateCall(Stream.fromQueue(input), options(states)).pipe(
        Stream.provide(Layer.merge(harness.layer, gatedTts)), Stream.runCollect, Effect.forkChild,
      );
      yield* waitUntil(() => sockets.length === 1);
      yield* Queue.fail(sockets[0]!, new ElevenLabsTransportError({ operation: "receive", reason: "lost", sessionLost: true }));
      yield* Deferred.await(acquired);
      yield* Queue.offer(input, { _tag: "hangup" });
      yield* Fiber.join(fiber).pipe(Effect.timeout("2 seconds"));
      expect(states.at(-1)?.phase).toBe("ended");
      expect(wire).toEqual([]);
    }),
  );

  it.effect("keeps a complete answer through loss while sending it on the first replacement", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness([
        Stream.make({ _tag: "TextDelta", text: "Original complete answer." }, { _tag: "Completed" }),
        Stream.make({ _tag: "TextDelta", text: "Wrong regeneration." }, { _tag: "Completed" }),
      ], Effect.void, () => Effect.void, Effect.void, (id) => id === "response-2"
        ? Effect.fail(new ElevenLabsTransportError({ operation: "send", reason: "lost again", sessionLost: true })) : Effect.void);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "Question", 1));
      yield* waitUntil(() => harness.finishedContexts.length === 1);
      yield* Queue.fail(harness.ttsEvents, new ElevenLabsTransportError({ operation: "receive", reason: "lost", sessionLost: true }));
      yield* waitUntil(() => harness.finishedContexts.includes("response-3" as ResponseId));
      yield* Queue.offer(harness.ttsConnections[2]!, { _tag: "Audio", responseId: "response-3" as ResponseId, payload: new Uint8Array([3, 4]) });
      yield* Queue.offer(harness.ttsConnections[2]!, { _tag: "Final", responseId: "response-3" as ResponseId });
      yield* waitUntil(() => states.at(-1)?.responses[2]?.status === "finished");
      yield* Queue.offer(input, { _tag: "hangup" });
      const output = yield* Fiber.join(fiber);
      expect(output).toContainEqual({ _tag: "assistantText", responseId: "response-3", text: "Original complete answer." });
      expect(states.at(-1)?.turns.map((entry) => entry.text)).toEqual(["Question", "Original complete answer."]);
      expect(harness.ttsConnections).toHaveLength(3);
    }),
  );

  it.effect("does not strand reconstruction when interrupt cancels a send-loss reporter waiting for the state lock", () =>
    Effect.gen(function* () {
      const sending = yield* Deferred.make<void>();
      const loseSend = yield* Deferred.make<void>();
      const invalidating = yield* Deferred.make<void>();
      const releaseInvalidation = yield* Deferred.make<void>();
      const harness = yield* makeHarness([
        Stream.make({ _tag: "TextDelta", text: "Obsolete." }, { _tag: "Completed" }),
        Stream.make({ _tag: "TextDelta", text: "Next answer." }, { _tag: "Completed" }),
      ], Effect.void, () => Effect.void, Effect.void, (id) => id === "response-1"
        ? Deferred.succeed(sending, undefined).pipe(Effect.andThen(Deferred.await(loseSend)),
            Effect.andThen(Effect.fail(new ElevenLabsTransportError({ operation: "send", reason: "lost", sessionLost: true })))) : Effect.void);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      let gated = false;
      const configured = { ...options(states), onStateChange: (state: SavedCallState) => Effect.gen(function*() {
        states.push(state);
        if (!gated && state.responses[0]?.status === "interrupted") {
          gated = true;
          yield* Deferred.succeed(invalidating, undefined);
          yield* Deferred.await(releaseInvalidation);
        }
      }) };
      const fiber = yield* coordinateCall(Stream.fromQueue(input), configured).pipe(Stream.provide(harness.layer), Stream.runCollect, Effect.forkChild);
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "First question", 1));
      yield* Deferred.await(sending);
      yield* Queue.offer(input, { _tag: "interrupt" });
      yield* Deferred.await(invalidating);
      yield* Deferred.succeed(loseSend, undefined);
      for (let i = 0; i < 10; i++) yield* Effect.yieldNow;
      yield* Deferred.succeed(releaseInvalidation, undefined);
      yield* waitUntil(() => harness.ttsConnections.length === 2);
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "Next question", 2));
      yield* waitUntil(() => harness.finishedContexts.includes("response-2" as ResponseId));
      yield* Queue.offer(harness.ttsConnections[1]!, { _tag: "Audio", responseId: "response-2" as ResponseId, payload: new Uint8Array([3, 4]) });
      yield* Queue.offer(harness.ttsConnections[1]!, { _tag: "Final", responseId: "response-2" as ResponseId });
      yield* waitUntil(() => states.at(-1)?.responses[1]?.status === "finished");
      yield* Queue.offer(input, { _tag: "hangup" });
      expect(Array.from(yield* Fiber.join(fiber))).toContainEqual({ _tag: "assistantText", responseId: "response-2", text: "Next answer." });
    }),
  );

  it.effect("reuses completed answer text under a new response ID after synthesis session loss", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness([
        Stream.make({ _tag: "TextDelta", text: "The complete answer." }, { _tag: "Completed" }),
        Stream.make({ _tag: "TextDelta", text: "Wrong regenerated answer." }, { _tag: "Completed" }),
      ]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states, 1));
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "A question", 1));
      yield* waitUntil(() => harness.finishedContexts.length === 1);
      yield* Queue.offer(harness.ttsEvents, { _tag: "Audio", responseId: "response-1" as ResponseId, payload: new Uint8Array([1, 2]) });
      yield* waitUntil(() => states.at(-1)?.responses[0]?.emittedThrough === 0);
      yield* Queue.offer(input, { _tag: "playbackAck", responseId: "response-1" as ResponseId, playedThrough: 0 });
      yield* waitUntil(() => states.at(-1)?.responses[0]?.playedThrough === 0);
      yield* Queue.fail(harness.ttsEvents, new ElevenLabsTransportError({ operation: "receive", reason: "lost", sessionLost: true }));
      yield* waitUntil(() => harness.finishedContexts.length === 2);
      expect(harness.ttsConnections).toHaveLength(2);
      yield* Queue.offer(harness.ttsConnections[1]!, { _tag: "Audio", responseId: "response-1" as ResponseId, payload: new Uint8Array([9, 9]) });
      yield* Queue.offer(harness.ttsConnections[1]!, { _tag: "Audio", responseId: "response-2" as ResponseId, payload: new Uint8Array([3, 4]) });
      yield* Queue.offer(harness.ttsConnections[1]!, { _tag: "Final", responseId: "response-2" as ResponseId });
      yield* waitUntil(() => states.at(-1)?.responses[1]?.status === "finished");
      yield* Queue.offer(input, { _tag: "hangup" });
      const output = yield* Fiber.join(fiber);
      expect(output.filter((event) => event._tag === "assistantText")).toEqual([
        { _tag: "assistantText", responseId: "response-2", text: "The complete answer." },
      ]);
      expect(output.filter((event) => event._tag === "transcript" && event.final)).toHaveLength(1);
      const clearIndex = output.findIndex((event) => event._tag === "clearPlayback" && event.responseId === "response-1");
      const newAudioIndex = output.findIndex((event) => event._tag === "audio" && event.responseId === "response-2");
      expect(clearIndex).toBeGreaterThanOrEqual(0);
      expect(newAudioIndex).toBeGreaterThan(clearIndex);
      expect(output.filter((event) => event._tag === "audio")).toHaveLength(2);
      expect(states.at(-1)?.responses[0]).toMatchObject({ status: "interrupted", emittedThrough: 0, playedThrough: 0 });
      expect(states.at(-1)?.turns.map((entry) => entry.text)).toEqual(["A question", "The complete answer."]);
    }),
  );

  it.effect("regenerates incomplete text once after concurrent send/receive loss without splicing a suffix", () =>
    Effect.gen(function* () {
      let loseFirst = true;
      let inbound: Harness["ttsEvents"];
      const harness = yield* makeHarness([
        Stream.make({ _tag: "TextDelta", text: "Obsolete prefix" }, { _tag: "Completed" }),
        Stream.make({ _tag: "TextDelta", text: "Fresh full answer." }, { _tag: "Completed" }),
      ], Effect.void, () => Effect.void, Effect.void, () => {
        if (!loseFirst) return Effect.void;
        loseFirst = false;
        return Queue.fail(inbound, new ElevenLabsTransportError({ operation: "receive", reason: "lost", sessionLost: true })).pipe(
          Effect.andThen(Effect.yieldNow),
          Effect.andThen(Effect.fail(new ElevenLabsTransportError({ operation: "send", reason: "lost", sessionLost: true }))),
        );
      });
      inbound = harness.ttsEvents;
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "Question", 1));
      yield* waitUntil(() => harness.finishedContexts.includes("response-2" as ResponseId));
      yield* Queue.offer(harness.ttsConnections[1]!, { _tag: "Audio", responseId: "response-2" as ResponseId, payload: new Uint8Array([3, 4]) });
      yield* Queue.offer(harness.ttsConnections[1]!, { _tag: "Final", responseId: "response-2" as ResponseId });
      yield* waitUntil(() => states.at(-1)?.responses[1]?.status === "finished");
      yield* Queue.offer(input, { _tag: "hangup" });
      const output = yield* Fiber.join(fiber);
      expect(output).toContainEqual({ _tag: "assistantText", responseId: "response-2", text: "Fresh full answer." });
      expect(output.some((event) => event._tag === "failure")).toBe(false);
      expect(states.at(-1)?.nextTurnNumber).toBe(3);
      expect(harness.ttsConnections).toHaveLength(2);
    }),
  );

  it.effect("keeps finished durable audio intact when an idle synthesis socket is lost", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness([Stream.make({ _tag: "TextDelta", text: "Finished." }, { _tag: "Completed" })]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "Question", 1));
      yield* waitUntil(() => harness.finishedContexts.length === 1);
      yield* Queue.offer(harness.ttsEvents, { _tag: "Audio", responseId: "response-1" as ResponseId, payload: new Uint8Array([1, 2]) });
      yield* Queue.offer(harness.ttsEvents, { _tag: "Final", responseId: "response-1" as ResponseId });
      yield* waitUntil(() => states.at(-1)?.responses[0]?.status === "finished");
      yield* Queue.fail(harness.ttsEvents, new ElevenLabsTransportError({ operation: "receive", reason: "lost", sessionLost: true }));
      yield* waitUntil(() => harness.ttsConnections.length === 2);
      yield* Queue.offer(input, { _tag: "hangup" });
      const output = yield* Fiber.join(fiber);
      expect(output.filter((event) => event._tag === "responseFinished")).toEqual([{ _tag: "responseFinished", responseId: "response-1" }]);
      expect(states.at(-1)?.responses).toHaveLength(1);
      expect(states.at(-1)?.responses[0]?.status).toBe("finished");
      expect(states.at(-1)?.nextResponseNumber).toBe(2);
    }),
  );

  it.effect("terminates repeated synthesis reconstruction losses without an unbounded retry loop", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness(Array.from({ length: 5 }, () =>
        Stream.make({ _tag: "TextDelta" as const, text: "Answer." }, { _tag: "Completed" as const })),
        Effect.void, () => Effect.void, Effect.void,
        () => Effect.fail(new ElevenLabsTransportError({ operation: "send", reason: "lost", sessionLost: true })));
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "Question", 1));
      const output = yield* Fiber.join(fiber);
      expect(output).toContainEqual(expect.objectContaining({ _tag: "failure", stage: "elevenlabs", recoverable: false }));
      expect(harness.ttsConnections).toHaveLength(4);
      expect(states.at(-1)?.phase).toBe("ended");
      expect(states.at(-1)?.turns.map((entry) => entry.text)).toEqual(["Question"]);
    }),
  );

  it.effect("interrupt and hangup revoke a replacement while synthesis acquisition is blocked", () =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const harness = yield* makeHarness([
        Stream.make({ _tag: "TextDelta", text: "Complete." }, { _tag: "Completed" }),
      ], Effect.void, () => Effect.void, Effect.void, () => Effect.void, Deferred.await(gate));
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "Question", 1));
      yield* waitUntil(() => harness.finishedContexts.length === 1);
      yield* Queue.fail(harness.ttsEvents, new ElevenLabsTransportError({ operation: "receive", reason: "lost", sessionLost: true }));
      yield* waitUntil(() => harness.ttsConnections.length === 2);
      yield* Queue.offer(input, { _tag: "interrupt" });
      yield* Queue.offer(input, { _tag: "hangup" });
      const output = yield* Fiber.join(fiber);
      expect(output).toContainEqual({ _tag: "interruptConfirmed" });
      expect(harness.startedContexts).toEqual(["response-1"]);
      expect(states.at(-1)?.phase).toBe("ended");
    }),
  );

  it.effect("processes interrupt and hangup while recognition audio sending is blocked", () =>
    Effect.gen(function* () {
      const sending = yield* Deferred.make<void>();
      const harness = yield* makeHarness([], Effect.void, () => Effect.void,
        Deferred.succeed(sending, undefined).pipe(Effect.andThen(Effect.never)));
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));
      yield* Queue.offer(input, { _tag: "audio", sequence: 0, capturedAt: 0, payload: new Uint8Array([1, 2]) });
      yield* Deferred.await(sending);
      yield* Queue.offer(input, { _tag: "interrupt" });
      yield* Queue.offer(input, { _tag: "hangup" });
      expect(yield* Fiber.join(fiber)).toContainEqual({ _tag: "interruptConfirmed" });
      expect(states.at(-1)?.phase).toBe("ended");
      expect(yield* Ref.get(harness.deepgramClosed)).toBe(1);
    }),
  );

  it.effect("streams a committed turn through generation and synthesized audio", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness([
        Stream.make(
          { _tag: "TextDelta", text: "Hello" },
          { _tag: "TextDelta", text: " there" },
          { _tag: "Completed" },
        ),
      ]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));

      yield* Queue.offer(harness.fluxEvents, turn("Update", "Hi", 1));
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "Hi there", 2));
      yield* waitUntil(() => harness.finishedContexts.length === 1);
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Audio",
        responseId: "response-1" as ResponseId,
        payload: new Uint8Array([1, 2]),
      });
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Final",
        responseId: "response-1" as ResponseId,
      });
      yield* waitUntil(() =>
        states.some((state) => state.responses[0]?.status === "finished"),
      );
      yield* Queue.offer(input, {
        _tag: "playbackAck",
        responseId: "response-1" as ResponseId,
        playedThrough: 0,
      });
      yield* waitUntil(() => states.at(-1)?.responses[0]?.playedThrough === 0);
      yield* Queue.offer(input, { _tag: "hangup" });
      const output = yield* Fiber.join(fiber);

      expect(output).toEqual([
        { _tag: "transcript", turnId: "turn-1", text: "Hi", final: false },
        { _tag: "transcript", turnId: "turn-1", text: "Hi there", final: true },
        {
          _tag: "audio",
          responseId: "response-1",
          sequence: 0,
          payload: new Uint8Array([1, 2]),
        },
        { _tag: "assistantText", responseId: "response-1", text: "Hello there" },
        { _tag: "responseFinished", responseId: "response-1" },
      ]);
      expect(harness.text).toEqual([
        { responseId: "response-1", text: "Hello" },
        { responseId: "response-1", text: " there" },
      ]);
      expect(states.at(-2)?.turns.at(-1)).toMatchObject({
        role: "assistant",
        text: "Hello there",
      });
      expect(states.at(-1)?.phase).toBe("ended");
    }),
  );

  for (const emptyFrame of [false, true]) {
    it.effect(`reports empty synthesis (empty frame: ${emptyFrame}) without committing an unspoken turn`, () =>
      Effect.gen(function* () {
        const harness = yield* makeHarness([
          Stream.make({ _tag: "TextDelta", text: "Unspoken answer" }, { _tag: "Completed" }),
          Stream.make({ _tag: "TextDelta", text: "Spoken answer" }, { _tag: "Completed" }),
        ]);
        const input = yield* Queue.unbounded<CallInput, Cause.Done>();
        const states: Array<SavedCallState> = [];
        const fiber = yield* Effect.forkChild(runCall(input, harness, states));
        yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "First question", 1));
        yield* waitUntil(() => harness.finishedContexts.length === 1);
        if (emptyFrame) {
          yield* Queue.offer(harness.ttsEvents, { _tag: "Audio", responseId: "response-1" as ResponseId, payload: new Uint8Array() });
        }
        yield* Queue.offer(harness.ttsEvents, { _tag: "Final", responseId: "response-1" as ResponseId });
        yield* waitUntil(() => states.at(-1)?.responses[0]?.status === "interrupted");
        expect(states.at(-1)?.phase).toBe("listening");
        yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "Second question", 2));
        yield* waitUntil(() => harness.finishedContexts.length === 2);
        yield* Queue.offer(harness.ttsEvents, { _tag: "Audio", responseId: "response-2" as ResponseId, payload: new Uint8Array([4, 9]) });
        yield* Queue.offer(harness.ttsEvents, { _tag: "Final", responseId: "response-2" as ResponseId });
        yield* waitUntil(() => states.at(-1)?.responses[1]?.status === "finished");
        yield* Queue.offer(input, { _tag: "hangup" });
        const output = yield* Fiber.join(fiber);
        expect(output).toContainEqual({ _tag: "failure", stage: "elevenlabs", reason: "Speech synthesis returned no audio; check voice access and provider account limits", recoverable: true });
        expect(output).not.toContainEqual({ _tag: "responseFinished", responseId: "response-1" });
        expect(output).not.toContainEqual({ _tag: "assistantText", responseId: "response-1", text: "Unspoken answer" });
        expect(output).toContainEqual({ _tag: "responseFinished", responseId: "response-2" });
        expect(states.at(-1)?.turns.filter((entry) => entry.role === "assistant").map((entry) => entry.text)).toEqual(["Spoken answer"]);
      }),
    );
  }

  it.effect("rejects premature synthesis final and cancels generation while the reader is blocked", () =>
    Effect.gen(function* () {
      const releaseGeneration = yield* Deferred.make<void>();
      const generationCancelled = yield* Deferred.make<void>();
      const releaseReader = yield* Deferred.make<void>();
      const readerBlocked = yield* Deferred.make<void>();
      const first = Stream.make({ _tag: "TextDelta", text: "Unfinished answer" } as ResponseGenerationEvent).pipe(
        Stream.concat(Stream.fromEffect(Deferred.await(releaseGeneration).pipe(
          Effect.as({ _tag: "Completed" } as ResponseGenerationEvent),
        ))),
        Stream.ensuring(Deferred.succeed(generationCancelled, undefined)),
      );
      const harness = yield* makeHarness([first]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const output: Array<CallOutput> = [];
      const fiber = yield* coordinateCall(Stream.fromQueue(input), options(states, 1)).pipe(
        Stream.provide(harness.layer),
        Stream.runForEach((event) => Effect.gen(function* () {
          output.push(event);
          yield* Deferred.succeed(readerBlocked, undefined);
          yield* Deferred.await(releaseReader);
        })),
        Effect.forkChild,
      );
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "Question", 1));
      yield* Deferred.await(readerBlocked);
      yield* waitUntil(() => harness.text.length === 1);
      yield* Queue.offer(harness.ttsEvents, { _tag: "Final", responseId: "response-1" as ResponseId });
      yield* Deferred.await(generationCancelled);
      expect(states.at(-1)?.responses[0]?.status).toBe("interrupted");
      yield* Deferred.succeed(releaseGeneration, undefined);
      yield* Deferred.succeed(releaseReader, undefined);
      yield* waitUntil(() => output.some((event) => event._tag === "failure"));
      yield* Queue.offer(input, { _tag: "hangup" });
      yield* Fiber.join(fiber);
      expect(harness.finishedContexts).toEqual([]);
      expect(output).toContainEqual({ _tag: "failure", stage: "elevenlabs", reason: "Speech synthesis ended before text generation completed", recoverable: true });
      expect(output.some((event) => event._tag === "assistantText" || event._tag === "responseFinished")).toBe(false);
      expect(states.at(-1)?.turns.filter((entry) => entry.role === "assistant")).toEqual([]);
    }),
  );

  it.effect("invalidates overlapping speech and drops late audio from the old response", () =>
    Effect.gen(function* () {
      const firstStarted = yield* Deferred.make<void>();
      const first: Stream.Stream<ResponseGenerationEvent, AiError.AiError> = Stream.make({
        _tag: "TextDelta",
        text: "old",
      }).pipe(
        Stream.tap(() => Deferred.succeed(firstStarted, undefined)),
        Stream.concat(Stream.never),
      );
      const second: Stream.Stream<ResponseGenerationEvent, AiError.AiError> = Stream.make(
        { _tag: "TextDelta", text: "new" },
        { _tag: "Completed" },
      );
      const harness = yield* makeHarness([first, second]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));

      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "first", 1));
      yield* Deferred.await(firstStarted);
      yield* Queue.offer(harness.fluxEvents, turn("StartOfTurn", "second", 2));
      yield* waitUntil(() =>
        harness.closedContexts.includes("response-1" as ResponseId),
      );
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Audio",
        responseId: "response-1" as ResponseId,
        payload: new Uint8Array([9]),
      });
      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "second", 3));
      yield* waitUntil(() =>
        harness.finishedContexts.includes("response-2" as ResponseId),
      );
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Audio",
        responseId: "response-2" as ResponseId,
        payload: new Uint8Array([2]),
      });
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Final",
        responseId: "response-2" as ResponseId,
      });
      yield* waitUntil(() =>
        states.some((state) => state.responses[1]?.status === "finished"),
      );
      yield* Queue.offer(input, { _tag: "hangup" });
      const output = yield* Fiber.join(fiber);

      expect(output).toContainEqual({
        _tag: "clearPlayback",
        responseId: "response-1",
      });
      expect(
        output.filter((event) => event._tag === "audio"),
      ).toEqual([
        {
          _tag: "audio",
          responseId: "response-2",
          sequence: 0,
          payload: new Uint8Array([2]),
        },
      ]);
      expect(states.find((state) => state.responses[0]?.status === "interrupted")).toBeDefined();
    }),
  );

  it.effect("uses the same invalidation path for an explicit interrupt", () =>
    Effect.gen(function* () {
      const generationStarted = yield* Deferred.make<void>();
      const finalized = yield* Deferred.make<void>();
      const response = Stream.unwrap(
        Deferred.succeed(generationStarted, undefined).pipe(Effect.as(Stream.never)),
      ).pipe(Stream.ensuring(Deferred.succeed(finalized, undefined)));
      const harness = yield* makeHarness([response]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));

      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "question", 1));
      yield* Deferred.await(generationStarted);
      yield* Queue.offer(input, { _tag: "interrupt" });
      yield* Deferred.await(finalized);
      yield* Queue.offer(input, { _tag: "hangup" });
      const output = yield* Fiber.join(fiber);

      expect(output).toContainEqual({
        _tag: "clearPlayback",
        responseId: "response-1",
      });
      expect(harness.closedContexts).toContain("response-1");
      expect(states.some((state) => state.responses[0]?.status === "interrupted")).toBe(true);
    }),
  );

  it.effect("maps provider failure safely and keeps the call available for another turn", () =>
    Effect.gen(function* () {
      const providerFailure = AiError.make({
        module: "FakeProvider",
        method: "stream",
        reason: new AiError.UnknownError({ description: "secret-provider-detail" }),
      });
      const harness = yield* makeHarness([Stream.fail(providerFailure)]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));

      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "question", 1));
      yield* waitUntil(() =>
        states.some((state) => state.responses[0]?.status === "interrupted"),
      );
      yield* Queue.offer(input, { _tag: "hangup" });
      const output = yield* Fiber.join(fiber);
      const failure = output.find((event) => event._tag === "failure");

      expect(failure).toEqual({
        _tag: "failure",
        stage: "openai",
        reason: "Language model generation failed",
        recoverable: true,
      });
      expect(JSON.stringify(output)).not.toContain("secret-provider-detail");
      expect(states.at(-2)?.phase).toBe("listening");
    }),
  );

  it.effect("applies bounded backpressure when the output reader is slow", () =>
    Effect.gen(function* () {
      const secondReceive = yield* Deferred.make<void>();
      const receiveCount = yield* Ref.make(0);
      const harness = yield* makeHarness(
        [
          Stream.make(
            { _tag: "TextDelta", text: "answer" },
            { _tag: "Completed" },
          ) as Stream.Stream<ResponseGenerationEvent, AiError.AiError>,
        ],
        Ref.updateAndGet(receiveCount, (count) => count + 1).pipe(
          Effect.tap((count) =>
            count === 2 ? Deferred.succeed(secondReceive, undefined) : Effect.void,
          ),
          Effect.asVoid,
        ),
      );
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const releaseReader = yield* Deferred.make<void>();
      const firstOutput = yield* Deferred.make<void>();
      const fiber = yield* coordinateCall(Stream.fromQueue(input), options(states, 1)).pipe(
        Stream.provide(harness.layer),
        Stream.runForEach(() =>
          Deferred.succeed(firstOutput, undefined).pipe(
            Effect.andThen(Deferred.await(releaseReader)),
          ),
        ),
        Effect.forkChild,
      );

      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "question", 1));
      yield* Deferred.await(firstOutput);
      yield* waitUntil(() => harness.finishedContexts.length === 1);
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Audio",
        responseId: "response-1" as ResponseId,
        payload: new Uint8Array([1]),
      });
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Audio",
        responseId: "response-1" as ResponseId,
        payload: new Uint8Array([2]),
      });
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Audio",
        responseId: "response-1" as ResponseId,
        payload: new Uint8Array([3]),
      });
      yield* Deferred.await(secondReceive);

      expect(yield* Ref.get(receiveCount)).toBe(2);

      yield* Deferred.succeed(releaseReader, undefined);
      yield* Queue.offer(input, { _tag: "hangup" });
      yield* Fiber.join(fiber);
    }),
  );

  it.effect("cancels active work before publishing hangup controls to a slow reader", () =>
    Effect.gen(function* () {
      const generationStarted = yield* Deferred.make<void>();
      const generationFinalized = yield* Deferred.make<void>();
      const secondReceive = yield* Deferred.make<void>();
      const receiveCount = yield* Ref.make(0);
      const response: Stream.Stream<ResponseGenerationEvent, AiError.AiError> = Stream.make({
        _tag: "TextDelta",
        text: "partial",
      }).pipe(
        Stream.tap(() => Deferred.succeed(generationStarted, undefined)),
        Stream.concat(Stream.never),
        Stream.ensuring(Deferred.succeed(generationFinalized, undefined)),
      );
      const harness = yield* makeHarness(
        [response],
        Ref.updateAndGet(receiveCount, (count) => count + 1).pipe(
          Effect.tap((count) =>
            count === 2 ? Deferred.succeed(secondReceive, undefined) : Effect.void,
          ),
          Effect.asVoid,
        ),
      );
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const seen: Array<CallOutput> = [];
      const readerBlocked = yield* Deferred.make<void>();
      const releaseReader = yield* Deferred.make<void>();
      let first = true;
      const fiber = yield* (coordinateCall(
        Stream.fromQueue(input),
        options(states, 1),
      ).pipe(Stream.provide(harness.layer)) as Stream.Stream<CallOutput>).pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            seen.push(event);
            if (!first) return;
            first = false;
            yield* Deferred.succeed(readerBlocked, undefined);
            yield* Deferred.await(releaseReader);
          }),
        ),
        Effect.forkChild,
      );

      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "question", 1));
      yield* Deferred.await(readerBlocked);
      yield* Deferred.await(generationStarted);
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Audio",
        responseId: "response-1" as ResponseId,
        payload: new Uint8Array([1]),
      });
      yield* Deferred.await(secondReceive);
      yield* Queue.offer(input, { _tag: "hangup" });

      yield* Deferred.await(generationFinalized);
      yield* waitUntil(() => states.at(-1)?.phase === "ended");
      yield* waitUntilEffect(
        Ref.get(harness.deepgramClosed).pipe(Effect.map((count) => count === 1)),
      );
      yield* waitUntilEffect(
        Ref.get(harness.ttsClosed).pipe(Effect.map((count) => count === 1)),
      );

      yield* Deferred.succeed(releaseReader, undefined);
      yield* Fiber.join(fiber);
      expect(seen).toContainEqual({
        _tag: "clearPlayback",
        responseId: "response-1",
      });
      expect(seen.some((event) => event._tag === "failure")).toBe(false);
    }),
  );

  it.effect("processes hangup while an interrupt control waits for a slow reader", () =>
    Effect.gen(function* () {
      const generationStarted = yield* Deferred.make<void>();
      const generationFinalized = yield* Deferred.make<void>();
      const secondReceive = yield* Deferred.make<void>();
      const receiveCount = yield* Ref.make(0);
      const response: Stream.Stream<ResponseGenerationEvent, AiError.AiError> = Stream.make({
        _tag: "TextDelta",
        text: "partial",
      }).pipe(
        Stream.tap(() => Deferred.succeed(generationStarted, undefined)),
        Stream.concat(Stream.never),
        Stream.ensuring(Deferred.succeed(generationFinalized, undefined)),
      );
      const harness = yield* makeHarness(
        [response],
        Ref.updateAndGet(receiveCount, (count) => count + 1).pipe(
          Effect.tap((count) =>
            count === 2 ? Deferred.succeed(secondReceive, undefined) : Effect.void,
          ),
          Effect.asVoid,
        ),
      );
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const seen: Array<CallOutput> = [];
      const readerBlocked = yield* Deferred.make<void>();
      const releaseReader = yield* Deferred.make<void>();
      let first = true;
      const fiber = yield* (coordinateCall(
        Stream.fromQueue(input),
        options(states, 1),
      ).pipe(Stream.provide(harness.layer)) as Stream.Stream<CallOutput>).pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            seen.push(event);
            if (!first) return;
            first = false;
            yield* Deferred.succeed(readerBlocked, undefined);
            yield* Deferred.await(releaseReader);
          }),
        ),
        Effect.forkChild,
      );

      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "question", 1));
      yield* Deferred.await(readerBlocked);
      yield* Deferred.await(generationStarted);
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Audio",
        responseId: "response-1" as ResponseId,
        payload: new Uint8Array([1]),
      });
      yield* Deferred.await(secondReceive);
      yield* Queue.offer(input, { _tag: "interrupt" });
      yield* Deferred.await(generationFinalized);
      yield* waitUntil(() =>
        harness.closedContexts.includes("response-1" as ResponseId),
      );
      yield* Queue.offer(input, { _tag: "hangup" });

      yield* waitUntil(() => states.at(-1)?.phase === "ended");
      yield* waitUntilEffect(
        Ref.get(harness.deepgramClosed).pipe(Effect.map((count) => count === 1)),
      );
      yield* waitUntilEffect(
        Ref.get(harness.ttsClosed).pipe(Effect.map((count) => count === 1)),
      );

      yield* Deferred.succeed(releaseReader, undefined);
      yield* Fiber.join(fiber);
      expect(seen).toContainEqual({
        _tag: "clearPlayback",
        responseId: "response-1",
      });
      expect(seen.some((event) => event._tag === "failure")).toBe(false);
    }),
  );

  it.effect.each(["interrupt", "invalidAck", "hangup"] as const)(
    "clears a recovered playback backlog without blocking hangup after %s",
    (scenario) =>
    Effect.gen(function* () {
      const outputFilled = yield* Deferred.make<void>();
      const harness = yield* makeHarness([], Effect.void, (event) =>
        event._tag === "Connected"
          ? Deferred.succeed(outputFilled, undefined).pipe(Effect.asVoid)
          : Effect.void,
      );
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const seen: Array<CallOutput> = [];
      const readerBlocked = yield* Deferred.make<void>();
      const releaseReader = yield* Deferred.make<void>();
      const persisted: SavedCallState = {
        ...initialCallState(),
        phase: "listening",
        activeResponseId: null,
        nextResponseNumber: 4,
        responses: [1, 2, 3].map((number) => ({
          responseId: `response-${number}` as ResponseId,
          userTurnId: `turn-${number}` as SavedCallState["turns"][number]["turnId"],
          status: "interrupted",
          emittedThrough: 0,
          playedThrough: null,
        })),
      };
      let first = true;
      const fiber = yield* (coordinateCall(Stream.fromQueue(input), {
        ...options(states, 1),
        initialState: persisted,
      }).pipe(Stream.provide(harness.layer)) as Stream.Stream<CallOutput>).pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            seen.push(event);
            if (!first) return;
            first = false;
            yield* Deferred.succeed(readerBlocked, undefined);
            yield* Deferred.await(releaseReader);
          }),
        ),
        Effect.forkChild,
      );
      yield* Queue.offer(harness.fluxEvents, turn("Update", "first", 1));
      yield* Deferred.await(readerBlocked);
      yield* Queue.offer(harness.fluxEvents, turn("Update", "second", 2));
      yield* Queue.offer(harness.fluxEvents, {
        _tag: "Connected", requestId: "request", sequence: 3,
      });
      yield* Deferred.await(outputFilled);
      for (let index = 0; index < 3; index++) {
        if (scenario === "interrupt") yield* Queue.offer(input, { _tag: "interrupt" });
        if (scenario === "invalidAck") {
          yield* Queue.offer(input, {
            _tag: "playbackAck",
            responseId: "response-1" as ResponseId,
            playedThrough: 1,
          });
        }
      }
      yield* Queue.offer(input, { _tag: "hangup" });
      yield* waitUntil(() => states.at(-1)?.phase === "ended");
      yield* waitUntilEffect(
        Ref.get(harness.ttsClosed).pipe(Effect.map((count) => count === 1)),
      );
      expect(yield* Ref.get(harness.deepgramClosed)).toBe(1);
      yield* Deferred.succeed(releaseReader, undefined);
      yield* Fiber.join(fiber);
      expect(seen.filter((event) => event._tag === "clearPlayback")).toEqual([
        { _tag: "clearPlayback", responseId: "response-1" },
        { _tag: "clearPlayback", responseId: "response-2" },
        { _tag: "clearPlayback", responseId: "response-3" },
      ]);
      expect(seen.filter((event) => event._tag === "interruptConfirmed").length)
        .toBe(scenario === "interrupt" ? 1 : 0);
    }),
  );

  it.effect("preserves a detected generation failure across concurrent hangup", () =>
    Effect.gen(function* () {
      const failGeneration = yield* Deferred.make<void>();
      const secondReceive = yield* Deferred.make<void>();
      const receiveCount = yield* Ref.make(0);
      const providerFailure = AiError.make({
        module: "FakeProvider",
        method: "stream",
        reason: new AiError.UnknownError({ description: "private-detail" }),
      });
      const response = Stream.make({
        _tag: "TextDelta",
        text: "partial",
      } satisfies ResponseGenerationEvent).pipe(
        Stream.concat(
          Stream.unwrap(
            Deferred.await(failGeneration).pipe(
              Effect.as(Stream.fail(providerFailure)),
            ),
          ),
        ),
      );
      const harness = yield* makeHarness(
        [response],
        Ref.updateAndGet(receiveCount, (count) => count + 1).pipe(
          Effect.tap((count) =>
            count === 2 ? Deferred.succeed(secondReceive, undefined) : Effect.void,
          ),
          Effect.asVoid,
        ),
      );
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const seen: Array<CallOutput> = [];
      const readerBlocked = yield* Deferred.make<void>();
      const releaseReader = yield* Deferred.make<void>();
      let first = true;
      const fiber = yield* (coordinateCall(
        Stream.fromQueue(input),
        options(states, 1),
      ).pipe(Stream.provide(harness.layer)) as Stream.Stream<CallOutput>).pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            seen.push(event);
            if (!first) return;
            first = false;
            yield* Deferred.succeed(readerBlocked, undefined);
            yield* Deferred.await(releaseReader);
          }),
        ),
        Effect.forkChild,
      );

      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "question", 1));
      yield* Deferred.await(readerBlocked);
      yield* waitUntil(() => harness.text.length === 1);
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Audio",
        responseId: "response-1" as ResponseId,
        payload: new Uint8Array([1]),
      });
      yield* Deferred.await(secondReceive);
      yield* Deferred.succeed(failGeneration, undefined);
      yield* waitUntil(() =>
        states.some((state) => state.responses[0]?.status === "interrupted"),
      );
      yield* Queue.offer(input, { _tag: "hangup" });
      yield* waitUntilEffect(
        Ref.get(harness.deepgramClosed).pipe(Effect.map((count) => count === 1)),
      );

      yield* Deferred.succeed(releaseReader, undefined);
      yield* Fiber.join(fiber);

      expect(seen).toContainEqual({
        _tag: "clearPlayback",
        responseId: "response-1",
      });
      expect(seen).toContainEqual({
        _tag: "failure",
        stage: "openai",
        reason: "Language model generation failed",
        recoverable: true,
      });
      expect(JSON.stringify(seen)).not.toContain("private-detail");
    }),
  );

  it.effect("suppresses close-induced receive errors but reports real receive failure", () =>
    Effect.gen(function* () {
      const normalHarness = yield* makeHarness([]);
      const normalInput = yield* Queue.unbounded<CallInput, Cause.Done>();
      const normalStates: Array<SavedCallState> = [];
      const normalFiber = yield* Effect.forkChild(
        runCall(normalInput, normalHarness, normalStates),
      );
      yield* Queue.offer(normalInput, { _tag: "hangup" });
      const normalOutput = yield* Fiber.join(normalFiber);
      expect(normalOutput.some((event) => event._tag === "failure")).toBe(false);

      const failedHarness = yield* makeHarness([]);
      const failedInput = yield* Queue.unbounded<CallInput, Cause.Done>();
      const failedStates: Array<SavedCallState> = [];
      const failedFiber = yield* Effect.forkChild(
        runCall(failedInput, failedHarness, failedStates),
      );
      yield* Queue.fail(
        failedHarness.fluxEvents,
        new DeepgramTransportError({
          operation: "receive",
          reason: "provider disconnected",
        }),
      );
      const failedOutput = yield* Fiber.join(failedFiber);
      expect(failedOutput).toContainEqual({
        _tag: "failure",
        stage: "deepgram",
        reason: "Receiving Deepgram events failed",
        recoverable: false,
      });
    }),
  );

  it.effect("ignores a provider event received before shutdown completes", () =>
    Effect.gen(function* () {
      const eventReceived = yield* Deferred.make<void>();
      const releaseEvent = yield* Deferred.make<void>();
      const harness = yield* makeHarness(
        [],
        Effect.void,
        () =>
          Deferred.succeed(eventReceived, undefined).pipe(
            Effect.andThen(Deferred.await(releaseEvent)),
          ),
      );
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));

      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "too late", 1));
      yield* Deferred.await(eventReceived);
      yield* Queue.offer(input, { _tag: "hangup" });
      yield* waitUntil(() => states.at(-1)?.phase === "ended");
      yield* waitUntilEffect(
        Ref.get(harness.deepgramClosed).pipe(Effect.map((count) => count === 1)),
      );
      yield* Deferred.succeed(releaseEvent, undefined);
      const output = yield* Fiber.join(fiber);

      expect(output).toEqual([]);
      expect(states.some((state) => state.turns.length > 0)).toBe(false);
    }),
  );

  it.effect("emits queued audio before finishing a response for a slow reader", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness([
        Stream.make(
          { _tag: "TextDelta", text: "answer" },
          { _tag: "Completed" },
        ),
      ]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const seen: Array<CallOutput> = [];
      const readerBlocked = yield* Deferred.make<void>();
      const releaseReader = yield* Deferred.make<void>();
      let first = true;
      const fiber = yield* (coordinateCall(
        Stream.fromQueue(input),
        options(states, 1),
      ).pipe(Stream.provide(harness.layer)) as Stream.Stream<CallOutput>).pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            seen.push(event);
            if (!first) return;
            first = false;
            yield* Deferred.succeed(readerBlocked, undefined);
            yield* Deferred.await(releaseReader);
          }),
        ),
        Effect.forkChild,
      );

      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "question", 1));
      yield* Deferred.await(readerBlocked);
      yield* waitUntil(() => harness.finishedContexts.length === 1);
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Audio",
        responseId: "response-1" as ResponseId,
        payload: new Uint8Array([7]),
      });
      yield* Queue.offer(harness.ttsEvents, {
        _tag: "Final",
        responseId: "response-1" as ResponseId,
      });
      yield* Deferred.succeed(releaseReader, undefined);
      yield* waitUntil(() =>
        states.some((state) => state.responses[0]?.status === "finished"),
      );
      yield* Queue.offer(input, { _tag: "interrupt" });
      yield* waitUntil(() => seen.some((event) => event._tag === "clearPlayback"));
      yield* Queue.offer(input, { _tag: "hangup" });
      yield* Fiber.join(fiber);

      expect(seen.slice(-5)).toEqual([
        {
          _tag: "audio",
          responseId: "response-1",
          sequence: 0,
          payload: new Uint8Array([7]),
        },
        { _tag: "assistantText", responseId: "response-1", text: "answer" },
        { _tag: "responseFinished", responseId: "response-1" },
        { _tag: "clearPlayback", responseId: "response-1" },
        { _tag: "interruptConfirmed" },
      ]);
    }),
  );

  it.live.each([false, true])("hangup retires a blocked synthesis flush (preceding interrupt: %s)", (interruptFirst) =>
    Effect.gen(function* () {
      const flushing = yield* Deferred.make<void>();
      const completed = yield* Deferred.make<void>();
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const flux = yield* Queue.unbounded<FluxEvent>();
      const wire = yield* Queue.unbounded<string>();
      const transport = Layer.succeed(ElevenLabsTransport, ElevenLabsTransport.of({
        connect: () => Effect.succeed({
          receiveText: Queue.take(wire),
          sendText: (text: string) => JSON.parse(text).flush === true
            ? Deferred.succeed(flushing, undefined).pipe(Effect.andThen(Effect.never))
            : Effect.void,
        }),
      }));
      const providers = Layer.mergeAll(
        ElevenLabsTts.layer.pipe(Layer.provide(transport)),
        Layer.succeed(DeepgramFlux, DeepgramFlux.of({ open: () => Effect.succeed({
          receive: Queue.take(flux), commitTurn: (_event, accept) => accept,
          sendAudio: () => Effect.void, forceEndTurn: Effect.void,
          configure: () => Effect.void, closeStream: Effect.void,
        }) })),
        Layer.succeed(ResponseGenerator, ResponseGenerator.of({ stream: () => Stream.make(
          { _tag: "TextDelta" as const, text: "Complete answer." }, { _tag: "Completed" as const },
        ) })),
      );
      const fiber = yield* coordinateCall(Stream.fromQueue(input), {
        initialState: initialCallState(), deepgramApiKey: Redacted.make("dummy"),
        elevenLabsApiKey: Redacted.make("dummy"), systemPrompt: "test",
        elevenLabs: { voiceId: "voice", modelId: "model", outputFormat: "pcm_16000" },
      }).pipe(Stream.provide(providers), Stream.runCollect,
        Effect.tap(() => Deferred.succeed(completed, undefined)), Effect.forkChild);
      yield* Queue.offer(flux, { _tag: "Turn", kind: "EndOfTurn", requestId: "request",
        sequence: 1, turnIndex: 1, transcript: "Question", endOfTurnConfidence: 0.9 });
      yield* Deferred.await(flushing);
      if (interruptFirst) yield* Queue.offer(input, { _tag: "interrupt" });
      yield* Queue.offer(input, { _tag: "hangup" });
      yield* Deferred.await(completed).pipe(Effect.timeout("2 seconds"));
      const output = yield* Fiber.join(fiber);
      expect(Array.from(output)).toContainEqual({ _tag: "clearPlayback", responseId: "response-1" });
    }),
  );

  it.effect("hangup interrupts active work, closes providers, and persists ended state", () =>
    Effect.gen(function* () {
      const generationStarted = yield* Deferred.make<void>();
      const finalized = yield* Deferred.make<void>();
      const response = Stream.unwrap(
        Deferred.succeed(generationStarted, undefined).pipe(Effect.as(Stream.never)),
      ).pipe(Stream.ensuring(Deferred.succeed(finalized, undefined)));
      const harness = yield* makeHarness([response]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const fiber = yield* Effect.forkChild(runCall(input, harness, states));

      yield* Queue.offer(harness.fluxEvents, turn("EndOfTurn", "bye", 1));
      yield* Deferred.await(generationStarted);
      yield* Queue.offer(input, { _tag: "hangup" });
      yield* Fiber.join(fiber);

      expect(yield* Deferred.isDone(finalized)).toBe(true);
      expect(yield* Ref.get(harness.deepgramClosed)).toBe(1);
      expect(yield* Ref.get(harness.ttsClosed)).toBe(1);
      expect(harness.closedContexts).toContain("response-1");
      expect(states.at(-1)?.phase).toBe("ended");
    }),
  );

  it.effect("abandons a persisted unfinished response when the coordinator restarts", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness([]);
      const input = yield* Queue.unbounded<CallInput, Cause.Done>();
      const states: Array<SavedCallState> = [];
      const persisted: SavedCallState = {
        phase: "responding",
        turns: [
          {
            turnId: "turn-1" as SavedCallState["turns"][number]["turnId"],
            role: "user",
            text: "unfinished question",
            responseId: null,
          },
        ],
        responses: [
          {
            responseId: "response-1" as ResponseId,
            userTurnId: "turn-1" as SavedCallState["turns"][number]["turnId"],
            status: "active",
            emittedThrough: null,
            playedThrough: null,
          },
        ],
        activeResponseId: "response-1" as ResponseId,
        nextTurnNumber: 2,
        nextResponseNumber: 2,
      };
      const fiber = yield* coordinateCall(Stream.fromQueue(input), {
        ...options(states),
        initialState: persisted,
      }).pipe(
        Stream.provide(harness.layer),
        Stream.runDrain,
        Effect.forkChild,
      );

      // Let startup finish and persist its recovery transition.
      for (let index = 0; index < 10; index++) yield* Effect.yieldNow;

      expect(states).toContainEqual({
        ...persisted,
        phase: "listening",
        responses: [{ ...persisted.responses[0], status: "interrupted" }],
        activeResponseId: null,
      });

      yield* Queue.offer(input, { _tag: "hangup" });
      yield* Fiber.join(fiber);
    }),
  );

  it.effect("abandons a persisted unfinished response even when provider startup fails", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness([]);
      const states: Array<SavedCallState> = [];
      const persisted: SavedCallState = {
        phase: "responding",
        turns: [
          {
            turnId: "turn-1" as SavedCallState["turns"][number]["turnId"],
            role: "user",
            text: "unfinished question",
            responseId: null,
          },
        ],
        responses: [
          {
            responseId: "response-1" as ResponseId,
            userTurnId: "turn-1" as SavedCallState["turns"][number]["turnId"],
            status: "active",
            emittedThrough: null,
            playedThrough: null,
          },
        ],
        activeResponseId: "response-1" as ResponseId,
        nextTurnNumber: 2,
        nextResponseNumber: 2,
      };
      const failedDeepgram = Layer.succeed(
        DeepgramFlux,
        DeepgramFlux.of({
          open: () =>
            Effect.fail(
              new DeepgramTransportError({
                operation: "connect",
                reason: "unavailable",
              }),
            ),
        }),
      );

      const output = yield* coordinateCall(Stream.empty, {
        ...options(states),
        initialState: persisted,
      }).pipe(
        Stream.provide(Layer.merge(harness.layer, failedDeepgram)),
        Stream.runCollect,
      );

      expect(Array.from(output)).toContainEqual({
        _tag: "failure",
        stage: "deepgram",
        reason: "Deepgram connection failed",
        recoverable: false,
      });
      expect(states).toContainEqual({
        ...persisted,
        phase: "listening",
        responses: [{ ...persisted.responses[0], status: "interrupted" }],
        activeResponseId: null,
      });
    }),
  );

  it.effect("closes Deepgram when ElevenLabs startup fails", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness([]);
      const states: Array<SavedCallState> = [];
      const failedTts = Layer.succeed(
        ElevenLabsTts,
        ElevenLabsTts.of({
          open: () =>
            Effect.fail(
              new ElevenLabsTransportError({
                operation: "connect",
                reason: "unavailable",
              }),
            ),
        }),
      );

      const output = yield* coordinateCall(Stream.empty, options(states)).pipe(
        Stream.provide(Layer.merge(harness.layer, failedTts)),
        Stream.runCollect,
      );

      expect(Array.from(output)).toContainEqual({
        _tag: "failure",
        stage: "elevenlabs",
        reason: "ElevenLabs connection failed",
        recoverable: false,
      });
      expect(yield* Ref.get(harness.deepgramClosed)).toBe(1);
    }),
  );
});
