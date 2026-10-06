import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Queue, Schema, Scope } from "effect";
import { ResponseId } from "../src/call-protocol.js";
import {
  buildElevenLabsUrl,
  ElevenLabsTransport,
  ElevenLabsTransportError,
  type ElevenLabsConnection,
  ElevenLabsTts,
} from "../src/elevenlabs-tts.js";

interface FakeTransport {
  readonly layer: Layer.Layer<ElevenLabsTransport>;
  readonly inbound: Queue.Queue<string>;
  readonly sent: Array<Record<string, unknown>>;
  readonly connects: Array<{
    url: string;
    headers: ReadonlyArray<readonly [string, string]>;
  }>;
  readonly closes: Array<string>;
}

const makeFakeTransport = (
  onSend: (
    message: Record<string, unknown>,
  ) => Effect.Effect<void, ElevenLabsTransportError> = () => Effect.void,
) => Effect.gen(function* () {
  const inbound = yield* Queue.unbounded<string>();
  const sent: Array<Record<string, unknown>> = [];
  const connects: FakeTransport["connects"] = [];
  const closes: Array<string> = [];
  const connection: ElevenLabsConnection = {
    receiveText: Queue.take(inbound),
    sendText: (text) => {
      const message = JSON.parse(text) as Record<string, unknown>;
      return Effect.sync(() => sent.push(message)).pipe(Effect.andThen(onSend(message)));
    },
  };
  const layer = Layer.succeed(
    ElevenLabsTransport,
    ElevenLabsTransport.of({
      connect: (url, headers) =>
        Effect.acquireRelease(
          Effect.sync(() => {
            connects.push({ url, headers });
            return connection;
          }),
          () => Effect.sync(() => closes.push("closed")),
        ),
    }),
  );
  return { layer, inbound, sent, connects, closes } satisfies FakeTransport;
});

const responseId = (value: string) => Schema.decodeUnknownSync(ResponseId)(value);

const options = {
  voiceId: "voice/with spaces",
  modelId: "eleven_flash_v2_5",
  outputFormat: "pcm_16000",
  inactivityTimeoutSeconds: 45,
  voiceSettings: {
    stability: 0.4,
    similarityBoost: 0.7,
    speed: 1.1,
    useSpeakerBoost: true,
  },
  chunkLengthSchedule: [80, 120],
} as const;

const provideFake = <A, E>(
  effect: Effect.Effect<A, E, ElevenLabsTts | Scope.Scope>,
  fake: FakeTransport,
) => effect.pipe(Effect.provide(ElevenLabsTts.layer), Effect.provide(fake.layer));

describe("ElevenLabs multi-context TTS", () => {
  it("builds a URL with explicit model, PCM format, and timeout", () => {
    const url = new URL(buildElevenLabsUrl(options));
    expect(url.pathname).toBe(
      "/v1/text-to-speech/voice%2Fwith%20spaces/multi-stream-input",
    );
    expect(url.searchParams.get("model_id")).toBe("eleven_flash_v2_5");
    expect(url.searchParams.get("output_format")).toBe("pcm_16000");
    expect(url.searchParams.get("inactivity_timeout")).toBe("45");
  });

  it.effect("initializes, streams, flushes, and closes a response context", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeTransport();
      const id = responseId("response-4");
      yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("secret-key", options);
            yield* session.startContext(id);
            yield* session.sendText(id, "Hello ");
            yield* session.sendText(id, "world. ");
            yield* session.finishContext(id);
          }),
          fake,
        ),
      );

      expect(fake.connects).toHaveLength(1);
      expect(fake.connects[0]?.headers).toEqual([["xi-api-key", "secret-key"]]);
      expect(fake.sent).toEqual([
        {
          context_id: "response-4",
          text: " ",
          voice_settings: {
            stability: 0.4,
            similarity_boost: 0.7,
            speed: 1.1,
            use_speaker_boost: true,
          },
          generation_config: { chunk_length_schedule: [80, 120] },
        },
        { context_id: "response-4", text: "Hello " },
        { context_id: "response-4", text: "world. " },
        { context_id: "response-4", flush: true },
        { context_id: "response-4", close_context: true },
        { close_socket: true },
      ]);
      expect(fake.closes).toEqual(["closed"]);
    }),
  );

  it.effect("routes interleaved audio to open contexts and decodes base64 immediately", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeTransport();
      const first = responseId("response-2");
      const second = responseId("response-9");
      const events = yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.startContext(first);
            yield* session.startContext(second);
            yield* Queue.offer(
              fake.inbound,
              '{"audio":"AQI=","contextId":"response-9","isFinal":false}',
            );
            yield* Queue.offer(
              fake.inbound,
              '{"audio":"AwQ=","contextId":"response-2","isFinal":false}',
            );
            yield* Queue.offer(fake.inbound, '{"contextId":"response-9","isFinal":true}');
            return [yield* session.receive, yield* session.receive, yield* session.receive];
          }),
          fake,
        ),
      );

      expect(events).toEqual([
        { _tag: "Audio", responseId: "response-9", payload: new Uint8Array([1, 2]) },
        { _tag: "Audio", responseId: "response-2", payload: new Uint8Array([3, 4]) },
        { _tag: "Final", responseId: "response-9" },
      ]);
    }),
  );

  it.effect("drops late audio after interruption while continuing to the next context", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeTransport();
      const oldId = responseId("response-1");
      const currentId = responseId("response-11");
      const event = yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.startContext(oldId);
            yield* session.startContext(currentId);
            yield* session.closeContext(oldId);
            yield* Queue.offer(
              fake.inbound,
              '{"audio":"CQ==","contextId":"response-1","isFinal":false}',
            );
            yield* Queue.offer(
              fake.inbound,
              '{"audio":"Cw==","contextId":"response-11","isFinal":false}',
            );
            return yield* session.receive;
          }),
          fake,
        ),
      );

      expect(event).toEqual({
        _tag: "Audio",
        responseId: "response-11",
        payload: new Uint8Array([11]),
      });
      expect(fake.sent).toContainEqual({
        context_id: "response-1",
        close_context: true,
      });
    }),
  );

  it.effect("rejects malformed base64 and messages without a routable context", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeTransport();
      const id = responseId("response-3");
      const outcomes = yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.startContext(id);
            yield* Queue.offer(
              fake.inbound,
              '{"audio":"not base64!","contextId":"response-3"}',
            );
            const malformed = yield* Effect.result(session.receive);
            yield* Queue.offer(fake.inbound, '{"audio":"AQI="}');
            const unroutable = yield* Effect.result(session.receive);
            return [malformed, unroutable];
          }),
          fake,
        ),
      );

      expect(outcomes[0]!._tag).toBe("Failure");
      expect(outcomes[1]!._tag).toBe("Failure");
    }),
  );

  it.effect("rejects writes after finish and duplicate context IDs", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeTransport();
      const id = responseId("response-5");
      const outcomes = yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.startContext(id);
            const duplicate = yield* Effect.result(session.startContext(id));
            yield* session.finishContext(id);
            const lateText = yield* Effect.result(session.sendText(id, "too late"));
            return [duplicate, lateText];
          }),
          fake,
        ),
      );

      expect(outcomes[0]!._tag).toBe("Failure");
      expect(outcomes[1]!._tag).toBe("Failure");
    }),
  );

  it.effect("releases a blocked receive and closes the socket once on interruption", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeTransport();
      yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.closeSocket;
            const fiber = yield* Effect.forkChild(session.receive);
            yield* Effect.yieldNow;
            yield* Fiber.interrupt(fiber);
          }),
          fake,
        ),
      );

      expect(fake.sent.filter((message) => message.close_socket === true)).toHaveLength(1);
      expect(fake.closes).toEqual(["closed"]);
    }),
  );

  it.effect("rejects new contexts after explicit socket shutdown", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeTransport();
      const id = responseId("response-after-close");
      const outcome = yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.closeSocket;
            return yield* Effect.result(session.startContext(id));
          }),
          fake,
        ),
      );

      expect(outcome).toMatchObject({
        _tag: "Failure",
        failure: {
          _tag: "ElevenLabsContextError",
          operation: "start",
          responseId: "response-after-close",
          reason: "socket-closed",
        },
      });
      expect(fake.sent).toEqual([{ close_socket: true }]);
    }),
  );

  it.effect("rejects a queued context start when socket shutdown wins the race", () =>
    Effect.gen(function* () {
      const sendStarted = yield* Deferred.make<void>();
      const releaseSend = yield* Deferred.make<void>();
      const fake = yield* makeFakeTransport((message) =>
        message.text === "blocking"
          ? Deferred.succeed(sendStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseSend)),
            )
          : Effect.void,
      );
      const first = responseId("response-active");
      const queued = responseId("response-queued");
      const outcome = yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.startContext(first);
            const sendFiber = yield* Effect.forkChild(session.sendText(first, "blocking"));
            yield* Deferred.await(sendStarted);
            const startFiber = yield* Effect.forkChild(
              Effect.result(session.startContext(queued)),
            );
            const closeFiber = yield* Effect.forkChild(session.closeSocket);
            yield* Effect.yieldNow;
            yield* Deferred.succeed(releaseSend, undefined);
            yield* Fiber.join(sendFiber);
            const result = yield* Fiber.join(startFiber);
            yield* Fiber.join(closeFiber);
            return result;
          }),
          fake,
        ),
      );

      expect(outcome).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "ElevenLabsContextError", reason: "socket-closed" },
      });
      expect(fake.sent).not.toContainEqual(expect.objectContaining({ context_id: queued }));
      expect(fake.sent.filter((message) => message.close_socket === true)).toHaveLength(1);
    }),
  );

  it.effect("finishes an interrupted context close after a blocked write releases", () =>
    Effect.gen(function* () {
      const sendStarted = yield* Deferred.make<void>();
      const releaseSend = yield* Deferred.make<void>();
      const fake = yield* makeFakeTransport((message) =>
        message.text === "blocking"
          ? Deferred.succeed(sendStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseSend)),
            )
          : Effect.void,
      );
      const oldId = responseId("response-old");
      const currentId = responseId("response-current");
      const event = yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.startContext(oldId);
            yield* session.startContext(currentId);
            const sendFiber = yield* Effect.forkChild(session.sendText(oldId, "blocking"));
            yield* Deferred.await(sendStarted);
            const closeFiber = yield* Effect.forkChild(session.closeContext(oldId));
            yield* Effect.yieldNow;
            yield* Queue.offer(
              fake.inbound,
              '{"audio":"AQ==","contextId":"response-old","isFinal":false}',
            );
            yield* Queue.offer(
              fake.inbound,
              '{"audio":"Ag==","contextId":"response-current","isFinal":false}',
            );
            const received = yield* session.receive;
            const interruptFiber = yield* Effect.forkChild(Fiber.interrupt(closeFiber));
            yield* Effect.yieldNow;
            yield* Deferred.succeed(releaseSend, undefined);
            yield* Fiber.join(sendFiber);
            yield* Fiber.join(interruptFiber);
            return received;
          }),
          fake,
        ),
      );

      expect(event).toEqual({
        _tag: "Audio",
        responseId: "response-current",
        payload: new Uint8Array([2]),
      });
      expect(
        fake.sent.filter(
          (message) => message.context_id === oldId && message.close_context === true,
        ),
      ).toHaveLength(1);
    }),
  );

  it.effect("tombstones a response closed before its queued start acquires the permit", () =>
    Effect.gen(function* () {
      const sendStarted = yield* Deferred.make<void>();
      const releaseSend = yield* Deferred.make<void>();
      const fake = yield* makeFakeTransport((message) =>
        message.text === "blocking"
          ? Deferred.succeed(sendStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseSend)),
            )
          : Effect.void,
      );
      const activeId = responseId("response-active-close-race");
      const cancelledId = responseId("response-cancelled-before-start");
      const outcome = yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.startContext(activeId);
            const sendFiber = yield* Effect.forkChild(
              session.sendText(activeId, "blocking"),
            );
            yield* Deferred.await(sendStarted);
            const startFiber = yield* Effect.forkChild(
              Effect.result(session.startContext(cancelledId)),
            );
            yield* session.closeContext(cancelledId);
            yield* Deferred.succeed(releaseSend, undefined);
            yield* Fiber.join(sendFiber);
            return yield* Fiber.join(startFiber);
          }),
          fake,
        ),
      );

      expect(outcome).toMatchObject({
        _tag: "Failure",
        failure: {
          _tag: "ElevenLabsContextError",
          operation: "start",
          reason: "already-started",
        },
      });
      expect(fake.sent).not.toContainEqual(expect.objectContaining({ context_id: cancelledId }));
    }),
  );

  it.effect("keeps repeated closes of a never-started response wire-silent", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeTransport();
      const id = responseId("response-never-started");
      const outcome = yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.closeContext(id);
            yield* session.closeContext(id);
            return yield* Effect.result(session.startContext(id));
          }),
          fake,
        ),
      );

      expect(outcome).toMatchObject({
        _tag: "Failure",
        failure: {
          _tag: "ElevenLabsContextError",
          operation: "start",
          reason: "already-started",
        },
      });
      expect(fake.sent).not.toContainEqual(expect.objectContaining({ context_id: id }));
    }),
  );

  it.effect("does not duplicate a finish close interrupted after the frame is sent", () =>
    Effect.gen(function* () {
      const closeSent = yield* Deferred.make<void>();
      const releaseClose = yield* Deferred.make<void>();
      const id = responseId("response-finish-race");
      const fake = yield* makeFakeTransport((message) =>
        message.context_id === id && message.close_context === true
          ? Deferred.succeed(closeSent, undefined).pipe(
              Effect.andThen(Deferred.await(releaseClose)),
            )
          : Effect.void,
      );
      yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.startContext(id);
            const finishFiber = yield* Effect.forkChild(session.finishContext(id));
            yield* Deferred.await(closeSent);
            const interruptFiber = yield* Effect.forkChild(Fiber.interrupt(finishFiber));
            yield* Effect.yieldNow;
            yield* Deferred.succeed(releaseClose, undefined);
            yield* Fiber.join(interruptFiber);
            yield* session.closeContext(id);
          }),
          fake,
        ),
      );

      expect(
        fake.sent.filter(
          (message) => message.context_id === id && message.close_context === true,
        ),
      ).toHaveLength(1);
    }),
  );

  it.effect("completes context closure when finish is interrupted during flush", () =>
    Effect.gen(function* () {
      const flushStarted = yield* Deferred.make<void>();
      const releaseFlush = yield* Deferred.make<void>();
      const id = responseId("response-finish-flush-race");
      const fake = yield* makeFakeTransport((message) =>
        message.context_id === id && message.flush === true
          ? Deferred.succeed(flushStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseFlush)),
            )
          : Effect.void,
      );
      yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            yield* session.startContext(id);
            const finishFiber = yield* Effect.forkChild(session.finishContext(id));
            yield* Deferred.await(flushStarted);
            const interruptFiber = yield* Effect.forkChild(Fiber.interrupt(finishFiber));
            yield* Effect.yieldNow;
            yield* Deferred.succeed(releaseFlush, undefined);
            yield* Fiber.join(interruptFiber);
          }),
          fake,
        ),
      );

      expect(
        fake.sent.filter(
          (message) => message.context_id === id && message.close_context === true,
        ),
      ).toHaveLength(1);
    }),
  );

  it.effect("allows retrying a context whose start command failed", () =>
    Effect.gen(function* () {
      let failStart = true;
      const fake = yield* makeFakeTransport((message) => {
        if (message.context_id !== "response-retry-start" || message.text !== " ") {
          return Effect.void;
        }
        if (!failStart) return Effect.void;
        failStart = false;
        return Effect.fail(
          new ElevenLabsTransportError({ operation: "send", reason: "injected failure" }),
        );
      });
      const id = responseId("response-retry-start");
      const retry = yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            const first = yield* Effect.result(session.startContext(id));
            expect(first).toMatchObject({
              _tag: "Failure",
              failure: { _tag: "ElevenLabsTransportError" },
            });
            return yield* Effect.result(session.startContext(id));
          }),
          fake,
        ),
      );

      expect(retry).toMatchObject({ _tag: "Success" });
    }),
  );

  it.effect("allows retrying a context whose start command was interrupted", () =>
    Effect.gen(function* () {
      const startStarted = yield* Deferred.make<void>();
      const releaseStart = yield* Deferred.make<void>();
      let blockFirstStart = true;
      const fake = yield* makeFakeTransport((message) => {
        if (
          message.context_id !== "response-retry-interrupted-start" ||
          message.text !== " " ||
          !blockFirstStart
        ) {
          return Effect.void;
        }
        blockFirstStart = false;
        return Deferred.succeed(startStarted, undefined).pipe(
          Effect.andThen(Deferred.await(releaseStart)),
        );
      });
      const id = responseId("response-retry-interrupted-start");
      const retry = yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const tts = yield* ElevenLabsTts;
            const session = yield* tts.open("key", options);
            const first = yield* Effect.forkChild(session.startContext(id));
            yield* Deferred.await(startStarted);
            yield* Fiber.interrupt(first);
            yield* Deferred.succeed(releaseStart, undefined);
            return yield* Effect.result(session.startContext(id));
          }),
          fake,
        ),
      );

      expect(retry).toMatchObject({ _tag: "Success" });
    }),
  );

});
