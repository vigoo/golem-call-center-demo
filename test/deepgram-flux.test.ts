import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Queue, Schema, Scope } from "effect";
import {
  buildFluxUrl,
  decodeFluxMessage,
  deepgramSocketFailure,
  DeepgramFlux,
  DeepgramTransport,
  DeepgramTransportError,
  type DeepgramConnection,
  FluxEvent,
} from "../src/deepgram-flux.js";

interface FakeTransport {
  readonly layer: Layer.Layer<DeepgramTransport>;
  readonly inbound: Queue.Queue<string>;
  readonly sent: Array<string | Uint8Array>;
  readonly connects: Array<{
    url: string;
    headers: ReadonlyArray<readonly [string, string]>;
  }>;
  readonly closes: Array<string>;
}

const makeFakeTransport = Effect.gen(function* () {
  const inbound = yield* Queue.unbounded<string>();
  const sent: Array<string | Uint8Array> = [];
  const connects: FakeTransport["connects"] = [];
  const closes: Array<string> = [];

  const connection: DeepgramConnection = {
    receiveText: Queue.take(inbound),
    sendBinary: (bytes) => Effect.sync(() => sent.push(bytes)),
    sendText: (text) => Effect.sync(() => sent.push(text)),
  };
  const layer = Layer.succeed(
    DeepgramTransport,
    DeepgramTransport.of({
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

const provideFake = <A, E>(effect: Effect.Effect<A, E, DeepgramFlux | Scope.Scope>, fake: FakeTransport) =>
  effect.pipe(Effect.provide(DeepgramFlux.layer), Effect.provide(fake.layer));

const makeRecoveringTransport = Effect.gen(function* () {
  const control = { replacementGate: Effect.void, failReplacementAfter: -1, wakeOnClose: true };
  const sockets: Array<{
    inbound: Queue.Queue<string | DeepgramTransportError>;
    sent: Array<Uint8Array>;
    failSend: boolean;
    closed: boolean;
  }> = [];
  const layer = Layer.succeed(DeepgramTransport, DeepgramTransport.of({
    connect: () => Effect.acquireRelease(Effect.gen(function* () {
      const socket = {
        inbound: yield* Queue.unbounded<string | DeepgramTransportError>(),
        sent: [] as Array<Uint8Array>, failSend: false, closed: false,
      };
      sockets.push(socket);
      return {
        socket,
        receiveText: Queue.take(socket.inbound).pipe(Effect.flatMap((value) =>
          typeof value === "string" ? Effect.succeed(value) : Effect.fail(value))),
        sendBinary: (bytes: Uint8Array) => Effect.suspend(() => {
          if (socket.failSend || (sockets.indexOf(socket) === 1 && socket.sent.length === control.failReplacementAfter)) return Effect.fail(new DeepgramTransportError({ operation: "send", reason: "lost", sessionLost: true }));
          return Effect.sync(() => { socket.sent.push(bytes.slice()); });
        }),
        sendText: () => Effect.void,
      };
    }), ({ socket }) => Effect.sync(() => { socket.closed = true; }).pipe(
      Effect.andThen(Effect.suspend(() => control.wakeOnClose
        ? Queue.offer(socket.inbound, new DeepgramTransportError({ operation: "receive", reason: "reader released", closeCode: 1000 })) : Effect.void)),
    )).pipe(Effect.tap(() => sockets.length > 1 ? control.replacementGate : Effect.void)),
  }));
  return { sockets, layer, control };
});

const endOfTurn = (seconds: number) => JSON.stringify({
  type: "TurnInfo", request_id: "request", sequence_id: 1, event: "EndOfTurn",
  turn_index: 0, audio_window_start: 0, audio_window_end: seconds,
  transcript: "A distinct utterance", words: [], end_of_turn_confidence: 0.9,
});

describe("Deepgram Flux", () => {
  it.live("does not replay the last sample at an exact floating-point boundary", () =>
    Effect.gen(function* () {
      const fake = yield* makeRecoveringTransport;
      yield* Effect.scoped(Effect.gen(function* () {
        const session = yield* (yield* DeepgramFlux).open("token");
        yield* session.sendAudio(new Uint8Array(2002));
        yield* Queue.offer(fake.sockets[0]!.inbound, endOfTurn(0.0625625));
        const final = yield* session.receive;
        expect(final).toMatchObject({ _tag: "Turn", audioEnd: 2002 });
        if (final._tag !== "Turn") throw new Error("Expected final");
        yield* session.commitTurn(final, Effect.void);
        yield* Queue.offer(fake.sockets[0]!.inbound, new DeepgramTransportError({ operation: "receive", reason: "lost", sessionLost: true }));
        const receiving = yield* Effect.forkChild(session.receive);
        yield* Effect.repeat(Effect.yieldNow, { while: () => fake.sockets.length < 2 });
        yield* Queue.offer(fake.sockets[1]!.inbound, '{"type":"Connected","request_id":"new","sequence_id":0}');
        yield* Fiber.join(receiving);
        expect(fake.sockets[1]!.sent).toEqual([]);
      }).pipe(Effect.provide(DeepgramFlux.layer), Effect.provide(fake.layer)));
    }).pipe(Effect.timeout("3 seconds")),
  );

  it.live("fences retired receive errors and rejects an old final during gated reconstruction", () =>
    Effect.gen(function* () {
      const fake = yield* makeRecoveringTransport;
      const gate = yield* Deferred.make<void>();
      fake.control.replacementGate = Deferred.await(gate);
      fake.control.wakeOnClose = false;
      yield* Effect.scoped(Effect.gen(function* () {
        const session = yield* (yield* DeepgramFlux).open("token");
        yield* session.sendAudio(new Uint8Array([1, 2, 3, 4]));
        yield* Queue.offer(fake.sockets[0]!.inbound, endOfTurn(1 / 16000));
        const old = yield* session.receive;
        if (old._tag !== "Turn") throw new Error("Expected final");
        const receiving = yield* Effect.forkChild(session.receive);
        fake.sockets[0]!.failSend = true;
        const sending = yield* Effect.forkChild(session.sendAudio(new Uint8Array([5, 6])));
        yield* Effect.repeat(Effect.yieldNow, { while: () => fake.sockets.length < 2 });
        let accepted = false;
        expect(yield* session.commitTurn(old, Effect.sync(() => { accepted = true; }))).toBeNull();
        expect(accepted).toBe(false);
        yield* Deferred.succeed(gate, undefined);
        yield* Fiber.join(sending);
        yield* Queue.offer(fake.sockets[1]!.inbound, endOfTurn(3 / 16000));
        expect(yield* Fiber.join(receiving)).toMatchObject({ _tag: "Turn", audioEnd: 6 });
        expect(fake.sockets).toHaveLength(2);
        expect(fake.sockets[1]!.sent).toEqual([new Uint8Array([1, 2, 3, 4, 5, 6])]);
      }).pipe(Effect.provide(DeepgramFlux.layer), Effect.provide(fake.layer)));
    }).pipe(Effect.timeout("3 seconds")),
  );

  it.live("hangup wins while a replacement is being acquired", () =>
    Effect.gen(function* () {
      const fake = yield* makeRecoveringTransport;
      const gate = yield* Deferred.make<void>();
      fake.control.replacementGate = Deferred.await(gate);
      yield* Effect.scoped(Effect.gen(function* () {
        const session = yield* (yield* DeepgramFlux).open("token");
        fake.sockets[0]!.failSend = true;
        const sending = yield* Effect.forkChild(session.sendAudio(new Uint8Array([5, 6])));
        yield* Effect.repeat(Effect.yieldNow, { while: () => fake.sockets.length < 2 });
        yield* session.closeStream;
        yield* Deferred.succeed(gate, undefined);
        yield* Fiber.join(sending);
        expect(fake.sockets[1]!.sent).toEqual([]);
        expect(fake.sockets.every((socket) => socket.closed)).toBe(true);
      }).pipe(Effect.provide(DeepgramFlux.layer), Effect.provide(fake.layer)));
    }).pipe(Effect.timeout("3 seconds")),
  );

  it.live("reconstructs again when the replacement is lost partway through PCM replay", () =>
    Effect.gen(function* () {
      const fake = yield* makeRecoveringTransport;
      fake.control.failReplacementAfter = 1;
      yield* Effect.scoped(Effect.gen(function* () {
        const session = yield* (yield* DeepgramFlux).open("token");
        const pcm = Uint8Array.from({ length: 5120 }, (_, index) => index % 251);
        yield* session.sendAudio(pcm);
        fake.sockets[0]!.failSend = true;
        yield* session.sendAudio(new Uint8Array([252, 253]));
        expect(fake.sockets).toHaveLength(3);
        expect(Uint8Array.from(fake.sockets[2]!.sent.flatMap((chunk) => [...chunk]))).toEqual(Uint8Array.from([...pcm, 252, 253]));
      }).pipe(Effect.provide(DeepgramFlux.layer), Effect.provide(fake.layer)));
    }).pipe(Effect.timeout("3 seconds")),
  );

  it.live("reconstructs only uncommitted PCM after receive and send session loss", () =>
    Effect.gen(function* () {
      const fake = yield* makeRecoveringTransport;
      yield* Effect.scoped(Effect.gen(function* () {
        const session = yield* (yield* DeepgramFlux).open("token");
        yield* session.sendAudio(new Uint8Array([10, 20, 30, 40, 50, 60]));
        yield* Queue.offer(fake.sockets[0]!.inbound, endOfTurn(2 / 16000));
        const first = yield* session.receive;
        expect(first).toMatchObject({ _tag: "Turn", audioEnd: 4 });
        if (first._tag !== "Turn") throw new Error("Expected turn");
        yield* session.commitTurn(first, Effect.void);
        yield* Queue.offer(fake.sockets[0]!.inbound, new DeepgramTransportError({ operation: "receive", reason: "lost", sessionLost: true }));
        const receiving = yield* Effect.forkChild(session.receive);
        yield* Effect.repeat(Effect.yieldNow, { while: () => fake.sockets.length < 2 });
        yield* Queue.offer(fake.sockets[1]!.inbound, endOfTurn(1 / 16000));
        const second = yield* Fiber.join(receiving);
        expect(second).toMatchObject({ _tag: "Turn", audioEnd: 6 });
        expect(fake.sockets[1]!.sent).toEqual([new Uint8Array([50, 60])]);
        expect(fake.sockets[0]!.closed).toBe(true);
        if (second._tag !== "Turn") throw new Error("Expected turn");
        yield* session.commitTurn(second, Effect.void);
        fake.sockets[1]!.failSend = true;
        yield* session.sendAudio(new Uint8Array([70, 80]));
        expect(fake.sockets).toHaveLength(3);
        expect(fake.sockets[2]!.sent).toEqual([new Uint8Array([70, 80])]);
        yield* Queue.offer(fake.sockets[2]!.inbound, endOfTurn(1 / 16000));
        expect(yield* session.receive).toMatchObject({ _tag: "Turn", audioEnd: 8 });
      }).pipe(Effect.provide(DeepgramFlux.layer), Effect.provide(fake.layer)));
      expect(fake.sockets.every((socket) => socket.closed)).toBe(true);
    }).pipe(Effect.timeout("3 seconds")),
  );

  it.live("asks for a repeat after overflow and discards only the damaged utterance", () =>
    Effect.gen(function* () {
      const fake = yield* makeRecoveringTransport;
      yield* Effect.scoped(Effect.gen(function* () {
        const session = yield* (yield* DeepgramFlux).open("token");
        yield* session.sendAudio(new Uint8Array(320002));
        yield* Queue.offer(fake.sockets[0]!.inbound, new DeepgramTransportError({ operation: "receive", reason: "lost", sessionLost: true }));
        expect(yield* session.receive).toEqual({ _tag: "RepeatRequired" });
        expect(fake.sockets[1]!.sent).toEqual([]);
        yield* session.sendAudio(new Uint8Array([1, 2]));
        yield* Queue.offer(fake.sockets[1]!.inbound, endOfTurn(1 / 16000));
        yield* session.sendAudio(new Uint8Array([3, 4]));
        yield* Queue.offer(fake.sockets[1]!.inbound, endOfTurn(2 / 16000));
        expect(yield* session.receive).toMatchObject({ _tag: "Turn", audioEnd: 320006 });
      }).pipe(Effect.provide(DeepgramFlux.layer), Effect.provide(fake.layer)));
    }).pipe(Effect.timeout("3 seconds")),
  );

  it.effect("does not reconstruct ordinary provider failures", () =>
    Effect.gen(function* () {
      const fake = yield* makeRecoveringTransport;
      yield* Effect.scoped(Effect.gen(function* () {
        const session = yield* (yield* DeepgramFlux).open("token");
        const error = new DeepgramTransportError({ operation: "receive", reason: "unauthorized" });
        yield* Queue.offer(fake.sockets[0]!.inbound, error);
        expect(yield* session.receive.pipe(Effect.result)).toMatchObject({ _tag: "Failure", failure: error });
        expect(fake.sockets).toHaveLength(1);
      }).pipe(Effect.provide(DeepgramFlux.layer), Effect.provide(fake.layer)));
    }),
  );

  it("builds the v2 raw PCM URL with repeated language hints", () => {
    const url = new URL(
      buildFluxUrl({
        model: "flux-general-multi",
        sampleRate: 16_000,
        languageHints: ["en", "es"],
        eotThreshold: 0.8,
      }),
    );
    expect(url.pathname).toBe("/v2/listen");
    expect(url.searchParams.get("model")).toBe("flux-general-multi");
    expect(url.searchParams.get("encoding")).toBe("linear16");
    expect(url.searchParams.get("sample_rate")).toBe("16000");
    expect(url.searchParams.getAll("language_hint")).toEqual(["en", "es"]);
    expect(url.searchParams.get("eot_threshold")).toBe("0.8");
  });

  it.effect("connects with token auth and sends binary audio and controls", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeTransport;
      yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const flux = yield* DeepgramFlux;
            const session = yield* flux.open("secret-token");
            yield* session.sendAudio(new Uint8Array([1, 2, 3]));
            yield* session.forceEndTurn;
            yield* session.configure({ languageHints: ["en"], numerals: true });
            yield* session.closeStream;
          }),
          fake,
        ),
      );

      expect(fake.connects).toHaveLength(1);
      expect(fake.connects[0]?.headers).toEqual([
        ["Authorization", "Token secret-token"],
      ]);
      expect(fake.sent[0]).toEqual(new Uint8Array([1, 2, 3]));
      expect(fake.sent.slice(1)).toEqual([
        '{"type":"ForceEndTurn"}',
        '{"type":"Configure","language_hints":["en"],"numerals":true}',
      ]);
      expect(fake.closes).toEqual(["closed"]);
    }),
  );

  it.effect("maps turn events and tolerates future fields", () =>
    Effect.gen(function* () {
      const event = yield* decodeFluxMessage(
        JSON.stringify({
          type: "TurnInfo",
          request_id: "request-1",
          sequence_id: 7,
          event: "EndOfTurn",
          turn_index: 3,
          audio_window_start: 0,
          audio_window_end: 1.3,
          transcript: "Hello there",
          words: [{ word: "Hello", confidence: 0.98 }],
          end_of_turn_confidence: 0.91,
          trigger: "future-trigger",
          future_field: { nested: true },
        }),
      );

      expect(event).toEqual({
        _tag: "Turn",
        kind: "EndOfTurn",
        requestId: "request-1",
        sequence: 7,
        turnIndex: 3,
        transcript: "Hello there",
        endOfTurnConfidence: 0.91,
        audioWindowEnd: 1.3,
      });
    }),
  );

  it.effect("decodes warning, configuration, and fatal error messages", () =>
    Effect.gen(function* () {
      const configured = yield* decodeFluxMessage(
        '{"type":"ConfigureSuccess","request_id":"r","sequence_id":2,"thresholds":{},"keyterms":[]}',
      );
      const warning = yield* decodeFluxMessage(
        '{"type":"Warning","request_id":"r","sequence_id":3,"code":"SLOW_AUDIO","description":"slow"}',
      );
      const fatal = yield* decodeFluxMessage(
        '{"type":"Error","sequence_id":4,"code":"INTERNAL_SERVER_ERROR","description":"failed"}',
      );

      expect(configured._tag).toBe("Configured");
      expect(warning._tag).toBe("Warning");
      expect(fatal._tag).toBe("FatalError");
    }),
  );

  it.effect("maps a minimal configuration failure to a schema-valid event", () =>
    Effect.gen(function* () {
      const rejected = yield* decodeFluxMessage(
        '{"type":"ConfigureFailure","request_id":"r","sequence_id":0}',
      );

      expect(rejected).toEqual({
        _tag: "ConfigurationRejected",
        requestId: "r",
        sequence: 0,
      });
      expect(() => Schema.encodeSync(FluxEvent)(rejected)).not.toThrow();
    }),
  );

  it("preserves close codes without exposing close reasons", () => {
    const clean = deepgramSocketFailure("receive", {
      _tag: "SocketCloseError",
      code: 1000,
      closeReason: "private-clean-close-reason",
    });
    const policyViolation = deepgramSocketFailure("receive", {
      _tag: "SocketCloseError",
      code: 1008,
      closeReason: "private-policy-close-reason",
    });

    expect(clean).toMatchObject({ closeCode: 1000 });
    expect(policyViolation).toMatchObject({ closeCode: 1008 });
    expect(JSON.stringify([clean, policyViolation])).not.toContain("private-");
    const lost = deepgramSocketFailure("send", { _tag: "SocketWriteError" }, true);
    expect(lost.sessionLost).toBe(true);
    expect(clean.sessionLost).toBeUndefined();
  });

  it.effect("rejects malformed and unknown provider messages without echoing payloads", () =>
    Effect.gen(function* () {
      const malformed = yield* Effect.exit(
        decodeFluxMessage('{"type":"TurnInfo","transcript":"private words"}'),
      );
      expect(malformed._tag).toBe("Failure");
      expect(JSON.stringify(malformed)).not.toContain("private words");

      const unknown = yield* Effect.exit(
        decodeFluxMessage('{"type":"BrandNewEvent","secret":"do not echo"}'),
      );
      expect(unknown._tag).toBe("Failure");
      expect(JSON.stringify(unknown)).not.toContain("do not echo");
    }),
  );

  it.effect("releases the transport when a blocked receive is interrupted", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeTransport;
      yield* Effect.scoped(
        provideFake(
          Effect.gen(function* () {
            const flux = yield* DeepgramFlux;
            const session = yield* flux.open("token");
            const fiber = yield* Effect.forkChild(session.receive);
            yield* Effect.yieldNow;
            yield* Fiber.interrupt(fiber);
          }),
          fake,
        ),
      );
      expect(fake.closes).toEqual(["closed"]);
    }),
  );
});
