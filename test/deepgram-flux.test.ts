import { describe, expect, it } from "@effect/vitest";
import { Effect, Fiber, Layer, Queue, Schema, Scope } from "effect";
import {
  buildFluxUrl,
  decodeFluxMessage,
  deepgramSocketFailure,
  DeepgramFlux,
  DeepgramTransport,
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

describe("Deepgram Flux", () => {
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
        '{"type":"CloseStream"}',
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
