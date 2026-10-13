import { Context, Deferred, Effect, Exit, Layer, Ref, Schema, Scope, Semaphore } from "effect";

const RequestId = Schema.NonEmptyString;
const ProviderSequence = Schema.Natural;
const NumberLike = Schema.Union([Schema.Number, Schema.String]);

const DeepgramWord = Schema.Struct({
  word: Schema.String,
  confidence: NumberLike,
  start: Schema.optionalKey(Schema.Number),
  end: Schema.optionalKey(Schema.Number),
});

export const DeepgramConnected = Schema.Struct({
  type: Schema.Literal("Connected"),
  request_id: RequestId,
  sequence_id: ProviderSequence,
});

export const DeepgramTurnInfo = Schema.Struct({
  type: Schema.Literal("TurnInfo"),
  request_id: RequestId,
  sequence_id: ProviderSequence,
  event: Schema.Literals([
    "Update",
    "StartOfTurn",
    "EagerEndOfTurn",
    "TurnResumed",
    "EndOfTurn",
  ]),
  turn_index: Schema.Natural,
  audio_window_start: NumberLike,
  audio_window_end: NumberLike,
  transcript: Schema.String,
  words: Schema.Array(DeepgramWord),
  end_of_turn_confidence: NumberLike,
  trigger: Schema.optionalKey(Schema.String),
  languages: Schema.optionalKey(Schema.Array(Schema.String)),
  languages_hinted: Schema.optionalKey(Schema.Array(Schema.String)),
});

const DeepgramThresholds = Schema.Struct({
  eager_eot_threshold: Schema.optionalKey(NumberLike),
  eot_threshold: Schema.optionalKey(NumberLike),
  eot_timeout_ms: Schema.optionalKey(NumberLike),
});

export const DeepgramConfigureSuccess = Schema.Struct({
  type: Schema.Literal("ConfigureSuccess"),
  request_id: RequestId,
  sequence_id: ProviderSequence,
  thresholds: DeepgramThresholds,
  keyterms: Schema.Union([Schema.String, Schema.Array(Schema.String)]),
  language_hints: Schema.optionalKey(Schema.Array(Schema.String)),
  numerals: Schema.optionalKey(Schema.Boolean),
});

export const DeepgramConfigureFailure = Schema.Struct({
  type: Schema.Literal("ConfigureFailure"),
  request_id: RequestId,
  sequence_id: ProviderSequence,
  code: Schema.optionalKey(Schema.String),
  description: Schema.optionalKey(Schema.String),
});

export const DeepgramWarning = Schema.Struct({
  type: Schema.Literal("Warning"),
  request_id: RequestId,
  sequence_id: ProviderSequence,
  code: Schema.String,
  description: Schema.String,
});

export const DeepgramFatalError = Schema.Struct({
  type: Schema.Literal("Error"),
  sequence_id: ProviderSequence,
  code: Schema.String,
  description: Schema.String,
});

export const DeepgramServerMessage = Schema.Union([
  DeepgramConnected,
  DeepgramTurnInfo,
  DeepgramConfigureSuccess,
  DeepgramConfigureFailure,
  DeepgramWarning,
  DeepgramFatalError,
]);
export type DeepgramServerMessage = typeof DeepgramServerMessage.Type;

export const FluxEvent = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("Connected"),
    requestId: RequestId,
    sequence: ProviderSequence,
  }),
  Schema.Struct({
    _tag: Schema.Literal("Turn"),
    kind: Schema.Literals([
      "Update",
      "StartOfTurn",
      "EagerEndOfTurn",
      "TurnResumed",
      "EndOfTurn",
    ]),
    requestId: RequestId,
    sequence: ProviderSequence,
    turnIndex: Schema.Natural,
    transcript: Schema.String,
    endOfTurnConfidence: NumberLike,
    audioWindowEnd: Schema.optionalKey(NumberLike),
    audioEnd: Schema.optionalKey(Schema.Natural),
    recognitionEpoch: Schema.optionalKey(Schema.Natural),
  }),
  Schema.Struct({
    _tag: Schema.Literal("Configured"),
    requestId: RequestId,
    sequence: ProviderSequence,
  }),
  Schema.Struct({
    _tag: Schema.Literal("ConfigurationRejected"),
    requestId: RequestId,
    sequence: ProviderSequence,
    code: Schema.optionalKey(Schema.String),
    description: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    _tag: Schema.Literal("Warning"),
    requestId: RequestId,
    sequence: ProviderSequence,
    code: Schema.String,
    description: Schema.String,
  }),
  Schema.Struct({
    _tag: Schema.Literal("FatalError"),
    sequence: ProviderSequence,
    code: Schema.String,
    description: Schema.String,
  }),
  Schema.Struct({ _tag: Schema.Literal("RepeatRequired") }),
]);
export type FluxEvent = typeof FluxEvent.Type;

export class DeepgramTransportError extends Schema.TaggedError<DeepgramTransportError>()(
  "DeepgramTransportError",
  {
    operation: Schema.Literals(["connect", "receive", "send", "close"]),
    reason: Schema.String,
    sessionLost: Schema.optionalKey(Schema.Boolean),
    closeCode: Schema.optionalKey(Schema.Number),
  },
) {}

export class DeepgramProtocolError extends Schema.TaggedError<DeepgramProtocolError>()(
  "DeepgramProtocolError",
  {
    reason: Schema.String,
  },
) {}

export type DeepgramError = DeepgramTransportError | DeepgramProtocolError;

export interface DeepgramConnection {
  readonly receiveText: Effect.Effect<string, DeepgramTransportError>;
  readonly sendBinary: (
    bytes: Uint8Array,
  ) => Effect.Effect<void, DeepgramTransportError>;
  readonly sendText: (text: string) => Effect.Effect<void, DeepgramTransportError>;
}

export class DeepgramTransport extends Context.Service<
  DeepgramTransport,
  {
    readonly connect: (
      url: string,
      headers: ReadonlyArray<readonly [string, string]>,
    ) => Effect.Effect<DeepgramConnection, DeepgramTransportError, Scope.Scope>;
  }
>()("call-center/DeepgramTransport") {}

export interface FluxOptions {
  readonly apiUrl?: string;
  readonly model?: "flux-general-en" | "flux-general-multi";
  readonly sampleRate?: number;
  readonly languageHints?: ReadonlyArray<string>;
  readonly eotThreshold?: number;
  readonly eotTimeoutMs?: number;
}

export interface FluxConfigureOptions {
  readonly languageHints?: ReadonlyArray<string>;
  readonly keyterms?: ReadonlyArray<string>;
  readonly numerals?: boolean;
  readonly eotThreshold?: number;
  readonly eotTimeoutMs?: number;
}

export interface FluxSession {
  readonly receive: Effect.Effect<FluxEvent, DeepgramError>;
  readonly commitTurn: <A, E>(event: FluxEvent & { _tag: "Turn" }, accept: Effect.Effect<A, E>) => Effect.Effect<A | null, E>;
  readonly sendAudio: (
    bytes: Uint8Array,
  ) => Effect.Effect<void, DeepgramTransportError>;
  readonly forceEndTurn: Effect.Effect<void, DeepgramTransportError>;
  readonly configure: (
    options: FluxConfigureOptions,
  ) => Effect.Effect<void, DeepgramTransportError>;
  readonly closeStream: Effect.Effect<void, DeepgramTransportError>;
}

export const buildFluxUrl = (options: FluxOptions = {}): string => {
  const url = new URL(options.apiUrl ?? "wss://api.deepgram.com/v2/listen");
  url.searchParams.set("model", options.model ?? "flux-general-en");
  url.searchParams.set("encoding", "linear16");
  url.searchParams.set("sample_rate", String(options.sampleRate ?? 16_000));
  for (const hint of options.languageHints ?? []) {
    url.searchParams.append("language_hint", hint);
  }
  if (options.eotThreshold !== undefined) {
    url.searchParams.set("eot_threshold", String(options.eotThreshold));
  }
  if (options.eotTimeoutMs !== undefined) {
    url.searchParams.set("eot_timeout_ms", String(options.eotTimeoutMs));
  }
  return url.toString();
};

export const deepgramAuthHeaders = (
  apiKey: string,
): ReadonlyArray<readonly [string, string]> => [["Authorization", `Token ${apiKey}`]];

export interface DeepgramSocketFailureReason {
  readonly _tag: string;
  readonly code?: number;
  readonly closeReason?: string;
}

export const deepgramSocketFailure = (
  operation: "connect" | "receive" | "send" | "close",
  socketReason: DeepgramSocketFailureReason,
  sessionLost = false,
): DeepgramTransportError =>
  new DeepgramTransportError({
    operation,
    reason: `WebSocket ${socketReason._tag}`,
    ...(sessionLost ? { sessionLost: true } : {}),
    ...(socketReason._tag === "SocketCloseError" && socketReason.code !== undefined
      ? { closeCode: socketReason.code }
      : {}),
  });

const mapServerMessage = (message: DeepgramServerMessage): FluxEvent => {
  switch (message.type) {
    case "Connected":
      return {
        _tag: "Connected",
        requestId: message.request_id,
        sequence: message.sequence_id,
      };
    case "TurnInfo":
      return {
        _tag: "Turn",
        kind: message.event,
        requestId: message.request_id,
        sequence: message.sequence_id,
        turnIndex: message.turn_index,
        transcript: message.transcript,
        endOfTurnConfidence: message.end_of_turn_confidence,
        audioWindowEnd: message.audio_window_end,
      };
    case "ConfigureSuccess":
      return {
        _tag: "Configured",
        requestId: message.request_id,
        sequence: message.sequence_id,
      };
    case "ConfigureFailure":
      return {
        _tag: "ConfigurationRejected",
        requestId: message.request_id,
        sequence: message.sequence_id,
        ...(message.code === undefined ? {} : { code: message.code }),
        ...(message.description === undefined
          ? {}
          : { description: message.description }),
      };
    case "Warning":
      return {
        _tag: "Warning",
        requestId: message.request_id,
        sequence: message.sequence_id,
        code: message.code,
        description: message.description,
      };
    case "Error":
      return {
        _tag: "FatalError",
        sequence: message.sequence_id,
        code: message.code,
        description: message.description,
      };
  }
};

export const decodeFluxMessage = Effect.fnUntraced(function*(
  text: string,
): Effect.fn.Return<FluxEvent, DeepgramProtocolError> {
  const json = yield* Effect.try({
    try: () => JSON.parse(text) as unknown,
    catch: () =>
      new DeepgramProtocolError({ reason: "Deepgram sent invalid JSON" }),
  });
  const message = yield* Schema.decodeUnknownEffect(DeepgramServerMessage)(json).pipe(
    Effect.mapError(
      () =>
        new DeepgramProtocolError({
          reason: "Deepgram sent a malformed or unsupported message",
        }),
    ),
  );
  return mapServerMessage(message);
});

const configureMessage = (options: FluxConfigureOptions): string => {
  const thresholds: Record<string, number> = {};
  if (options.eotThreshold !== undefined) thresholds.eot_threshold = options.eotThreshold;
  if (options.eotTimeoutMs !== undefined) thresholds.eot_timeout_ms = options.eotTimeoutMs;

  return JSON.stringify({
    type: "Configure",
    ...(Object.keys(thresholds).length > 0 ? { thresholds } : {}),
    ...(options.languageHints === undefined
      ? {}
      : { language_hints: options.languageHints }),
    ...(options.keyterms === undefined ? {} : { keyterms: options.keyterms }),
    ...(options.numerals === undefined ? {} : { numerals: options.numerals }),
  });
};

export class DeepgramFlux extends Context.Service<
  DeepgramFlux,
  {
    readonly open: (
      apiKey: string,
      options?: FluxOptions,
    ) => Effect.Effect<FluxSession, DeepgramTransportError, Scope.Scope>;
  }
>()("call-center/DeepgramFlux") {
  static readonly layer = Layer.effect(
    DeepgramFlux,
    Effect.gen(function* () {
      const transport = yield* DeepgramTransport;
      const open = Effect.fnUntraced(function*(
        apiKey: string,
        options: FluxOptions = {},
      ) {
        const parent = yield* Effect.scope;
        const sender = yield* Semaphore.make(1);
        const acceptance = yield* Semaphore.make(1);
        const bytesPerSecond = (options.sampleRate ?? 16000) * 2;
        const limit = bytesPerSecond * 10;
        const catchingUp = yield* Ref.make(false);
        const audio = yield* Ref.make({ bytes: new Uint8Array(), start: 0, end: 0, committed: 0 });
        const suppressed = yield* Ref.make(false);
        const repeatRequired = yield* Ref.make(false);
        const configuration = yield* Ref.make<string | null>(null);
        const connect = Effect.fnUntraced(function*(epoch: number) {
          const scope = yield* Scope.fork(parent);
          const retired = yield* Deferred.make<void>();
          const connection = yield* transport.connect(
            buildFluxUrl(options), deepgramAuthHeaders(apiKey),
          ).pipe(Scope.provide(scope), Effect.onError(() => Scope.close(scope, Exit.void)));
          return { connection, scope, retired, origin: 0, epoch };
        });
        type PhysicalSession = { connection: DeepgramConnection; scope: Scope.Closeable; retired: Deferred.Deferred<void>; origin: number; epoch: number };
        // null is reconstructing; closed is terminal and can never be overwritten.
        const current = yield* Ref.make<PhysicalSession | null | "closed">(yield* connect(0));
        const writePcm = Effect.fnUntraced(function*(session: PhysicalSession, bytes: Uint8Array) {
          for (let offset = 0; offset < bytes.length; offset += 2560) {
            if ((yield* Ref.get(current)) === "closed") return;
            const chunk = bytes.subarray(offset, offset + 2560);
            yield* session.connection.sendBinary(chunk);
            if (yield* Ref.get(catchingUp)) {
              yield* Effect.sleep(chunk.length * 1000 / bytesPerSecond / 1.25);
            }
          }
        });
        // Only the sender permit replaces a physical session or writes PCM. Receives never hold it.
        const reconstruct = Effect.fnUntraced(function*(lost: PhysicalSession) {
          let physical = lost;
          // Bound consecutive losses during one reconstruction, not the call's lifetime.
          for (let attempt = 0; attempt < 3; attempt++) {
            const retired = yield* acceptance.withPermit(Ref.modify(current, (owned) =>
              owned === physical ? [true, null] as const : [false, owned] as const,
            ));
            if (!retired) return;
            yield* Deferred.succeed(physical.retired, undefined);
            yield* Scope.close(physical.scope, Exit.void);
            const connected = yield* connect(physical.epoch + 1);
            const retained = yield* Ref.get(audio);
            const complete = retained.start <= retained.committed;
            physical = { ...connected, origin: complete ? retained.committed : retained.end };
            const installed = yield* Ref.modify(current, (owned) => owned === null
              ? [true, physical] as const : [false, owned] as const);
            if (!installed) {
              yield* Scope.close(physical.scope, Exit.void);
              return;
            }
            if (!complete) {
              yield* Ref.set(audio, { bytes: new Uint8Array(), start: retained.end, end: retained.end, committed: retained.end });
              yield* Ref.set(suppressed, true);
              yield* Ref.set(repeatRequired, true);
            }
            yield* Ref.set(catchingUp, true);
            const restored = yield* Effect.gen(function*() {
              const config = yield* Ref.get(configuration);
              if (config !== null) yield* physical.connection.sendText(config);
              if (complete) yield* writePcm(physical, retained.bytes);
            }).pipe(Effect.result);
            if (restored._tag === "Success") {
              yield* Effect.logInfo("Deepgram recognition session reconstructed");
              return;
            }
            if (!restored.failure.sessionLost || attempt === 2) return yield* restored.failure;
          }
        });
        const receive: FluxSession["receive"] = Effect.suspend(() => Effect.gen(function*() {
          const session = yield* sender.withPermit(Ref.get(current));
          if (session === null || session === "closed") return yield* Effect.interrupt;
          if (yield* Ref.getAndSet(repeatRequired, false)) return { _tag: "RepeatRequired" } as const;
          const received = yield* Effect.raceFirst(
            session.connection.receiveText.pipe(Effect.result),
            Deferred.await(session.retired).pipe(Effect.as(null)),
          );
          if (received === null) return yield* receive;
          if ((yield* Ref.get(current)) !== session) return yield* receive;
          if (received._tag === "Failure") {
            if (!received.failure.sessionLost) return yield* received.failure;
            yield* sender.withPermit(reconstruct(session));
            return yield* receive;
          }
          const event = yield* decodeFluxMessage(received.success);
          if (event._tag !== "Turn") return event;
          const end = Number(event.audioWindowEnd);
          if (!Number.isFinite(end) || end < 0) {
            return yield* new DeepgramProtocolError({ reason: "Deepgram returned an invalid audio boundary" });
          }
          const retained = yield* Ref.get(audio);
          const audioEnd = Math.min(retained.end, session.origin + Math.round(end * bytesPerSecond / 2) * 2);
          if (audioEnd <= retained.committed && retained.committed > 0) return yield* receive;
          if (event.kind === "EndOfTurn") {
            if (audioEnd <= retained.committed) return yield* receive;
            if (yield* Ref.getAndSet(suppressed, false)) {
              yield* Ref.update(audio, (latest) => ({
                ...latest, committed: audioEnd, start: Math.max(latest.start, audioEnd),
                bytes: latest.bytes.slice(Math.max(0, audioEnd - latest.start)),
              }));
              return yield* receive;
            }
          } else if (yield* Ref.get(suppressed)) return yield* receive;
          return { ...event, audioEnd, recognitionEpoch: session.epoch };
        }));
        const sendAudio = Effect.fnUntraced(function*(bytes: Uint8Array) {
          yield* sender.withPermit(Effect.gen(function*() {
            if ((yield* Ref.get(current)) === "closed") return;
            yield* Ref.update(audio, (previous) => {
              const joined = new Uint8Array(previous.bytes.length + bytes.length);
              joined.set(previous.bytes);
              joined.set(bytes, previous.bytes.length);
              const kept = joined.slice(Math.max(0, joined.length - limit));
              return { ...previous, bytes: kept, end: previous.end + bytes.length, start: previous.end + bytes.length - kept.length };
            });
            const session = yield* Ref.get(current);
            if (session === null || session === "closed") return;
            yield* writePcm(session, bytes).pipe(Effect.catch((error) =>
              error.sessionLost ? reconstruct(session) : Effect.fail(error),
            ));
          }));
        });

        const session: FluxSession = {
          receive,
          commitTurn: (event, accept) => acceptance.withPermit(Effect.gen(function*() {
            const physical = yield* Ref.get(current);
            if (physical === null || physical === "closed" || physical.epoch !== event.recognitionEpoch) return null;
            const value = yield* accept;
            yield* Ref.update(audio, (latest) => {
              const committed = Math.max(latest.committed, event.audioEnd ?? latest.committed);
              return { ...latest, committed, start: Math.max(latest.start, committed), bytes: latest.bytes.slice(Math.max(0, committed - latest.start)) };
            });
            return value;
          })),
          sendAudio,
          forceEndTurn: sender.withPermit(Ref.get(current).pipe(Effect.flatMap((physical) =>
            physical === null || physical === "closed" ? Effect.void : physical.connection.sendText(JSON.stringify({ type: "ForceEndTurn" }))))),
          configure: (configureOptions) =>
            sender.withPermit(Effect.gen(function*() {
              const config = configureMessage(configureOptions);
              yield* Ref.set(configuration, config);
              const physical = yield* Ref.get(current);
              if (physical !== null && physical !== "closed") yield* physical.connection.sendText(config);
            })),
          closeStream: Effect.gen(function*() {
            const physical = yield* Ref.getAndSet(current, "closed");
            if (physical !== null && physical !== "closed") {
              yield* Deferred.succeed(physical.retired, undefined);
              yield* Scope.close(physical.scope, Exit.void);
            }
          }),
        };
        return session;
      });

      return DeepgramFlux.of({ open });
    }),
  );
}
