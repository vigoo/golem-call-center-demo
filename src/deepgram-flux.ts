import { Context, Effect, Layer, Schema, Scope } from "effect";

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
]);
export type FluxEvent = typeof FluxEvent.Type;

export class DeepgramTransportError extends Schema.TaggedError<DeepgramTransportError>()(
  "DeepgramTransportError",
  {
    operation: Schema.Literals(["connect", "receive", "send", "close"]),
    reason: Schema.String,
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
): DeepgramTransportError =>
  new DeepgramTransportError({
    operation,
    reason: `WebSocket ${socketReason._tag}`,
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
        const connection = yield* transport.connect(
          buildFluxUrl(options),
          deepgramAuthHeaders(apiKey),
        );

        const session: FluxSession = {
          receive: connection.receiveText.pipe(Effect.flatMap(decodeFluxMessage)),
          sendAudio: connection.sendBinary,
          forceEndTurn: connection.sendText(JSON.stringify({ type: "ForceEndTurn" })),
          configure: (configureOptions) =>
            connection.sendText(configureMessage(configureOptions)),
          closeStream: connection.sendText(JSON.stringify({ type: "CloseStream" })),
        };
        return session;
      });

      return DeepgramFlux.of({ open });
    }),
  );
}
