import { Context, Effect, Exit, Layer, Ref, Schema, Scope, Semaphore } from "effect";
import type { ResponseId } from "./call-protocol.js";

const ContextId = Schema.NonEmptyString;

const Alignment = Schema.Struct({
  charStartTimesMs: Schema.optionalKey(Schema.Array(Schema.Int)),
  charDurationsMs: Schema.optionalKey(Schema.Array(Schema.Int)),
  chars: Schema.optionalKey(Schema.Array(Schema.String)),
});

export const ElevenLabsAudioOutput = Schema.Struct({
  audio: Schema.Uint8ArrayFromBase64,
  contextId: ContextId,
  isFinal: Schema.optionalKey(Schema.NullOr(Schema.Literal(false))),
  normalizedAlignment: Schema.optionalKey(Schema.NullOr(Alignment)),
  alignment: Schema.optionalKey(Schema.NullOr(Alignment)),
});

export const ElevenLabsFinalOutput = Schema.Struct({
  isFinal: Schema.Literal(true),
  contextId: ContextId,
});

export const ElevenLabsServerMessage = Schema.Union([
  ElevenLabsAudioOutput,
  ElevenLabsFinalOutput,
]);
export type ElevenLabsServerMessage = typeof ElevenLabsServerMessage.Type;

export type ElevenLabsEvent =
  | {
      readonly _tag: "Audio";
      readonly responseId: ResponseId;
      readonly payload: Uint8Array;
    }
  | { readonly _tag: "Final"; readonly responseId: ResponseId };

export class ElevenLabsTransportError extends Schema.TaggedError<ElevenLabsTransportError>()(
  "ElevenLabsTransportError",
  {
    operation: Schema.Literals(["connect", "receive", "send", "close"]),
    reason: Schema.String,
    closeCode: Schema.optionalKey(Schema.Number),
  },
) {}

export class ElevenLabsProtocolError extends Schema.TaggedError<ElevenLabsProtocolError>()(
  "ElevenLabsProtocolError",
  { reason: Schema.String },
) {}

export class ElevenLabsContextError extends Schema.TaggedError<ElevenLabsContextError>()(
  "ElevenLabsContextError",
  {
    operation: Schema.Literals(["start", "send", "finish"]),
    responseId: Schema.String,
    reason: Schema.Literals(["already-started", "not-open", "socket-closed"]),
  },
) {}

export type ElevenLabsError =
  | ElevenLabsTransportError
  | ElevenLabsProtocolError
  | ElevenLabsContextError;

export interface ElevenLabsConnection {
  readonly receiveText: Effect.Effect<string, ElevenLabsTransportError>;
  readonly sendText: (text: string) => Effect.Effect<void, ElevenLabsTransportError>;
}

export class ElevenLabsTransport extends Context.Service<
  ElevenLabsTransport,
  {
    readonly connect: (
      url: string,
      headers: ReadonlyArray<readonly [string, string]>,
    ) => Effect.Effect<ElevenLabsConnection, ElevenLabsTransportError, Scope.Scope>;
  }
>()("call-center/ElevenLabsTransport") {}

export interface ElevenLabsVoiceSettings {
  readonly stability: number;
  readonly similarityBoost: number;
  readonly speed?: number;
  readonly style?: number;
  readonly useSpeakerBoost?: boolean;
}

export interface ElevenLabsOptions {
  readonly apiUrl?: string;
  readonly voiceId: string;
  readonly modelId: string;
  readonly outputFormat: string;
  readonly inactivityTimeoutSeconds?: number;
  readonly voiceSettings?: ElevenLabsVoiceSettings;
  readonly chunkLengthSchedule?: ReadonlyArray<number>;
}

export interface ElevenLabsSession {
  readonly startContext: (
    responseId: ResponseId,
  ) => Effect.Effect<void, ElevenLabsContextError | ElevenLabsTransportError>;
  readonly sendText: (
    responseId: ResponseId,
    text: string,
  ) => Effect.Effect<void, ElevenLabsContextError | ElevenLabsTransportError>;
  readonly finishContext: (
    responseId: ResponseId,
  ) => Effect.Effect<void, ElevenLabsContextError | ElevenLabsTransportError>;
  readonly closeContext: (
    responseId: ResponseId,
  ) => Effect.Effect<void, ElevenLabsTransportError>;
  readonly receive: Effect.Effect<ElevenLabsEvent, ElevenLabsProtocolError | ElevenLabsTransportError>;
  readonly closeSocket: Effect.Effect<void, ElevenLabsTransportError>;
}

export interface ElevenLabsSocketFailureReason {
  readonly _tag: string;
  readonly code?: number;
  readonly closeReason?: string;
}

export const elevenLabsSocketFailure = (
  operation: "connect" | "receive" | "send" | "close",
  socketReason: ElevenLabsSocketFailureReason,
): ElevenLabsTransportError =>
  new ElevenLabsTransportError({
    operation,
    reason: `WebSocket ${socketReason._tag}`,
    ...(socketReason._tag === "SocketCloseError" && socketReason.code !== undefined
      ? { closeCode: socketReason.code }
      : {}),
  });

export const buildElevenLabsUrl = (options: ElevenLabsOptions): string => {
  const url = new URL(options.apiUrl ?? "wss://api.elevenlabs.io");
  url.pathname = `/v1/text-to-speech/${encodeURIComponent(options.voiceId)}/multi-stream-input`;
  url.searchParams.set("model_id", options.modelId);
  url.searchParams.set("output_format", options.outputFormat);
  if (options.inactivityTimeoutSeconds !== undefined) {
    url.searchParams.set("inactivity_timeout", String(options.inactivityTimeoutSeconds));
  }
  return url.toString();
};

export const elevenLabsAuthHeaders = (
  apiKey: string,
): ReadonlyArray<readonly [string, string]> => [["xi-api-key", apiKey]];

export const decodeElevenLabsMessage = Effect.fnUntraced(function*(
  text: string,
): Effect.fn.Return<ElevenLabsServerMessage, ElevenLabsProtocolError> {
  const json = yield* Effect.try({
    try: () => JSON.parse(text) as unknown,
    catch: () => new ElevenLabsProtocolError({ reason: "ElevenLabs sent invalid JSON" }),
  });
  return yield* Schema.decodeUnknownEffect(ElevenLabsServerMessage)(json).pipe(
    Effect.mapError(
      () =>
        new ElevenLabsProtocolError({
          reason: "ElevenLabs sent a malformed or unsupported message",
        }),
    ),
  );
});

type ContextState = "open" | "finishing" | "closed" | "tombstone";

const contextError = (
  operation: "start" | "send" | "finish",
  responseId: ResponseId,
  reason: "already-started" | "not-open" | "socket-closed",
) => new ElevenLabsContextError({ operation, responseId, reason });

const contextStartMessage = (responseId: ResponseId, options: ElevenLabsOptions): string =>
  JSON.stringify({
    context_id: responseId,
    text: " ",
    ...(options.voiceSettings === undefined
      ? {}
      : {
          voice_settings: {
            stability: options.voiceSettings.stability,
            similarity_boost: options.voiceSettings.similarityBoost,
            ...(options.voiceSettings.speed === undefined
              ? {}
              : { speed: options.voiceSettings.speed }),
            ...(options.voiceSettings.style === undefined
              ? {}
              : { style: options.voiceSettings.style }),
            ...(options.voiceSettings.useSpeakerBoost === undefined
              ? {}
              : { use_speaker_boost: options.voiceSettings.useSpeakerBoost }),
          },
        }),
    ...(options.chunkLengthSchedule === undefined
      ? {}
      : {
          generation_config: {
            chunk_length_schedule: options.chunkLengthSchedule,
          },
        }),
  });

export class ElevenLabsTts extends Context.Service<
  ElevenLabsTts,
  {
    readonly open: (
      apiKey: string,
      options: ElevenLabsOptions,
    ) => Effect.Effect<ElevenLabsSession, ElevenLabsTransportError, Scope.Scope>;
  }
>()("call-center/ElevenLabsTts") {
  static readonly layer = Layer.effect(
    ElevenLabsTts,
    Effect.gen(function* () {
      const transport = yield* ElevenLabsTransport;

      return ElevenLabsTts.of({
        open: (apiKey, options) =>
          Effect.gen(function* () {
            const connection = yield* transport.connect(
              buildElevenLabsUrl(options),
              elevenLabsAuthHeaders(apiKey),
            );
            const contexts = yield* Ref.make<ReadonlyMap<ResponseId, ContextState>>(new Map());
            const wireClosedContexts = yield* Ref.make<ReadonlySet<ResponseId>>(new Set());
            const socketState = yield* Ref.make<"open" | "closing" | "closed">("open");
            const commandLock = yield* Semaphore.make(1);

            const startContextUnlocked = Effect.fn(
              "ElevenLabsTts.startContext",
            )(function*(
              responseId,
            ): Effect.fn.Return<void, ElevenLabsContextError | ElevenLabsTransportError> {
              if ((yield* Ref.get(socketState)) !== "open") {
                return yield* Effect.fail(contextError("start", responseId, "socket-closed"));
              }
              const started = yield* Ref.modify(contexts, (current) => {
                if (current.has(responseId)) return [false, current] as const;
                return [true, new Map(current).set(responseId, "open")] as const;
              });
              if (!started) {
                return yield* Effect.fail(
                  contextError("start", responseId, "already-started"),
                );
              }
              yield* connection.sendText(contextStartMessage(responseId, options)).pipe(
                Effect.onExit((exit) =>
                  Exit.isFailure(exit)
                    ? Ref.update(contexts, (current) => {
                        if (current.get(responseId) !== "open") return current;
                        const updated = new Map(current);
                        updated.delete(responseId);
                        return updated;
                      })
                    : Effect.void,
                ),
              );
            });
            const startContext: ElevenLabsSession["startContext"] = (responseId) =>
              commandLock.withPermit(startContextUnlocked(responseId));

            const sendTextUnlocked = Effect.fn(
              "ElevenLabsTts.sendText",
            )(function*(
              responseId,
              text,
            ): Effect.fn.Return<void, ElevenLabsContextError | ElevenLabsTransportError> {
              if ((yield* Ref.get(socketState)) !== "open") {
                return yield* Effect.fail(contextError("send", responseId, "socket-closed"));
              }
              const current = yield* Ref.get(contexts);
              if (current.get(responseId) !== "open") {
                return yield* Effect.fail(contextError("send", responseId, "not-open"));
              }
              yield* connection.sendText(JSON.stringify({ context_id: responseId, text }));
            });
            const sendText: ElevenLabsSession["sendText"] = (responseId, text) =>
              commandLock.withPermit(sendTextUnlocked(responseId, text));

            const finishContextUnlocked = Effect.fn(
              "ElevenLabsTts.finishContext",
            )(function*(
              responseId,
            ): Effect.fn.Return<void, ElevenLabsContextError | ElevenLabsTransportError> {
              if ((yield* Ref.get(socketState)) !== "open") {
                return yield* Effect.fail(contextError("finish", responseId, "socket-closed"));
              }
              const startedFinishing = yield* Ref.modify(contexts, (current) => {
                if (current.get(responseId) !== "open") return [false, current] as const;
                return [
                  true,
                  new Map(current).set(responseId, "finishing" as const),
                ] as const;
              });
              if (!startedFinishing) {
                return yield* Effect.fail(contextError("finish", responseId, "not-open"));
              }
              yield* Effect.uninterruptible(
                connection
                  .sendText(JSON.stringify({ context_id: responseId, flush: true }))
                  .pipe(
                    Effect.andThen(
                      connection.sendText(
                        JSON.stringify({ context_id: responseId, close_context: true }),
                      ),
                    ),
                    Effect.andThen(
                      Ref.update(wireClosedContexts, (current) =>
                        new Set(current).add(responseId),
                      ),
                    ),
                  ),
              );
            });
            const finishContext: ElevenLabsSession["finishContext"] = (responseId) =>
              commandLock.withPermit(finishContextUnlocked(responseId));

            const closeContext: ElevenLabsSession["closeContext"] = (responseId) =>
              Effect.uninterruptible(
                Ref.modify(contexts, (current) => {
                  const state = current.get(responseId);
                  return [
                    state !== undefined && state !== "tombstone",
                    new Map(current).set(
                      responseId,
                      state === undefined || state === "tombstone" ? "tombstone" : "closed",
                    ),
                  ] as const;
                }).pipe(
                  Effect.flatMap((wireCloseNeeded) =>
                    wireCloseNeeded
                      ? commandLock.withPermit(
                          Effect.gen(function* () {
                            if ((yield* Ref.get(socketState)) !== "open") return;
                            if ((yield* Ref.get(wireClosedContexts)).has(responseId)) return;
                            yield* connection.sendText(
                              JSON.stringify({ context_id: responseId, close_context: true }),
                            );
                            yield* Ref.update(wireClosedContexts, (current) =>
                              new Set(current).add(responseId),
                            );
                          }),
                        )
                      : Effect.void,
                  ),
                ),
              );

            const receive: ElevenLabsSession["receive"] = Effect.suspend(() =>
              connection.receiveText.pipe(
                Effect.flatMap(decodeElevenLabsMessage),
                Effect.flatMap((message) =>
                  Effect.gen(function* () {
                    const current = yield* Ref.get(contexts);
                    const responseId = message.contextId as ResponseId;
                    const state = current.get(responseId);
                    if (
                      state === undefined ||
                      state === "closed" ||
                      state === "tombstone" ||
                      (yield* Ref.get(socketState)) !== "open"
                    ) {
                      return yield* receive;
                    }
                    if (message.isFinal === true) {
                      yield* Ref.update(contexts, (latest) =>
                        new Map(latest).set(responseId, "closed"),
                      );
                      return { _tag: "Final", responseId } satisfies ElevenLabsEvent;
                    }
                    return {
                      _tag: "Audio",
                      responseId,
                      payload: message.audio,
                    } satisfies ElevenLabsEvent;
                  }),
                ),
              ),
            );

            const closeSocket: ElevenLabsSession["closeSocket"] = Effect.uninterruptible(
              Effect.gen(function* () {
                yield* Ref.update(socketState, (state) =>
                  state === "open" ? "closing" : state,
                );
                yield* Ref.update(contexts, (current) =>
                  new Map(Array.from(current.keys(), (id) => [id, "closed" as const])),
                );
                yield* commandLock.withPermit(
                  Effect.gen(function* () {
                    if ((yield* Ref.get(socketState)) === "closed") return;
                    yield* Ref.update(contexts, (current) =>
                      new Map(Array.from(current.keys(), (id) => [id, "closed" as const])),
                    );
                    yield* connection.sendText(JSON.stringify({ close_socket: true }));
                    yield* Ref.set(socketState, "closed");
                  }),
                );
              }),
            );

            yield* Effect.addFinalizer(() => closeSocket.pipe(Effect.ignore));

            return {
              startContext,
              sendText,
              finishContext,
              closeContext,
              receive,
              closeSocket,
            } satisfies ElevenLabsSession;
          }),
      });
    }),
  );
}
