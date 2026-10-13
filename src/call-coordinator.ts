import {
  Cause,
  Deferred,
  Effect,
  Fiber,
  Queue,
  Redacted,
  Ref,
  Result,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import {
  type CallInput,
  type CallOutput,
  type CallTransitionError,
  type ProviderStage,
  type ResponseId,
  type SavedCallState,
  TurnId,
} from "./call-protocol.js";
import {
  acknowledgePlayback,
  endCall,
  finishResponse,
  interruptResponse,
  recordAudioChunk,
  startCall,
  startResponse,
} from "./call-state.js";
import { DeepgramFlux, type FluxEvent, type FluxOptions } from "./deepgram-flux.js";
import {
  type ElevenLabsEvent,
  ElevenLabsTts,
  type ElevenLabsOptions,
} from "./elevenlabs-tts.js";
import { ResponseGenerator } from "./response-generation.js";

export interface CallCoordinatorOptions {
  readonly initialState: SavedCallState;
  readonly deepgramApiKey: Redacted.Redacted<string>;
  readonly deepgram?: FluxOptions;
  readonly elevenLabsApiKey: Redacted.Redacted<string>;
  readonly elevenLabs: ElevenLabsOptions;
  readonly systemPrompt: string;
  readonly outputQueueCapacity?: number;
  readonly onStateChange?: (state: SavedCallState) => Effect.Effect<void>;
}

interface CurrentGeneration {
  readonly responseId: ResponseId;
  readonly fiber: Fiber.Fiber<void>;
}

interface ResponseFailure {
  readonly stage: "openai" | "elevenlabs";
  readonly reason: string;
}

interface StartedGeneration {
  readonly responseId: ResponseId;
  readonly turns: SavedCallState["turns"];
}

type PendingOutput =
  | CallOutput
  | { readonly _tag: "ttsFinal"; readonly responseId: ResponseId }
  | { readonly _tag: "controlBatch"; readonly events: ReadonlyArray<CallOutput> };

const failureOutput = (
  stage: ProviderStage,
  reason: string,
  recoverable: boolean,
): CallOutput => ({ _tag: "failure", stage, reason, recoverable });

const coordinatorFailure = (error: CallTransitionError): CallOutput =>
  failureOutput("coordinator", `Call state rejected ${error._tag}`, true);

const partialTurnId = (state: SavedCallState) =>
  Schema.decodeUnknownSync(TurnId)(`turn-${state.nextTurnNumber}`);

export const coordinateCall = <E>(
  input: Stream.Stream<CallInput, E>,
  options: CallCoordinatorOptions,
): Stream.Stream<CallOutput, never, DeepgramFlux | ElevenLabsTts | ResponseGenerator> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const deepgram = yield* DeepgramFlux;
      const tts = yield* ElevenLabsTts;
      const generator = yield* ResponseGenerator;
      const output = yield* Queue.bounded<PendingOutput, Cause.Done>(
        options.outputQueueCapacity ?? 16,
      );
      const controlPublications = yield* Queue.bounded<ReadonlyArray<CallOutput>, Cause.Done>(1);
      const stateLock = yield* Semaphore.make(1);
      const publicationLock = yield* Semaphore.make(1);
      const state = yield* Ref.make(options.initialState);
      const audioInput = yield* Queue.bounded<Uint8Array, Cause.Done>(128);
      const queuedAudioBytes = yield* Ref.make(0);
      const currentGeneration = yield* Ref.make<CurrentGeneration | null>(null);
      const completedText = yield* Ref.make<ReadonlyMap<ResponseId, string>>(new Map());
      const nextAudioSequence = yield* Ref.make<ReadonlyMap<ResponseId, number>>(new Map());
      const clearedPlayback = yield* Ref.make<ReadonlySet<ResponseId>>(new Set());
      const interruptConfirmationPending = yield* Ref.make(false);
      const shuttingDown = yield* Ref.make(false);
      const onStateChange = options.onStateChange ?? (() => Effect.void);

      const publicationFiber = yield* Stream.fromQueue(controlPublications).pipe(
        Stream.runForEach((events) =>
          Queue.offer(output, {
            _tag: "controlBatch",
            events,
          }).pipe(Effect.asVoid),
        ),
        Effect.ensuring(Queue.end(output)),
        Effect.forkScoped,
      );

      const enqueue = (event: CallOutput) => Queue.offer(output, event).pipe(Effect.asVoid);

      const saveState = (next: SavedCallState) =>
        Ref.set(state, next).pipe(Effect.andThen(onStateChange(next)));

      const updateState = <A>(
        transition: (current: SavedCallState) => Result.Result<readonly [SavedCallState, A], CallTransitionError>,
      ): Effect.Effect<Result.Result<A, CallTransitionError>> =>
        stateLock.withPermit(
          Effect.gen(function* () {
            const result = transition(yield* Ref.get(state));
            if (Result.isFailure(result)) return Result.fail(result.failure);
            const [next, value] = result.success;
            yield* saveState(next);
            return Result.succeed(value);
          }),
        );

      const activateCall = stateLock.withPermit(
        Effect.gen(function* () {
          const current = yield* Ref.get(state);
          if (current.phase === "idle") {
            const started = startCall(current);
            if (Result.isSuccess(started)) yield* saveState(started.success);
          } else if (current.phase === "responding") {
            const interrupted = interruptResponse(current);
            if (Result.isSuccess(interrupted)) yield* saveState(interrupted.success.state);
          }
        }),
      );
      yield* activateCall;

      const deepgramSession = yield* deepgram
        .open(Redacted.value(options.deepgramApiKey), options.deepgram)
        .pipe(
          Effect.mapError(
            () =>
              ({
                stage: "deepgram",
                reason: "Deepgram connection failed",
              }) as const,
          ),
        );
      const ttsSession = yield* tts
        .open(Redacted.value(options.elevenLabsApiKey), options.elevenLabs)
        .pipe(
          Effect.onError(() => deepgramSession.closeStream.pipe(Effect.ignore)),
          Effect.mapError(
            () =>
              ({
                stage: "elevenlabs",
                reason: "ElevenLabs connection failed",
              }) as const,
          ),
        );

      const invalidateResponse = Effect.fnUntraced(function*(
        expectedResponseId: ResponseId | undefined,
        interruptFiber: boolean,
      ) {
        const invalidated = yield* stateLock.withPermit(
          Effect.gen(function* () {
            const result = interruptResponse(yield* Ref.get(state), expectedResponseId);
            if (Result.isFailure(result)) {
              return { _tag: "Rejected", error: result.failure } as const;
            }
            yield* saveState(result.success.state);
            const responseId = result.success.responseId;
            const cleared = yield* Ref.get(clearedPlayback);
            const playbackResponseIds = result.success.state.responses
              .filter(
                (response) =>
                  response.emittedThrough !== null &&
                  response.playedThrough !== response.emittedThrough &&
                  !cleared.has(response.responseId) &&
                  response.responseId !== responseId,
              )
              .map((response) => response.responseId);
            if (responseId === null) {
              if (playbackResponseIds.length === 0) {
                return { _tag: "NoActiveResponse" } as const;
              }
              yield* Ref.update(clearedPlayback, (current) =>
                new Set([...current, ...playbackResponseIds]),
              );
              return {
                _tag: "PlaybackOnly",
                responseIds: playbackResponseIds,
              } as const;
            }
            const fiber = yield* Ref.modify(currentGeneration, (current) =>
              current?.responseId === responseId
                ? [current.fiber, null] as const
                : [null, current] as const,
            );
            yield* Ref.update(completedText, (current) => {
              const updated = new Map(current);
              updated.delete(responseId);
              return updated;
            });
            yield* Ref.update(clearedPlayback, (current) =>
              new Set([...current, responseId, ...playbackResponseIds]),
            );
            return {
              _tag: "Invalidated",
              responseId,
              responseIds: [responseId, ...playbackResponseIds],
              fiber,
            } as const;
          }),
        );
        if (invalidated._tag === "PlaybackOnly") {
          return invalidated;
        }
        if (invalidated._tag !== "Invalidated") return invalidated;

        const { responseId, fiber } = invalidated;
        if (interruptFiber && fiber !== null) yield* Fiber.interrupt(fiber);
        yield* ttsSession.closeContext(responseId).pipe(Effect.ignore);
        return {
          _tag: "Invalidated",
          responseId,
          responseIds: invalidated.responseIds,
        } as const;
      });

      const invalidateAndPublish = Effect.fnUntraced(function*(
        expectedResponseId: ResponseId | undefined,
        interruptFiber: boolean,
        failure?: CallOutput,
        confirmInterrupt = false,
      ) {
        return yield* publicationLock.withPermit(
          Effect.gen(function* () {
            const invalidated = yield* invalidateResponse(
              expectedResponseId,
              interruptFiber,
            );
            const confirmation: ReadonlyArray<CallOutput> = confirmInterrupt &&
              !(yield* Ref.getAndSet(interruptConfirmationPending, true))
              ? [{ _tag: "interruptConfirmed" }] : [];
            if (
              invalidated._tag !== "Invalidated" &&
              invalidated._tag !== "PlaybackOnly"
            ) {
              if (confirmation.length) yield* Queue.offer(controlPublications, confirmation);
              return invalidated;
            }
            yield* Queue.offer(controlPublications, [
              ...invalidated.responseIds.map(
                (responseId): CallOutput => ({
                  _tag: "clearPlayback",
                  responseId,
                }),
              ),
              ...(failure === undefined ? [] : [failure]),
              ...confirmation,
            ]);
            return invalidated;
          }),
        );
      });

      const failResponse = Effect.fnUntraced(function*(
        responseId: ResponseId,
        failure: ResponseFailure,
      ) {
        yield* invalidateAndPublish(
          responseId,
          false,
          failureOutput(failure.stage, failure.reason, true),
        );
      });

      const runGeneration = Effect.fnUntraced(function*(started: StartedGeneration) {
        const { responseId } = started;
        const isCurrent = stateLock.withPermit(
          Ref.get(state).pipe(Effect.map((current) => current.activeResponseId === responseId)),
        );
        if (!(yield* isCurrent)) return;

        const opened = yield* ttsSession.startContext(responseId).pipe(
          Effect.mapError(
            (): ResponseFailure => ({
              stage: "elevenlabs",
              reason: "Speech synthesis context failed to start",
            }),
          ),
          Effect.result,
        );
        if (Result.isFailure(opened)) {
          yield* failResponse(responseId, opened.failure);
          return;
        }
        if (!(yield* isCurrent)) {
          yield* ttsSession.closeContext(responseId).pipe(Effect.ignore);
          return;
        }

        let text = "";
        let completed = false;
        const generated = yield* generator
          .stream(options.systemPrompt, started.turns)
          .pipe(
            Stream.mapError(
              (): ResponseFailure => ({
                stage: "openai",
                reason: "Language model generation failed",
              }),
            ),
            Stream.runForEach((event) =>
              Effect.gen(function* () {
                if (!(yield* isCurrent)) return yield* Effect.interrupt;
                if (event._tag === "TextDelta") {
                  text += event.text;
                  yield* ttsSession.sendText(responseId, event.text).pipe(
                    Effect.mapError(
                      (): ResponseFailure => ({
                        stage: "elevenlabs",
                        reason: "Speech synthesis text streaming failed",
                      }),
                    ),
                  );
                  return;
                }

                completed = true;
                yield* Ref.update(completedText, (current) =>
                  new Map(current).set(responseId, text),
                );
                yield* ttsSession.finishContext(responseId).pipe(
                  Effect.mapError(
                    (): ResponseFailure => ({
                      stage: "elevenlabs",
                      reason: "Speech synthesis context failed to finish",
                    }),
                  ),
                );
              }),
            ),
            Effect.result,
          );

        if (Result.isFailure(generated)) {
          yield* failResponse(responseId, generated.failure);
        } else if (!completed) {
          yield* failResponse(responseId, {
            stage: "openai",
            reason: "Language model generation ended without completion",
          });
        }
      });

      const beginResponse = Effect.fnUntraced(function*(event: FluxEvent & { _tag: "Turn" }) {
        const transcript = event.transcript;
        // Acceptance and trimming share the recognition epoch fence; retired finals are replayed.
        const started = yield* deepgramSession.commitTurn(event, updateState((current) => {
          const result = startResponse(current, transcript);
          return Result.isFailure(result)
            ? Result.fail(result.failure)
            : Result.succeed([
                result.success.state,
                {
                  responseId: result.success.responseId,
                  userTurnId: result.success.userTurnId,
                  turns: result.success.state.turns,
                },
              ] as const);
        }).pipe(Effect.flatMap((result) => Result.isFailure(result)
          ? Effect.fail(result.failure) : Effect.succeed(result.success)))).pipe(Effect.result);
        if (Result.isFailure(started)) {
          yield* enqueue(coordinatorFailure(started.failure));
          return;
        }
        if (started.success === null) return;
        const accepted = started.success;

        yield* enqueue({
          _tag: "transcript",
          turnId: accepted.userTurnId,
          text: transcript,
          final: true,
        });

        const gate = yield* Deferred.make<void>();
        const fiber = yield* Deferred.await(gate).pipe(
          Effect.andThen(
            runGeneration({
              responseId: accepted.responseId,
              turns: accepted.turns,
            }),
          ),
          Effect.forkScoped,
        );
        const installed = yield* stateLock.withPermit(
          Effect.gen(function* () {
            const current = yield* Ref.get(state);
            if (current.activeResponseId !== accepted.responseId) return false;
            yield* Ref.set(currentGeneration, {
              responseId: accepted.responseId,
              fiber,
            });
            return true;
          }),
        );
        if (installed) yield* Deferred.succeed(gate, undefined);
        else yield* Fiber.interrupt(fiber);
      });

      const handleTurn = Effect.fnUntraced(function*(event: FluxEvent & { _tag: "Turn" }) {
        if (event.kind === "StartOfTurn") {
          yield* invalidateAndPublish(undefined, true);
        }
        if (event.kind === "Update" || event.kind === "StartOfTurn") {
          const turnId = yield* stateLock.withPermit(
            Ref.get(state).pipe(Effect.map(partialTurnId)),
          );
          yield* enqueue({
            _tag: "transcript",
            turnId,
            text: event.transcript,
            final: false,
          });
        }
        if (event.kind === "EndOfTurn") yield* beginResponse(event);
      });

      const handleDeepgramEvent = Effect.fnUntraced(function*(event: FluxEvent) {
        switch (event._tag) {
          case "Turn":
            yield* handleTurn(event);
            break;
          case "ConfigurationRejected":
            yield* enqueue(
              failureOutput("deepgram", "Deepgram rejected its configuration", true),
            );
            break;
          case "Warning":
            yield* enqueue(failureOutput("deepgram", "Deepgram reported a warning", true));
            break;
          case "RepeatRequired":
            yield* enqueue(failureOutput("deepgram", "Recognition restarted without a complete utterance. Please repeat after a pause.", true));
            break;
          case "FatalError":
            yield* shutdown(
              failureOutput("deepgram", "Deepgram reported a fatal error", false),
            );
            break;
          case "Connected":
          case "Configured":
            break;
        }
      });

      const handleTtsEvent = Effect.fnUntraced(function*(event: ElevenLabsEvent) {
        if (event._tag === "Audio") {
          if (event.payload.byteLength === 0) return;
          const sequence = yield* Ref.get(nextAudioSequence).pipe(
            Effect.map((current) => current.get(event.responseId) ?? 0),
          );
          const offered = yield* Queue.offer(output, {
            _tag: "audio",
            responseId: event.responseId,
            sequence,
            payload: event.payload,
          });
          if (offered) {
            yield* Ref.update(nextAudioSequence, (current) =>
              new Map(current).set(event.responseId, sequence + 1),
            );
          }
          return;
        }

        const text = (yield* Ref.get(completedText)).get(event.responseId);
        if (text === undefined) {
          yield* invalidateAndPublish(
            event.responseId,
            true,
            failureOutput("elevenlabs", "Speech synthesis ended before text generation completed", true),
          );
          return;
        }
        const audioChunks = (yield* Ref.get(nextAudioSequence)).get(event.responseId) ?? 0;
        if (text.trim() && audioChunks === 0) {
          yield* failResponse(event.responseId, {
            stage: "elevenlabs",
            reason: "Speech synthesis returned no audio; check voice access and provider account limits",
          });
          return;
        }
        yield* Queue.offer(output, {
          _tag: "ttsFinal",
          responseId: event.responseId,
        });
      });

      const shutdown = Effect.fnUntraced(function*(terminalFailure?: CallOutput) {
        if (yield* Ref.getAndSet(shuttingDown, true)) return;
        yield* Queue.end(audioInput);
        const active = yield* stateLock.withPermit(
          Effect.gen(function* () {
            const current = yield* Ref.get(state);
            const responseId = current.activeResponseId;
            const cleared = yield* Ref.get(clearedPlayback);
            const playbackResponseIds = current.responses
              .filter(
                (response) =>
                  response.emittedThrough !== null &&
                  response.playedThrough !== response.emittedThrough &&
                  !cleared.has(response.responseId) &&
                  response.responseId !== responseId,
              )
              .map((response) => response.responseId);
            const fiber = yield* Ref.getAndSet(currentGeneration, null);
            yield* saveState(endCall(current));
            return { responseId, playbackResponseIds, fiber };
          }),
        );
        const clearResponseIds = [
          ...(active.responseId === null ? [] : [active.responseId]),
          ...active.playbackResponseIds,
        ];
        if (active.fiber !== null) yield* Fiber.interrupt(active.fiber.fiber);
        if (active.responseId !== null) {
          yield* ttsSession.closeContext(active.responseId).pipe(Effect.ignore);
        }
        yield* deepgramSession.closeStream.pipe(Effect.ignore);
        yield* ttsSession.closeSocket.pipe(Effect.ignore);
        yield* publicationLock.withPermit(
          Effect.gen(function* () {
            const controls: Array<CallOutput> = [];
            for (const responseId of clearResponseIds) {
              controls.push({
                _tag: "clearPlayback",
                responseId,
              });
            }
            if (terminalFailure !== undefined) controls.push(terminalFailure);
            if (controls.length > 0) {
              yield* Queue.offer(controlPublications, controls);
            }
            yield* Queue.end(controlPublications);
          }),
        );
        yield* Fiber.join(publicationFiber);
      });

      const handleInput = Effect.fnUntraced(function*(event: CallInput) {
        switch (event._tag) {
          case "audio": {
            // Recovery may replay up to ten seconds. Do not block control/ACK ingestion on it.
            const accepted = yield* Ref.modify(queuedAudioBytes, (bytes) => {
              if (bytes + event.payload.length > 320000 || !Queue.offerUnsafe(audioInput, event.payload)) return [false, bytes] as const;
              return [true, bytes + event.payload.length] as const;
            });
            if (!accepted) yield* shutdown(failureOutput("deepgram", "Recognition input backlog exceeded its bounded capacity", false));
            break;
          }
          case "playbackAck": {
            const acknowledged = yield* updateState((current) => {
              const result = acknowledgePlayback(
                current,
                event.responseId,
                event.playedThrough,
              );
              return Result.isFailure(result)
                ? Result.fail(result.failure)
                : Result.succeed([result.success, undefined] as const);
            });
            if (Result.isFailure(acknowledged)) {
              // Invalid client diagnostics must not block a following hangup.
              yield* Effect.sync(() =>
                Queue.offerUnsafe(output, coordinatorFailure(acknowledged.failure)),
              );
            }
            break;
          }
          case "interrupt":
            yield* invalidateAndPublish(undefined, true, undefined, true);
            break;
          case "hangup":
            yield* shutdown();
            return yield* Effect.interrupt;
        }
      });

      yield* Stream.fromQueue(audioInput).pipe(
        Stream.runForEach((bytes) => Ref.update(queuedAudioBytes, (count) => count - bytes.length).pipe(
          Effect.andThen(deepgramSession.sendAudio(bytes)),
        )),
        Effect.catch(() => Ref.get(shuttingDown).pipe(Effect.flatMap((closing) => closing ? Effect.void :
          shutdown(failureOutput("deepgram", "Sending audio to Deepgram failed", false)),
        ))),
        Effect.forkScoped,
      );
      yield* Effect.forever(deepgramSession.receive.pipe(
        Effect.flatMap((event) =>
          Ref.get(shuttingDown).pipe(
            Effect.flatMap((closing) => closing ? Effect.interrupt : handleDeepgramEvent(event)),
          ),
        ),
      )).pipe(
        Effect.catch(() =>
          Ref.get(shuttingDown).pipe(
            Effect.flatMap((closing) =>
              closing
                ? Effect.void
                : shutdown(
                    failureOutput(
                      "deepgram",
                      "Receiving Deepgram events failed",
                      false,
                    ),
                  ),
            ),
          ),
        ),
        Effect.forkScoped,
      );
      yield* Effect.forever(ttsSession.receive.pipe(
        Effect.flatMap((event) =>
          Ref.get(shuttingDown).pipe(
            Effect.flatMap((closing) => closing ? Effect.interrupt : handleTtsEvent(event)),
          ),
        ),
      )).pipe(
        Effect.catch(() =>
          Ref.get(shuttingDown).pipe(
            Effect.flatMap((closing) =>
              closing
                ? Effect.void
                : shutdown(
                    failureOutput(
                      "elevenlabs",
                      "Receiving synthesized audio failed",
                      false,
                    ),
                  ),
            ),
          ),
        ),
        Effect.forkScoped,
      );
      yield* input.pipe(
        Stream.runForEach(handleInput),
        Effect.matchCauseEffect({
          onFailure: (cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : shutdown(
                  failureOutput("coordinator", "Call input stream failed", false),
                ),
          onSuccess: () => shutdown(),
        }),
        Effect.forkScoped,
      );

      return Stream.fromQueue(output).pipe(
        Stream.mapEffect((event) => {
          if (event._tag === "controlBatch") {
            return event.events.some((control) => control._tag === "interruptConfirmed")
              ? Ref.set(interruptConfirmationPending, false).pipe(Effect.as(event.events))
              : Effect.succeed(event.events);
          }
          if (event._tag === "ttsFinal") {
            return stateLock.withPermit(
              Effect.gen(function* () {
                const text = (yield* Ref.get(completedText)).get(event.responseId);
                if (text === undefined) return [];
                const result = finishResponse(
                  yield* Ref.get(state),
                  event.responseId,
                  text,
                );
                if (Result.isFailure(result)) return [];
                yield* saveState(result.success);
                yield* Ref.update(completedText, (current) => {
                  const updated = new Map(current);
                  updated.delete(event.responseId);
                  return updated;
                });
                yield* Ref.update(currentGeneration, (current) =>
                  current?.responseId === event.responseId ? null : current,
                );
                return [{
                  _tag: "assistantText",
                  responseId: event.responseId,
                  text,
                } satisfies CallOutput, {
                  _tag: "responseFinished",
                  responseId: event.responseId,
                } satisfies CallOutput];
              }),
            );
          }
          if (event._tag !== "audio") return Effect.succeed([event] as ReadonlyArray<CallOutput>);
          return updateState((current) => {
            const result = recordAudioChunk(current, event.responseId, event.sequence);
            return Result.isFailure(result)
              ? Result.fail(result.failure)
              : Result.succeed([result.success, event] as const);
          }).pipe(
            Effect.map((result) =>
              Result.isSuccess(result) ? [result.success] : [],
            ),
          );
        }),
        Stream.flatMap(Stream.fromIterable),
      );
    }).pipe(
      Effect.catch((startup) =>
        Effect.succeed(
          Stream.make(
            failureOutput(startup.stage, startup.reason, false),
          ),
        ),
      ),
    ),
  );
