import { Schema } from "effect";

export const CallId = Schema.NonEmptyString.pipe(Schema.brand("CallId"));
export type CallId = typeof CallId.Type;

export const TurnId = Schema.NonEmptyString.pipe(Schema.brand("TurnId"));
export type TurnId = typeof TurnId.Type;

export const ResponseId = Schema.NonEmptyString.pipe(Schema.brand("ResponseId"));
export type ResponseId = typeof ResponseId.Type;

export const Sequence = Schema.Natural;
export type Sequence = typeof Sequence.Type;

export const ProviderStage = Schema.Literals([
  "deepgram",
  "openai",
  "elevenlabs",
  "coordinator",
]);
export type ProviderStage = typeof ProviderStage.Type;

export const AudioInput = Schema.Struct({
  _tag: Schema.Literal("audio"),
  sequence: Sequence,
  capturedAt: Schema.Natural,
  payload: Schema.Uint8ArrayFromBase64,
});

export const PlaybackAckInput = Schema.Struct({
  _tag: Schema.Literal("playbackAck"),
  responseId: ResponseId,
  playedThrough: Sequence,
});

export const InterruptInput = Schema.Struct({
  _tag: Schema.Literal("interrupt"),
});

export const HangupInput = Schema.Struct({
  _tag: Schema.Literal("hangup"),
});

export const CallInput = Schema.Union([
  AudioInput,
  PlaybackAckInput,
  InterruptInput,
  HangupInput,
]);
export type CallInput = typeof CallInput.Type;

export const TranscriptOutput = Schema.Struct({
  _tag: Schema.Literal("transcript"),
  turnId: TurnId,
  text: Schema.String,
  final: Schema.Boolean,
});

export const AudioOutput = Schema.Struct({
  _tag: Schema.Literal("audio"),
  responseId: ResponseId,
  sequence: Sequence,
  payload: Schema.Uint8ArrayFromBase64,
});

export const ClearPlaybackOutput = Schema.Struct({
  _tag: Schema.Literal("clearPlayback"),
  responseId: ResponseId,
});

export const ResponseFinishedOutput = Schema.Struct({
  _tag: Schema.Literal("responseFinished"),
  responseId: ResponseId,
});

export const FailureOutput = Schema.Struct({
  _tag: Schema.Literal("failure"),
  stage: ProviderStage,
  reason: Schema.String,
  recoverable: Schema.Boolean,
});

export const CallOutput = Schema.Union([
  TranscriptOutput,
  AudioOutput,
  ClearPlaybackOutput,
  ResponseFinishedOutput,
  FailureOutput,
]);
export type CallOutput = typeof CallOutput.Type;

export const ConversationTurn = Schema.Struct({
  turnId: TurnId,
  role: Schema.Literals(["user", "assistant"]),
  text: Schema.String,
  responseId: Schema.NullOr(ResponseId),
});
export type ConversationTurn = typeof ConversationTurn.Type;

export const ResponseProgress = Schema.Struct({
  responseId: ResponseId,
  userTurnId: TurnId,
  status: Schema.Literals(["active", "interrupted", "finished"]),
  emittedThrough: Schema.NullOr(Sequence),
  playedThrough: Schema.NullOr(Sequence),
});
export type ResponseProgress = typeof ResponseProgress.Type;

const SavedCallFields = {
  turns: Schema.Array(ConversationTurn),
  responses: Schema.Array(ResponseProgress),
  nextTurnNumber: Schema.Natural,
  nextResponseNumber: Schema.Natural,
} as const;

export const SavedCallState = Schema.Union([
  Schema.Struct({
    ...SavedCallFields,
    phase: Schema.Literal("idle"),
    activeResponseId: Schema.Null,
  }),
  Schema.Struct({
    ...SavedCallFields,
    phase: Schema.Literal("listening"),
    activeResponseId: Schema.Null,
  }),
  Schema.Struct({
    ...SavedCallFields,
    phase: Schema.Literal("responding"),
    activeResponseId: ResponseId,
  }),
  Schema.Struct({
    ...SavedCallFields,
    phase: Schema.Literal("ended"),
    activeResponseId: Schema.Null,
  }),
]);
export type SavedCallState = typeof SavedCallState.Type;

// Static metadata extraction cannot evaluate custom refinement functions. Keep the
// wire shape above static, and validate cross-record invariants in the snapshot strategy.
export const ValidatedCallState = SavedCallState.check(Schema.makeFilter((state) => {
  const turns = new Map(state.turns.map((turn) => [turn.turnId, turn]));
  const responses = new Map(state.responses.map((response) => [response.responseId, response]));
  if (turns.size !== state.turns.length || responses.size !== state.responses.length) {
    return "Snapshot contains duplicate IDs";
  }
  const validId = (id: string, prefix: string, next: number) => {
    const number = Number(id.slice(prefix.length));
    return id === `${prefix}${number}` && Number.isSafeInteger(number) && number > 0 && number < next;
  };
  if (!Number.isSafeInteger(state.nextTurnNumber) || state.nextTurnNumber < 1 ||
      !Number.isSafeInteger(state.nextResponseNumber) || state.nextResponseNumber < 1 ||
      state.turns.some((turn) => !validId(turn.turnId, "turn-", state.nextTurnNumber)) ||
      state.responses.some((response) => !validId(response.responseId, "response-", state.nextResponseNumber))) {
    return "Snapshot counters must exceed allocated IDs";
  }
  const active = state.responses.filter((response) => response.status === "active");
  if (state.phase === "responding"
      ? active.length !== 1 || active[0]?.responseId !== state.activeResponseId
      : active.length !== 0) {
    return "Snapshot active response does not match its phase";
  }
  if (state.responses.some((response) => {
    const user = turns.get(response.userTurnId);
    return user?.role !== "user" || user.responseId !== null ||
      (response.playedThrough !== null &&
        (response.emittedThrough === null || response.playedThrough > response.emittedThrough));
  })) return "Snapshot has an invalid user-turn reference or playback position";
  const assistantResponses = new Set<string>();
  for (const turn of state.turns) {
    if (turn.role === "user") {
      if (turn.responseId !== null) return "User turn must not reference an assistant response";
    } else {
      const response = turn.responseId === null ? undefined : responses.get(turn.responseId);
      if (response?.status !== "finished" || assistantResponses.has(response.responseId)) {
        return "Assistant turn must uniquely reference a finished response";
      }
      assistantResponses.add(response.responseId);
    }
  }
  if (state.responses.some((response) => response.status === "finished" && !assistantResponses.has(response.responseId))) {
    return "Finished response must have an assistant turn";
  }
  return true;
}));

export const CallTransitionError = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("InvalidCallPhase"),
    operation: Schema.String,
    phase: Schema.String,
  }),
  Schema.Struct({
    _tag: Schema.Literal("StaleResponse"),
    expected: Schema.NullOr(ResponseId),
    received: ResponseId,
  }),
  Schema.Struct({
    _tag: Schema.Literal("UnknownResponse"),
    responseId: ResponseId,
  }),
  Schema.Struct({
    _tag: Schema.Literal("OutOfOrderSequence"),
    responseId: ResponseId,
    expected: Sequence,
    received: Sequence,
  }),
  Schema.Struct({
    _tag: Schema.Literal("PlaybackAheadOfEmission"),
    responseId: ResponseId,
    emittedThrough: Schema.NullOr(Sequence),
    playedThrough: Sequence,
  }),
]);
export type CallTransitionError = typeof CallTransitionError.Type;
