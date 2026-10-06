import { Result, Schema } from "effect";
import {
  type CallTransitionError,
  type ResponseId,
  ResponseId as ResponseIdSchema,
  type ResponseProgress,
  type SavedCallState,
  type Sequence,
  type TurnId,
  TurnId as TurnIdSchema,
} from "./call-protocol.js";

export interface StartedResponse {
  readonly state: SavedCallState;
  readonly responseId: ResponseId;
  readonly userTurnId: TurnId;
}

export interface InterruptedResponse {
  readonly state: SavedCallState;
  readonly responseId: ResponseId | null;
}

const makeTurnId = (number: number): TurnId =>
  Schema.decodeUnknownSync(TurnIdSchema)(`turn-${number}`);

const makeResponseId = (number: number): ResponseId =>
  Schema.decodeUnknownSync(ResponseIdSchema)(`response-${number}`);

const updateResponse = (
  state: SavedCallState,
  responseId: ResponseId,
  update: (response: ResponseProgress) => ResponseProgress,
): ReadonlyArray<ResponseProgress> =>
  state.responses.map((response) =>
    response.responseId === responseId ? update(response) : response,
  );

const findResponse = (
  state: SavedCallState,
  responseId: ResponseId,
): ResponseProgress | undefined =>
  state.responses.find((response) => response.responseId === responseId);

export const initialCallState = (): SavedCallState => ({
  phase: "idle",
  turns: [],
  responses: [],
  activeResponseId: null,
  nextTurnNumber: 1,
  nextResponseNumber: 1,
});

export const startCall = (
  state: SavedCallState,
): Result.Result<SavedCallState, CallTransitionError> =>
  state.phase === "idle"
    ? Result.succeed({ ...state, phase: "listening" })
    : Result.fail({
        _tag: "InvalidCallPhase",
        operation: "startCall",
        phase: state.phase,
      });

export const startResponse = (
  state: SavedCallState,
  transcript: string,
): Result.Result<StartedResponse, CallTransitionError> => {
  if (state.phase !== "listening" || state.activeResponseId !== null) {
    return Result.fail({
      _tag: "InvalidCallPhase",
      operation: "startResponse",
      phase: state.phase,
    });
  }

  const userTurnId = makeTurnId(state.nextTurnNumber);
  const responseId = makeResponseId(state.nextResponseNumber);
  const response: ResponseProgress = {
    responseId,
    userTurnId,
    status: "active",
    emittedThrough: null,
    playedThrough: null,
  };

  return Result.succeed({
    responseId,
    userTurnId,
    state: {
      ...state,
      phase: "responding",
      turns: [
        ...state.turns,
        { turnId: userTurnId, role: "user", text: transcript, responseId: null },
      ],
      responses: [...state.responses, response],
      activeResponseId: responseId,
      nextTurnNumber: state.nextTurnNumber + 1,
      nextResponseNumber: state.nextResponseNumber + 1,
    },
  });
};

export const recordAudioChunk = (
  state: SavedCallState,
  responseId: ResponseId,
  sequence: Sequence,
): Result.Result<SavedCallState, CallTransitionError> => {
  if (state.activeResponseId !== responseId) {
    return Result.fail({
      _tag: "StaleResponse",
      expected: state.activeResponseId,
      received: responseId,
    });
  }

  const response = findResponse(state, responseId);
  if (response === undefined) {
    return Result.fail({ _tag: "UnknownResponse", responseId });
  }

  const expected = response.emittedThrough === null ? 0 : response.emittedThrough + 1;
  if (sequence !== expected) {
    return Result.fail({
      _tag: "OutOfOrderSequence",
      responseId,
      expected,
      received: sequence,
    });
  }

  return Result.succeed({
    ...state,
    responses: updateResponse(state, responseId, (current) => ({
      ...current,
      emittedThrough: sequence,
    })),
  });
};

export const acknowledgePlayback = (
  state: SavedCallState,
  responseId: ResponseId,
  playedThrough: Sequence,
): Result.Result<SavedCallState, CallTransitionError> => {
  const response = findResponse(state, responseId);
  if (response === undefined) {
    return Result.fail({ _tag: "UnknownResponse", responseId });
  }

  if (response.emittedThrough === null || playedThrough > response.emittedThrough) {
    return Result.fail({
      _tag: "PlaybackAheadOfEmission",
      responseId,
      emittedThrough: response.emittedThrough,
      playedThrough,
    });
  }

  if (response.playedThrough !== null && playedThrough <= response.playedThrough) {
    return Result.succeed(state);
  }

  return Result.succeed({
    ...state,
    responses: updateResponse(state, responseId, (current) => ({
      ...current,
      playedThrough,
    })),
  });
};

export const interruptResponse = (
  state: SavedCallState,
  expectedResponseId?: ResponseId,
): Result.Result<InterruptedResponse, CallTransitionError> => {
  if (state.activeResponseId === null) {
    return Result.succeed({ state, responseId: null });
  }

  if (
    expectedResponseId !== undefined &&
    expectedResponseId !== state.activeResponseId
  ) {
    return Result.fail({
      _tag: "StaleResponse",
      expected: state.activeResponseId,
      received: expectedResponseId,
    });
  }

  const responseId = state.activeResponseId;
  return Result.succeed({
    responseId,
    state: {
      ...state,
      phase: "listening",
      responses: updateResponse(state, responseId, (response) => ({
        ...response,
        status: "interrupted",
      })),
      activeResponseId: null,
    },
  });
};

export const finishResponse = (
  state: SavedCallState,
  responseId: ResponseId,
  text: string,
): Result.Result<SavedCallState, CallTransitionError> => {
  if (state.activeResponseId !== responseId) {
    return Result.fail({
      _tag: "StaleResponse",
      expected: state.activeResponseId,
      received: responseId,
    });
  }

  const assistantTurnId = makeTurnId(state.nextTurnNumber);
  return Result.succeed({
    ...state,
    phase: "listening",
    turns: [
      ...state.turns,
      {
        turnId: assistantTurnId,
        role: "assistant",
        text,
        responseId,
      },
    ],
    responses: updateResponse(state, responseId, (response) => ({
      ...response,
      status: "finished",
    })),
    activeResponseId: null,
    nextTurnNumber: state.nextTurnNumber + 1,
  });
};

export const endCall = (state: SavedCallState): SavedCallState => {
  const activeResponseId = state.activeResponseId;
  return {
    ...state,
    phase: "ended",
    responses:
      activeResponseId === null
        ? state.responses
        : updateResponse(state, activeResponseId, (response) => ({
            ...response,
            status: "interrupted",
          })),
    activeResponseId: null,
  };
};
