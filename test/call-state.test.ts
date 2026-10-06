import { describe, expect, it } from "@effect/vitest";
import { Result, Schema } from "effect";
import { ResponseId } from "../src/call-protocol.js";
import {
  acknowledgePlayback,
  endCall,
  finishResponse,
  initialCallState,
  interruptResponse,
  recordAudioChunk,
  startCall,
  startResponse,
} from "../src/call-state.js";

const getSuccess = <A, E>(result: Result.Result<A, E>): A => {
  expect(Result.isSuccess(result)).toBe(true);
  if (Result.isFailure(result)) throw new Error("expected transition success");
  return result.success;
};

const responseId = (value: string) => Schema.decodeUnknownSync(ResponseId)(value);

describe("call state transitions", () => {
  it("creates one current response and persists completed turns", () => {
    const listening = getSuccess(startCall(initialCallState()));
    const started = getSuccess(startResponse(listening, "I need help"));

    expect(started.responseId).toBe("response-1");
    expect(started.userTurnId).toBe("turn-1");
    expect(started.state.activeResponseId).toBe("response-1");
    expect(started.state.turns).toEqual([
      {
        turnId: "turn-1",
        role: "user",
        text: "I need help",
        responseId: null,
      },
    ]);

    const duplicate = startResponse(started.state, "must not overlap");
    expect(Result.isFailure(duplicate)).toBe(true);
    if (Result.isFailure(duplicate)) expect(duplicate.failure._tag).toBe("InvalidCallPhase");

    const completed = getSuccess(finishResponse(started.state, started.responseId, "I can help"));
    expect(completed.phase).toBe("listening");
    expect(completed.activeResponseId).toBeNull();
    expect(completed.turns[1]).toEqual({
      turnId: "turn-2",
      role: "assistant",
      text: "I can help",
      responseId: "response-1",
    });
    expect(completed.responses[0]?.status).toBe("finished");
  });

  it("rejects stale responses and sequence gaps with asymmetric IDs", () => {
    const listening = getSuccess(startCall(initialCallState()));
    const started = getSuccess(startResponse(listening, "hello"));

    const stale = recordAudioChunk(started.state, responseId("response-11"), 0);
    expect(Result.isFailure(stale)).toBe(true);
    if (Result.isFailure(stale)) {
      expect(stale.failure).toMatchObject({
        _tag: "StaleResponse",
        expected: "response-1",
        received: "response-11",
      });
    }

    const first = getSuccess(recordAudioChunk(started.state, started.responseId, 0));
    const gap = recordAudioChunk(first, started.responseId, 2);
    expect(Result.isFailure(gap)).toBe(true);
    if (Result.isFailure(gap)) {
      expect(gap.failure).toMatchObject({
        _tag: "OutOfOrderSequence",
        expected: 1,
        received: 2,
      });
    }
  });

  it("keeps playback acknowledgements monotonic and bounded by emitted audio", () => {
    const listening = getSuccess(startCall(initialCallState()));
    const started = getSuccess(startResponse(listening, "hello"));
    const afterZero = getSuccess(recordAudioChunk(started.state, started.responseId, 0));
    const afterOne = getSuccess(recordAudioChunk(afterZero, started.responseId, 1));

    const ahead = acknowledgePlayback(afterOne, started.responseId, 2);
    expect(Result.isFailure(ahead)).toBe(true);
    if (Result.isFailure(ahead)) expect(ahead.failure._tag).toBe("PlaybackAheadOfEmission");

    const acknowledged = getSuccess(acknowledgePlayback(afterOne, started.responseId, 1));
    const olderAck = getSuccess(acknowledgePlayback(acknowledged, started.responseId, 0));
    expect(olderAck).toBe(acknowledged);
    expect(olderAck.responses[0]?.playedThrough).toBe(1);
  });

  it("invalidates an interrupted generation but accepts its late playback acknowledgement", () => {
    const listening = getSuccess(startCall(initialCallState()));
    const first = getSuccess(startResponse(listening, "first"));
    const withAudio = getSuccess(recordAudioChunk(first.state, first.responseId, 0));
    const interrupted = getSuccess(interruptResponse(withAudio));

    expect(interrupted.responseId).toBe("response-1");
    expect(interrupted.state.activeResponseId).toBeNull();
    expect(interrupted.state.responses[0]?.status).toBe("interrupted");

    const second = getSuccess(startResponse(interrupted.state, "second"));
    expect(second.responseId).toBe("response-2");
    const staleAudio = recordAudioChunk(second.state, first.responseId, 1);
    expect(Result.isFailure(staleAudio)).toBe(true);

    const lateAck = getSuccess(acknowledgePlayback(second.state, first.responseId, 0));
    expect(lateAck.responses[0]?.playedThrough).toBe(0);
    expect(lateAck.activeResponseId).toBe("response-2");
  });

  it("ends the call by invalidating active work", () => {
    const listening = getSuccess(startCall(initialCallState()));
    const started = getSuccess(startResponse(listening, "bye"));
    const ended = endCall(started.state);

    expect(ended.phase).toBe("ended");
    expect(ended.activeResponseId).toBeNull();
    expect(ended.responses[0]?.status).toBe("interrupted");
    expect(Result.isFailure(startResponse(ended, "too late"))).toBe(true);
  });
});
