import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";
import { CallInput, CallOutput, SavedCallState, ValidatedCallState } from "../src/call-protocol.js";
import { initialCallState, startCall, startResponse, recordAudioChunk,
  acknowledgePlayback, finishResponse, interruptResponse, endCall } from "../src/call-state.js";
import { Result } from "effect";

describe("call protocol schemas", () => {
  it("round-trips binary input and output as base64", () => {
    const input = Schema.decodeUnknownSync(CallInput)({
      _tag: "audio",
      sequence: 7,
      capturedAt: 1234,
      payload: "AQID",
    });
    expect(input._tag).toBe("audio");
    if (input._tag !== "audio") throw new Error("expected audio input");
    expect(Array.from(input.payload)).toEqual([1, 2, 3]);
    expect(Schema.encodeSync(CallInput)(input)).toEqual({
      _tag: "audio",
      sequence: 7,
      capturedAt: 1234,
      payload: "AQID",
    });

    const output = Schema.decodeUnknownSync(CallOutput)({
      _tag: "audio",
      responseId: "response-9",
      sequence: 0,
      payload: "/wA=",
    });
    expect(output._tag).toBe("audio");
    if (output._tag === "audio") {
      expect(Array.from(output.payload)).toEqual([255, 0]);
    }
  });

  it("rejects empty IDs, negative sequences, and unknown event tags", () => {
    expect(() =>
      Schema.decodeUnknownSync(CallInput)({
        _tag: "playbackAck",
        responseId: "",
        playedThrough: 0,
      }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(CallInput)({
        _tag: "audio",
        sequence: -1,
        capturedAt: 1234,
        payload: "AQ==",
      }),
    ).toThrow();
    expect(() => Schema.decodeUnknownSync(CallInput)({ _tag: "pause" })).toThrow();
  });

  it("round-trips saved call state", () => {
    const state = Schema.decodeUnknownSync(SavedCallState)({
      phase: "responding",
      turns: [
        {
          turnId: "turn-4",
          role: "user",
          text: "hello",
          responseId: null,
        },
      ],
      responses: [
        {
          responseId: "response-8",
          userTurnId: "turn-4",
          status: "active",
          emittedThrough: 3,
          playedThrough: 1,
        },
      ],
      activeResponseId: "response-8",
      nextTurnNumber: 5,
      nextResponseNumber: 9,
    });

    expect(Schema.decodeUnknownSync(SavedCallState)(Schema.encodeSync(SavedCallState)(state))).toEqual(
      state,
    );
  });

  it("rejects cross-record snapshot corruption", () => {
    const valid = {
      phase: "responding", activeResponseId: "response-8", nextTurnNumber: 5, nextResponseNumber: 9,
      turns: [{ turnId: "turn-4", role: "user", text: "hello", responseId: null }],
      responses: [{ responseId: "response-8", userTurnId: "turn-4", status: "active",
        emittedThrough: 3, playedThrough: 1 }],
    };
    const corruptions = [
      { turns: [...valid.turns, ...valid.turns] },
      { responses: [...valid.responses, ...valid.responses] },
      { nextTurnNumber: 4 },
      { nextResponseNumber: 8 },
      { activeResponseId: "response-7" },
      { phase: "listening", activeResponseId: null },
      { responses: [{ ...valid.responses[0], status: "interrupted" }] },
      { responses: [{ ...valid.responses[0], userTurnId: "turn-3" }] },
      { responses: [{ ...valid.responses[0], playedThrough: 4 }] },
      { responses: [{ ...valid.responses[0], emittedThrough: null }] },
      { turns: [{ ...valid.turns[0], role: "assistant", responseId: "response-8" }] },
      { phase: "listening", activeResponseId: null,
        responses: [{ ...valid.responses[0], status: "finished" }] },
    ];
    for (const corruption of corruptions) {
      expect(() => Schema.decodeUnknownSync(ValidatedCallState)({ ...valid, ...corruption })).toThrow();
    }
  });

  it("accepts reducer-produced snapshots across the lifecycle", () => {
    const initial = initialCallState();
    const listening = Result.getOrThrow(startCall(initial));
    const started = Result.getOrThrow(startResponse(listening, "Hello"));
    const emitted = Result.getOrThrow(recordAudioChunk(started.state, started.responseId, 0));
    const played = Result.getOrThrow(acknowledgePlayback(emitted, started.responseId, 0));
    const finished = Result.getOrThrow(finishResponse(played, started.responseId, "Hi"));
    const interrupted = Result.getOrThrow(interruptResponse(played)).state;
    for (const state of [initial, listening, started.state, emitted, played, finished,
      interrupted, endCall(played), endCall(finished)]) {
      expect(Schema.decodeUnknownSync(ValidatedCallState)(Schema.encodeSync(ValidatedCallState)(state))).toEqual(state);
    }
  });
});
