import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";
import { CallInput, CallOutput, SavedCallState } from "../src/call-protocol.js";

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
});
