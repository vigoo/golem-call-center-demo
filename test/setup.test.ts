import { expect, it } from "@effect/vitest";
import { Effect } from "effect";

it.effect("runs Effect tests", () =>
  Effect.gen(function* () {
    const value = yield* Effect.succeed("ready");

    expect(value).toBe("ready");
  }),
);
