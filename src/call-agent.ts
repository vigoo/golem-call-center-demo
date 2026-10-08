import { defineAgent, Http, method, Snapshot, WitTypes } from "@golemcloud/effect-golem";
import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { CallAgentConfig } from "./call-agent-config.js";
import { coordinateCall } from "./call-coordinator.js";
import { CallId, CallInput, CallOutput, SavedCallState, ValidatedCallState } from "./call-protocol.js";
import { initialCallState } from "./call-state.js";
import { DeepgramFlux } from "./deepgram-flux.js";
import { DeepgramTransportLive } from "./deepgram-flux-live.js";
import { ElevenLabsTts } from "./elevenlabs-tts.js";
import { ElevenLabsTransportLive } from "./elevenlabs-tts-live.js";
import { openAiLanguageModelLayer, ResponseGenerator } from "./response-generation.js";

export const CallAgent = defineAgent({
  name: "CallAgent",
  mode: "durable",
  id: { callId: CallId },
  config: CallAgentConfig,
  http: Http.mount("/calls/{callId}"),
  snapshotting: Snapshot.define({
    schema: SavedCallState,
    policy: Snapshot.policy.everyN(1),
  }),
  methods: {
    run: method({
      input: { input: WitTypes.AgentStream(CallInput) },
      success: WitTypes.AgentStream(CallOutput),
      http: [Http.post("/run", {
        durableStreams: {
          slots: [
            { source: "input", slot: "input", name: "input" },
            { source: "output", slot: "$result", name: "output" },
          ],
          allowExternalWrites: true,
          allowInvocationDelete: true,
          load: {
            maxConcurrentReadersPerStream: 4,
            maxAppendRequestsPerSecondPerStream: 25,
          },
        },
      })],
    }),
  },
}).implement({
  init: () => Ref.make(initialCallState()),
  snapshot: Snapshot.strategy<Ref.Ref<SavedCallState>, SavedCallState>({
    save: (state) => Ref.get(state).pipe(Effect.flatMap(Schema.decodeUnknownEffect(ValidatedCallState))),
    restore: (saved) => Schema.decodeUnknownEffect(ValidatedCallState)(saved).pipe(
      Effect.tap((state) => Effect.logInfo("Call snapshot restored").pipe(
        Effect.annotateLogs({ phase: state.phase, turns: state.turns.length }),
      )),
      Effect.flatMap(Ref.make),
    ),
  }),
  methods: (state) => ({
    run: ({ input }) => Effect.gen(function* () {
      const initialState = yield* Ref.get(state);
      if (initialState.phase === "ended") {
        return Stream.make({
          _tag: "failure" as const,
          stage: "coordinator" as const,
          reason: "Call has ended; start a new call ID",
          recoverable: false,
        });
      }
      const config = yield* CallAgentConfig;
      const providers = Layer.mergeAll(
        DeepgramFlux.layer.pipe(Layer.provide(DeepgramTransportLive)),
        ElevenLabsTts.layer.pipe(Layer.provide(ElevenLabsTransportLive)),
        ResponseGenerator.layer.pipe(Layer.provide(openAiLanguageModelLayer({
          apiKey: yield* config.openAi.apiKey.get,
          apiUrl: yield* config.openAi.apiUrl,
          model: yield* config.openAi.model,
        }))),
      );
      return coordinateCall(input, {
        initialState,
        onStateChange: (next) => Ref.set(state, next),
        deepgramApiKey: yield* config.deepgram.apiKey.get,
        deepgram: {
          apiUrl: yield* config.deepgram.apiUrl,
          model: yield* config.deepgram.model,
          sampleRate: 16000,
        },
        elevenLabsApiKey: yield* config.elevenLabs.apiKey.get,
        elevenLabs: {
          apiUrl: yield* config.elevenLabs.apiUrl,
          voiceId: yield* config.elevenLabs.voiceId,
          modelId: yield* config.elevenLabs.modelId,
          outputFormat: yield* config.elevenLabs.outputFormat,
          inactivityTimeoutSeconds: yield* config.elevenLabs.inactivityTimeoutSeconds,
          voiceSettings: {
            stability: yield* config.elevenLabs.stability,
            similarityBoost: yield* config.elevenLabs.similarityBoost,
            speed: yield* config.elevenLabs.speed,
          },
          chunkLengthSchedule: yield* config.elevenLabs.chunkLengthSchedule,
        },
        systemPrompt: yield* config.openAi.systemPrompt,
      }).pipe(Stream.provide(providers));
    }),
  }),
});
