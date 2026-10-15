import { defineConfig } from "@golemcloud/effect-golem";
import { Schema } from "effect";

// Export a value until the static extractor supports imported config classes (GOL-739).
export const CallAgentConfig = defineConfig("CallAgent.Config", {
  isolatedInference: Schema.Boolean,
  deepgram: Schema.Struct({
    apiKey: Schema.Redacted(Schema.String),
    apiUrl: Schema.String,
    model: Schema.Literals(["flux-general-en", "flux-general-multi"]),
  }),
  openAi: Schema.Struct({
    apiKey: Schema.Redacted(Schema.String),
    apiUrl: Schema.String,
    model: Schema.String,
    systemPrompt: Schema.String,
  }),
  elevenLabs: Schema.Struct({
    apiKey: Schema.Redacted(Schema.String),
    apiUrl: Schema.String,
    voiceId: Schema.String,
    modelId: Schema.String,
    outputFormat: Schema.String,
    inactivityTimeoutSeconds: Schema.Int,
    stability: Schema.Number,
    similarityBoost: Schema.Number,
    speed: Schema.Number,
    chunkLengthSchedule: Schema.Array(Schema.Number),
  }),
});
