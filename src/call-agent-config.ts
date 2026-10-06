import { defineConfig } from "@golemcloud/effect-golem";
import { Schema } from "effect";

export class CallAgentConfig extends defineConfig("CallAgent.Config", {
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
}) {}
