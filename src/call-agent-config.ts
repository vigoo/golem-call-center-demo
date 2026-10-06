import { defineConfig } from "@golemcloud/effect-golem";
import { Schema } from "effect";

export class CallAgentConfig extends defineConfig("CallAgent.Config", {
  openAi: Schema.Struct({
    apiKey: Schema.Redacted(Schema.String),
    apiUrl: Schema.String,
    model: Schema.String,
    systemPrompt: Schema.String,
  }),
}) {}
