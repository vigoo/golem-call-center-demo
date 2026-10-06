# Call Center

An Effect-based Golem application for the streaming call-center prototype described in
[`DEVELOPMENT_PLAN.md`](./DEVELOPMENT_PLAN.md).

## Development prerequisites

- Node.js 22.12 or newer
- Golem CLI 1.6
- The Golem repository checked out at `../../golem`
- The local Effect SDK built at `../../golem/sdks/effect`

Install dependencies and run the local checks:

```nu
npm install
npm test
npm run typecheck
```

The application intentionally uses a `file:` dependency on the local Effect Golem SDK. Point the
Golem CLI at the same checkout for every build or deployment:

```nu
$env.GOLEM_EFFECT_GOLEM_PATH = ("../../golem/sdks/effect" | path expand)
golem build -Y
```

Provider configuration, local deployment, and browser usage will be documented as those stages are
implemented. Until then, `src/streaming-agent.ts` remains the generated Durable Streams example.
