// Local demo controller, deliberately outside the deployed Golem component.
import { createServer, request as proxyRequest } from "node:http";
import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { homedir } from "node:os";

const environment = process.env.GOLEM_DEMO_ENV ?? "local";
if (!["local", "integration"].includes(environment)) throw new Error("GOLEM_DEMO_ENV must be local or integration");
const port = 19008;
const target = environment === "local" ? "call-center.localhost:19006" : "call-center-test.localhost:19006";
const allowedHosts = new Set([`localhost:${port}`, `127.0.0.1:${port}`]);
let crashing = false;
const server = createServer(async (request, response) => {
  const reply = (status, value) => response.writeHead(status, {
    "Content-Type": "application/json", "Cache-Control": "no-store",
  }).end(JSON.stringify(value));
  if (!allowedHosts.has(request.headers.host)) return reply(403, { error: "Local demo host required" });
  if (request.url === "/__demo" || request.url === "/__demo/crash") {
    if (request.url === "/__demo" && request.method === "GET") return reply(200, { environment });
    if (request.url !== "/__demo/crash" || request.method !== "POST") return reply(405, { error: "Method not allowed" });
    if (request.headers.origin !== `http://${request.headers.host}` || request.headers["content-type"] !== "application/json") {
      return reply(403, { error: "Same-origin JSON request required" });
    }
    try {
      let body = "";
      for await (const chunk of request) {
        body += chunk;
        if (body.length > 1024) return reply(413, { error: "Request too large" });
      }
      const { callId } = JSON.parse(body);
      if (typeof callId !== "string" || callId.length > 120 || !callId.trim()) {
        return reply(400, { error: "Call ID must contain 1–120 characters and not be blank" });
      }
      if (crashing) return reply(409, { error: "A crash request is already running" });
      crashing = true;
      console.log(`Simulating crash: CallAgent(${JSON.stringify(callId)}) in ${environment}`);
      execFile("golem", ["agent", "simulate-crash", `CallAgent(${JSON.stringify(callId)})`, "-E", environment, "-Y"], {
        cwd: fileURLToPath(new URL(".", import.meta.url)),
        env: { ...process.env, GOLEM_EFFECT_GOLEM_PATH: process.env.GOLEM_EFFECT_GOLEM_PATH ?? join(homedir(), "projects/golem/golem/sdks/effect") },
        timeout: 30000, maxBuffer: 1024 * 1024,
      }, (error, stdout, stderr) => {
        crashing = false;
        if (error) {
          console.error(stderr || error.message);
          if (!response.destroyed) reply(502, { error: "Crash command failed or timed out; check the controller log. Its outcome may be uncertain." });
        } else {
          console.log(stdout);
          if (!response.destroyed) reply(200, { callId, message: "Crash command completed; recovery is not yet confirmed." });
        }
      });
    } catch {
      if (!response.destroyed) reply(400, { error: "Invalid crash request" });
    }
    return;
  }
  // Preserve streaming bodies and Golem's CSP, MIME types and opaque stream cursors.
  const upstream = proxyRequest({ hostname: "127.0.0.1", port: 19006, path: request.url,
    method: request.method, headers: { ...request.headers, host: target },
  }, (incoming) => {
    response.writeHead(incoming.statusCode, incoming.headers);
    incoming.on("error", () => response.destroy());
    incoming.pipe(response);
  });
  upstream.on("error", () => {
    if (!response.headersSent) reply(502, { error: "Local Golem server is unavailable" });
    else response.destroy();
  });
  request.on("error", () => upstream.destroy());
  response.on("close", () => { if (!response.writableEnded) upstream.destroy(); });
  request.pipe(upstream);
});
server.listen(port, "127.0.0.1", () => console.log(`Demo console: http://localhost:${port} (${environment} → ${target})`));
