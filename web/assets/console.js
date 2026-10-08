import { CallTransport } from "./transport.js";
import { toBase64 } from "./pcm.js";

const $ = (id) => document.getElementById(id);
$("call-id").value = crypto.randomUUID();
let current = null;
function log(message) {
  const entry = document.createElement("li");
  const time = document.createElement("time"); time.textContent = new Date().toLocaleTimeString();
  entry.append(time, document.createTextNode(message)); $("events").prepend(entry);
  while ($("events").children.length > 100) $("events").lastChild.remove();
}
function status(value, detail) { $("status").textContent = value; $("detail").textContent = detail; }
function controls(busy, live = false) {
  $("start").disabled = busy; $("call-id").disabled = busy;
  $("interrupt").disabled = !live; $("hangup").disabled = !live;
}
function stopCapture(call) {
  call.capturing = false;
  call.node?.disconnect(); call.source?.disconnect();
  call.filter?.disconnect();
  call.media?.getTracks().forEach((track) => track.stop());
}
async function release(call) {
  if (call.disposed) return;
  call.disposed = true;
  if (current === call) { current = null; controls(false); }
  stopCapture(call); call.transport?.dispose();
  try { if (call.context?.state !== "closed") await call.context?.close(); }
  catch (error) { log(`Audio cleanup: ${error.message}`); }
}
async function fail(call, error) {
  if (call.disposed || call.failing || current !== call) return;
  call.failing = true; stopCapture(call);
  $("error").hidden = false;
  $("error").textContent = `Call stopped: ${error.message}. Start with a new call ID.`;
  log(`Failure: ${error.message}`); status("Failed", "Microphone stopped; session is being closed.");
  try {
    if (call.transport) await call.transport.request("/streams/input", {
      method: "POST", headers: { "Stream-Closed": "true" }, body: JSON.stringify(["hangup"]),
    });
  }
  catch (cleanup) { log(`Input closure could not be confirmed: ${cleanup.message}`); }
  await release(call);
}
function output(call, event) {
  if (current !== call) return;
  const [kind, value] = Object.entries(event)[0];
  if (kind === "transcript") {
    if (value.final) {
      $("empty-transcript").hidden = true; $("partial").textContent = "";
      const turn = document.createElement("p"); turn.className = "turn";
      const label = document.createElement("span"); label.textContent = `You · ${value.turnId}`;
      turn.append(label, document.createTextNode(value.text)); $("transcripts").append(turn);
    } else $("partial").textContent = value.text;
  }
  log(`${kind}${value.responseId ? ` · ${value.responseId}` : ""}${kind === "audio" ? ` · chunk ${value.sequence}` : ""}`);
  if (kind === "failure" && !value.recoverable) throw new Error(`${value.stage}: ${value.reason}`);
}

$("start").addEventListener("click", async () => {
  const id = $("call-id").value.trim();
  if (!id || current) return;
  const call = { capturing: false, sequence: 0 }; current = call;
  controls(true); $("error").hidden = true; status("Starting", "Requesting microphone permission…");
  try {
    call.media = await navigator.mediaDevices.getUserMedia({ audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true } });
    if (call.disposed) { stopCapture(call); return; }
    call.context = new AudioContext(); await call.context.resume();
    if (call.disposed) return;
    await call.context.audioWorklet.addModule("/assets/capture-worklet.js");
    if (call.disposed) return;
    call.transport = new CallTransport(id); await call.transport.create();
    if (call.disposed) return;
    call.read = call.transport.read((event) => output(call, event));
    call.read.then(async () => {
      if (current === call && !call.failing) { status("Ended", "Output stream closed. Use a new call ID."); await release(call); }
    }).catch((error) => fail(call, error));
    call.source = call.context.createMediaStreamSource(call.media);
    call.node = new AudioWorkletNode(call.context, "pcm-capture");
    // Browser-side anti-alias filtering before the 16 kHz linear resampler.
    call.filter = call.context.createBiquadFilter(); call.filter.type = "lowpass"; call.filter.frequency.value = 7200;
    call.source.connect(call.filter).connect(call.node).connect(call.context.destination);
    call.capturing = true;
    call.node.port.onmessage = ({ data }) => {
      if (!call.capturing) return;
      const sequence = call.sequence++;
      $("input-count").textContent = `${call.sequence} frames`;
      call.transport.append([{ audio: { sequence, capturedAt: Date.now(), payload: toBase64(data) } }])
        .catch((error) => fail(call, error));
    };
    controls(true, true); status("Listening", "Microphone → PCM16 / 16 kHz / mono. Playback not connected yet."); log("Call stream connected");
  } catch (error) { await fail(call, error); }
});
$("interrupt").addEventListener("click", () => {
  const call = current; if (!call?.capturing) return;
  call.transport.append(["interrupt"]).catch((error) => fail(call, error)); log("Interrupt requested");
});
$("hangup").addEventListener("click", async () => {
  const call = current; if (!call) return;
  controls(true); stopCapture(call); status("Ending", "Closing input and waiting for output closure…");
  try {
    await call.transport.append(["hangup"], true);
    await Promise.race([call.read, new Promise((_, reject) => setTimeout(() => reject(new Error("Output closure timed out")), 15000))]);
  } catch (error) { await fail(call, error); }
});
window.addEventListener("pagehide", () => {
  if (!current) return;
  const call = current;
  // Best effort only; ordinary Hang up is the confirmed closure path.
  if (call.transport) fetch(call.transport.base + "/streams/input", {
    method: "POST", keepalive: true, headers: { "Content-Type": "application/json", "Stream-Closed": "true" }, body: JSON.stringify(["hangup"]),
  }).catch(() => {});
  release(call);
});
controls(false); status("Ready", "Start a call to grant microphone access."); log("Console loaded · same-origin Golem HTTP deployment");
