import { boundaries, scenarios } from "./walkthrough-data.js";

const $ = (id) => document.getElementById(id);
let scenarioId = "turn", index = 0;
const scenarioButtons = new Map(), boundaryButtons = new Map();
function inspect(id) {
  const boundary = boundaries[id];
  $("owner-title").textContent = boundary.title;
  $("owner-detail").textContent = boundary.detail;
  $("owner-source").textContent = boundary.source;
  for (const [key, button] of boundaryButtons) button.setAttribute("aria-pressed", String(key === id));
}
for (const [id, scenario] of Object.entries(scenarios)) {
  const button = document.createElement("button"); button.textContent = scenario.title;
  button.addEventListener("click", () => { location.hash = `${id}/0`; });
  scenarioButtons.set(id, button); $("scenarios").append(button);
}
for (const [id, boundary] of Object.entries(boundaries)) {
  const button = document.createElement("button"); button.className = "boundary";
  const title = document.createElement("strong"); title.textContent = boundary.title;
  const subtitle = document.createElement("span"); subtitle.textContent = boundary.subtitle;
  button.append(title, subtitle); button.addEventListener("click", () => inspect(id));
  boundaryButtons.set(id, button); $("boundaries").append(button);
}
function render() {
  const parts = location.hash.slice(1).split("/");
  const valid = parts.length === 2 && Object.hasOwn(scenarios, parts[0]) && /^(0|[1-9]\d*)$/.test(parts[1]) && Number(parts[1]) < scenarios[parts[0]].steps.length;
  [scenarioId, index] = valid ? [parts[0], Number(parts[1])] : ["turn", 0];
  if (!valid) history.replaceState(null, "", "#turn/0");
  const scenario = scenarios[scenarioId], step = scenario.steps[index];
  for (const [id, button] of scenarioButtons) button.setAttribute("aria-pressed", String(id === scenarioId));
  for (const [id, button] of boundaryButtons) button.dataset.active = String(step.path.includes(id));
  $("steps").replaceChildren();
  scenario.steps.forEach((entry, position) => {
    const item = document.createElement("li"), button = document.createElement("button");
    button.textContent = `${String(position + 1).padStart(2, "0")}  ${entry.title}`;
    if (position === index) button.setAttribute("aria-current", "step");
    button.addEventListener("click", () => { location.hash = `${scenarioId}/${position}`; });
    item.append(button); $("steps").append(item);
  });
  $("scenario-title").textContent = scenario.title;
  $("step-count").textContent = `STEP ${index + 1} / ${scenario.steps.length}`;
  for (const id of ["state", "description", "invariant", "protocol", "payload", "source"]) $(id).textContent = step[id];
  $("step-title").textContent = step.title;
  $("route").textContent = step.path.map((id) => boundaries[id].title).join(" → ");
  $("previous").disabled = index === 0;
  $("next").disabled = index === scenario.steps.length - 1;
  $("copy-status").textContent = "";
  inspect(step.path.at(-1));
}
$("previous").addEventListener("click", () => { if (index > 0) location.hash = `${scenarioId}/${index - 1}`; });
$("next").addEventListener("click", () => { if (index + 1 < scenarios[scenarioId].steps.length) location.hash = `${scenarioId}/${index + 1}`; });
$("copy-link").addEventListener("click", async () => {
  try { await navigator.clipboard.writeText(location.href); $("copy-status").textContent = "Link copied"; }
  catch { $("copy-status").textContent = "Copy the address bar URL to share this step."; }
});
window.addEventListener("hashchange", render);
render();
