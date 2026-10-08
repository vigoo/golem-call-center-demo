const callId = document.querySelector("#call-id");
callId.value = crypto.randomUUID();
const entry = document.createElement("li");
entry.textContent = "Console loaded · same-origin Golem HTTP deployment";
document.querySelector("#events").append(entry);
