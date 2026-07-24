import { browserSupportsWebAuthn, startAuthentication, startRegistration } from "@simplewebauthn/browser";

document.documentElement.dataset.effectClient = "vite-effect-client-module";

const message = document.querySelector<HTMLElement>("[data-passkey-message]");
const registerForm = document.querySelector<HTMLFormElement>("[data-passkey-register]");
const loginForm = document.querySelector<HTMLFormElement>("[data-passkey-login]");
const randomizeButton = document.querySelector<HTMLButtonElement>("[data-passkey-randomize]");

fillFreshSignup();

if (!browserSupportsWebAuthn()) {
  setMessage("This browser cannot use passkeys.");
}

registerForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  await withSubmitLock(registerForm, async () => {
    setMessage("Creating passkey...");
    const formData = new FormData(registerForm);
    const optionsJSON = await postJson("/api/passkeys/register/options", {
      username: formData.get("username"),
      displayName: formData.get("displayName"),
    });
    const credential = await startRegistration({ optionsJSON });
    const result = await postJson<{ redirect?: string }>("/api/passkeys/register/verify", credential);
    location.assign(result.redirect ?? "/me");
  });
});

randomizeButton?.addEventListener("click", () => {
  fillFreshSignup(true);
});

loginForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  await withSubmitLock(loginForm, async () => {
    setMessage("Opening passkey prompt...");
    const optionsJSON = await postJson("/api/passkeys/authenticate/options");
    const credential = await startAuthentication({ optionsJSON });
    const result = await postJson<{ redirect?: string }>("/api/passkeys/authenticate/verify", credential);
    location.assign(result.redirect ?? "/me");
  });
});

for (const form of document.querySelectorAll<HTMLFormElement>("[data-effect-form]")) {
  form.addEventListener("submit", () => {
    form.querySelector<HTMLButtonElement>("button[type='submit']")?.setAttribute("disabled", "");
  });
}

async function postJson<T = any>(url: string, body?: unknown): Promise<T> {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(body ?? {}),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(String(payload.message ?? `Request failed with ${response.status}`));
  }
  return payload as T;
}

async function withSubmitLock(form: HTMLFormElement, run: () => Promise<void>): Promise<void> {
  const button = form.querySelector<HTMLButtonElement>("button[type='submit']");
  button?.setAttribute("disabled", "");
  try {
    await run();
  } catch (error) {
    setMessage(error instanceof Error ? error.message : String(error));
  } finally {
    button?.removeAttribute("disabled");
  }
}

function setMessage(value: string): void {
  if (message) {
    message.textContent = value;
  }
}

function fillFreshSignup(force = false): void {
  if (!registerForm) {
    return;
  }
  const username = registerForm.elements.namedItem("username");
  const displayName = registerForm.elements.namedItem("displayName");
  if (!(username instanceof HTMLInputElement) || !(displayName instanceof HTMLInputElement)) {
    return;
  }
  if (!force && (username.value || displayName.value)) {
    return;
  }
  const suffix = randomSuffix();
  username.value = `guest-${suffix}`;
  displayName.value = `Passkey Tester ${suffix.toUpperCase()}`;
}

function randomSuffix(): string {
  const bytes = new Uint8Array(5);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (byte) => byte.toString(36).padStart(2, "0")).join("").slice(0, 8);
}
