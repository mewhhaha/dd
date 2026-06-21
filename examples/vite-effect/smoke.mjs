import { createServer } from "vite";

const server = await createServer({
  configFile: new URL("./vite.config.ts", import.meta.url).pathname,
  server: {
    host: "localhost",
    port: 0,
  },
});

try {
  await server.listen();
  const { port } = server.httpServer.address();
  const base = `http://localhost:${port}`;

  const home = await fetch(`${base}/`);
  const homeText = await home.text();
  if (!homeText.includes("Passkey Auth") || !homeText.includes('data-worker="vite-effect"')) {
    throw new Error(`home did not render passkey worker: ${homeText}`);
  }
  if (!homeText.includes("data-passkey-randomize") || homeText.includes('value="ada"')) {
    throw new Error(`home did not expose generated signup controls: ${homeText}`);
  }

  const anonymousSession = await fetch(`${base}/api/session`);
  if (anonymousSession.status !== 401) {
    throw new Error(`anonymous session should be 401, got ${anonymousSession.status}`);
  }

  const anonymousAudit = await fetch(`${base}/audit`);
  if (anonymousAudit.status !== 401) {
    throw new Error(`anonymous audit should be 401, got ${anonymousAudit.status}`);
  }

  const invalidRegistration = await postJson(`${base}/api/passkeys/register/options`, {
    username: "   ",
    displayName: "",
  }, { ok: false });
  if (invalidRegistration.status !== 400) {
    throw new Error(`invalid registration should be 400, got ${invalidRegistration.status}`);
  }

  const registrationOptions = await postJson(`${base}/api/passkeys/register/options`, {
    username: "guest-smoke",
    displayName: "Smoke Passkey",
  });
  if (
    !registrationOptions.challenge
    || registrationOptions.rp?.name !== "Vite Effect"
    || registrationOptions.rp?.id !== "localhost"
    || registrationOptions.user?.name !== "guest-smoke"
    || registrationOptions.user?.displayName !== "Smoke Passkey"
    || registrationOptions.authenticatorSelection?.residentKey !== "required"
    || registrationOptions.authenticatorSelection?.userVerification !== "required"
  ) {
    throw new Error(`registration options were not passkey-shaped: ${JSON.stringify(registrationOptions)}`);
  }

  const badRegistration = await postJson(`${base}/api/passkeys/register/verify`, {
    id: "not-a-real-passkey",
    rawId: "not-a-real-passkey",
    response: {
      clientDataJSON: "bad",
      attestationObject: "bad",
    },
    clientExtensionResults: {},
    type: "public-key",
  }, { ok: false });
  if (badRegistration.status !== 401) {
    throw new Error(`bad registration should be 401, got ${badRegistration.status}`);
  }

  const authenticationOptions = await postJson(`${base}/api/passkeys/authenticate/options`);
  if (
    !authenticationOptions.challenge
    || authenticationOptions.rpId !== "localhost"
    || authenticationOptions.userVerification !== "required"
  ) {
    throw new Error(`authentication options were not passkey-shaped: ${JSON.stringify(authenticationOptions)}`);
  }

  const badAuthentication = await postJson(`${base}/api/passkeys/authenticate/verify`, {
    id: "missing-passkey",
    rawId: "missing-passkey",
    response: {
      clientDataJSON: "bad",
      authenticatorData: "bad",
      signature: "bad",
    },
    clientExtensionResults: {},
    type: "public-key",
  }, { ok: false });
  if (badAuthentication.status !== 401) {
    throw new Error(`bad authentication should be 401, got ${badAuthentication.status}`);
  }

  const logout = await fetch(`${base}/logout`, {
    method: "POST",
    redirect: "manual",
  });
  if (logout.status !== 303 || !logout.headers.get("set-cookie")?.includes("Max-Age=0")) {
    throw new Error(`logout did not clear session: ${logout.status}`);
  }

  const workerRoute = await fetch(`${base}/src/status?raw`);
  const workerRouteText = await workerRoute.text();
  if (workerRouteText !== "frontend worker src namespace") {
    throw new Error(`worker /src route was swallowed by Vite: ${workerRouteText}`);
  }

  const authWorkerRoute = await fetch(`${base}/__auth/status`);
  const authWorkerRouteText = await authWorkerRoute.text();
  if (authWorkerRouteText !== "auth worker ready") {
    throw new Error(`auth worker route did not proxy through dynamic binding: ${authWorkerRouteText}`);
  }

  const moduleResponse = await fetch(`${base}/src/client.ts`, {
    headers: { "sec-fetch-dest": "script" },
  });
  const moduleText = await moduleResponse.text();
  if (!moduleText.includes("vite-effect-client-module") || moduleText.includes("Passkey Auth")) {
    throw new Error(`Vite module did not bypass effect worker: ${moduleText}`);
  }
} finally {
  await server.close();
}

async function postJson(url, body = {}, options = { ok: true }) {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const payload = await response.json();
  if (options.ok && !response.ok) {
    throw new Error(`${url} failed with ${response.status}: ${JSON.stringify(payload)}`);
  }
  return options.ok ? payload : { status: response.status, payload };
}
