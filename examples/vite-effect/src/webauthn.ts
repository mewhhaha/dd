import {
  generateAuthenticationOptions,
  generateRegistrationOptions,
  verifyAuthenticationResponse,
  verifyRegistrationResponse,
  type AuthenticationResponseJSON,
  type PublicKeyCredentialCreationOptionsJSON,
  type PublicKeyCredentialRequestOptionsJSON,
  type RegistrationResponseJSON,
  type VerifiedAuthenticationResponse,
  type VerifiedRegistrationResponse,
  type WebAuthnCredential,
} from "@simplewebauthn/server";
import { Context, Effect, Layer } from "effect";
import { internalError, unauthorized, type AppError } from "./errors";
import { RequestContext } from "./runtime";
import type { PasskeyCredential, User } from "./types";

type PasskeysService = {
  readonly registrationOptions: (user: User) => Effect.Effect<PublicKeyCredentialCreationOptionsJSON, AppError>;
  readonly authenticationOptions: Effect.Effect<PublicKeyCredentialRequestOptionsJSON, AppError>;
  readonly verifyRegistration: (
    response: RegistrationResponseJSON,
    expectedChallenge: string,
  ) => Effect.Effect<VerifiedRegistrationResponse & { verified: true }, AppError>;
  readonly verifyAuthentication: (
    response: AuthenticationResponseJSON,
    expectedChallenge: string,
    credential: PasskeyCredential,
  ) => Effect.Effect<VerifiedAuthenticationResponse & { verified: true }, AppError>;
  readonly credentialFromRegistration: (
    registration: VerifiedRegistrationResponse & { verified: true },
    response: RegistrationResponseJSON,
  ) => PasskeyCredential;
  readonly credentialFromAuthentication: (
    credential: PasskeyCredential,
    authentication: VerifiedAuthenticationResponse & { verified: true },
  ) => PasskeyCredential;
};

export class Passkeys extends Context.Tag("vite-effect/Passkeys")<Passkeys, PasskeysService>() {}

export const PasskeysLive = Layer.effect(Passkeys, Effect.gen(function* () {
  const { request } = yield* RequestContext;
  const rpID = relyingPartyID(request);
  const origin = expectedOrigin(request);

  function registrationOptions(user: User): Effect.Effect<PublicKeyCredentialCreationOptionsJSON, AppError> {
    return Effect.tryPromise({
      try: () => generateRegistrationOptions({
        rpName: "Vite Effect",
        rpID,
        userID: new TextEncoder().encode(user.id),
        userName: user.username,
        userDisplayName: user.displayName,
        attestationType: "none",
        excludeCredentials: user.credentials.map((credential) => ({
          id: credential.id,
          transports: credential.transports,
        })),
        authenticatorSelection: {
          residentKey: "required",
          userVerification: "required",
        },
      }),
      catch: (error) => internalError(`Registration options failed: ${String(error)}`),
    });
  }

  const authenticationOptions = Effect.tryPromise({
    try: () => generateAuthenticationOptions({
      rpID,
      userVerification: "required",
    }),
    catch: (error) => internalError(`Authentication options failed: ${String(error)}`),
  });

  function verifyRegistration(
    response: RegistrationResponseJSON,
    expectedChallenge: string,
  ): Effect.Effect<VerifiedRegistrationResponse & { verified: true }, AppError> {
    return Effect.tryPromise({
      try: () => verifyRegistrationResponse({
        response,
        expectedChallenge,
        expectedOrigin: origin,
        expectedRPID: rpID,
        requireUserVerification: true,
      }),
      catch: () => unauthorized("Passkey registration failed"),
    }).pipe(
      Effect.flatMap((result) => result.verified
        ? Effect.succeed(result)
        : Effect.fail(unauthorized("Passkey registration was not verified"))),
    );
  }

  function verifyAuthentication(
    response: AuthenticationResponseJSON,
    expectedChallenge: string,
    credential: PasskeyCredential,
  ): Effect.Effect<VerifiedAuthenticationResponse & { verified: true }, AppError> {
    return Effect.tryPromise({
      try: () => verifyAuthenticationResponse({
        response,
        expectedChallenge,
        expectedOrigin: origin,
        expectedRPID: rpID,
        credential: toWebAuthnCredential(credential),
        requireUserVerification: true,
      }),
      catch: () => unauthorized("Passkey sign-in failed"),
    }).pipe(
      Effect.flatMap((result) => result.verified
        ? Effect.succeed(result as VerifiedAuthenticationResponse & { verified: true })
        : Effect.fail(unauthorized("Passkey sign-in was not verified"))),
    );
  }

  return {
    registrationOptions,
    authenticationOptions,
    verifyRegistration,
    verifyAuthentication,
    credentialFromRegistration,
    credentialFromAuthentication,
  };
}));

function credentialFromRegistration(
  registration: VerifiedRegistrationResponse & { verified: true },
  response: RegistrationResponseJSON,
): PasskeyCredential {
  const now = new Date().toISOString();
  return {
    id: registration.registrationInfo.credential.id,
    publicKey: bytesToBase64URL(registration.registrationInfo.credential.publicKey),
    counter: registration.registrationInfo.credential.counter,
    transports: registration.registrationInfo.credential.transports ?? response.response.transports,
    deviceType: registration.registrationInfo.credentialDeviceType,
    backedUp: registration.registrationInfo.credentialBackedUp,
    createdAt: now,
  };
}

function credentialFromAuthentication(
  credential: PasskeyCredential,
  authentication: VerifiedAuthenticationResponse & { verified: true },
): PasskeyCredential {
  return {
    ...credential,
    counter: authentication.authenticationInfo.newCounter,
    deviceType: authentication.authenticationInfo.credentialDeviceType,
    backedUp: authentication.authenticationInfo.credentialBackedUp,
    lastUsedAt: new Date().toISOString(),
  };
}

function toWebAuthnCredential(credential: PasskeyCredential): WebAuthnCredential {
  return {
    id: credential.id,
    publicKey: base64URLToBytes(credential.publicKey),
    counter: credential.counter,
    transports: credential.transports,
  };
}

function relyingPartyID(request: Request): string {
  return new URL(request.url).hostname;
}

function expectedOrigin(request: Request): string {
  return new URL(request.url).origin;
}

function bytesToBase64URL(bytes: Uint8Array): string {
  let value = "";
  for (const byte of bytes) {
    value += String.fromCharCode(byte);
  }
  return btoa(value).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64URLToBytes(value: string): Uint8Array<ArrayBuffer> {
  const base64 = value.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(value.length / 4) * 4, "=");
  const binary = atob(base64);
  const bytes: Uint8Array<ArrayBuffer> = new Uint8Array(new ArrayBuffer(binary.length));
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}
