import "virtual:react-router/unstable_rsc/inject-hmr-runtime";

import { startTransition, StrictMode } from "react";
import { hydrateRoot } from "react-dom/client";
import {
  createFromReadableStream,
  createTemporaryReferenceSet,
  encodeReply,
  setServerCallback,
} from "@vitejs/plugin-rsc/browser";
import {
  unstable_createCallServer as createCallServer,
  unstable_RSCHydratedRouter as RSCHydratedRouter,
  type unstable_RSCPayload as RSCPayload,
} from "react-router/dom";

type FlightChunk = string | Uint8Array;
type ViteRscRequire = (id: string) => unknown;

declare global {
  interface Window {
    __FLIGHT_DATA?: FlightChunk[] & {
      push(chunk: FlightChunk): number;
    };
    __dd_vite_rsc_require_wrapped?: boolean;
    __vite_rsc_client_require__?: ViteRscRequire;
  }
}

setServerCallback(
  createCallServer({
    createFromReadableStream,
    createTemporaryReferenceSet,
    encodeReply,
  }),
);

createFromReadableStream<RSCPayload>(getSettledRSCStream()).then((payload) => {
  startTransition(async () => {
    const formState =
      payload.type === "render" ? await payload.formState : undefined;

    hydrateRoot(
      document,
      <StrictMode>
        <RSCHydratedRouter
          createFromReadableStream={createFromReadableStream}
          payload={payload}
        />
      </StrictMode>,
      {
        formState,
      },
    );
  });
});

function getSettledRSCStream() {
  const encoder = new TextEncoder();
  let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
  let pendingClientReferences = 0;
  let closeRequested = false;
  let closeTimer: ReturnType<typeof setTimeout> | undefined;

  const scheduleClose = () => {
    if (!closeRequested || !controller || pendingClientReferences > 0) {
      return;
    }
    if (closeTimer) {
      clearTimeout(closeTimer);
    }
    closeTimer = setTimeout(() => {
      if (pendingClientReferences === 0) {
        controller?.close();
      }
    }, 50);
  };

  const trackClientReferenceLoads = () => {
    const originalRequire = window.__vite_rsc_client_require__;
    if (window.__dd_vite_rsc_require_wrapped || typeof originalRequire !== "function") {
      return;
    }
    window.__dd_vite_rsc_require_wrapped = true;
    window.__vite_rsc_client_require__ = (id: string) => {
      const value = originalRequire(id);
      if (value && typeof (value as Promise<unknown>).then === "function") {
        pendingClientReferences += 1;
        if (closeTimer) {
          clearTimeout(closeTimer);
          closeTimer = undefined;
        }
        Promise.resolve(value).finally(() => {
          pendingClientReferences -= 1;
          scheduleClose();
        });
      }
      return value;
    };
  };

  const handleChunk = (chunk: FlightChunk) => {
    controller?.enqueue(typeof chunk === "string" ? encoder.encode(chunk) : chunk);
  };

  const stream = new ReadableStream<Uint8Array>({
    start(nextController) {
      controller = nextController;
      trackClientReferenceLoads();
      window.__FLIGHT_DATA ||= [];
      window.__FLIGHT_DATA.forEach(handleChunk);
      window.__FLIGHT_DATA.push = (chunk: FlightChunk) => {
        handleChunk(chunk);
        return 0;
      };
    },
  });

  const requestClose = () => {
    closeRequested = true;
    scheduleClose();
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", requestClose, { once: true });
  } else {
    requestClose();
  }

  return stream;
}
