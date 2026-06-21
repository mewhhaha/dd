"use client";

import { useEffect, useRef, useState } from "react";
import { useRevalidator } from "react-router";

export function LiveSocketStatus() {
  const [status, setStatus] = useState(currentSocketStatus);

  useEffect(() => {
    const onStatus = (event: Event) => {
      setStatus((event as CustomEvent<string>).detail);
    };
    setStatus(currentSocketStatus);
    window.addEventListener(SOCKET_STATUS_EVENT, onStatus);

    return () => {
      window.removeEventListener(SOCKET_STATUS_EVENT, onStatus);
    };
  }, []);

  return (
    <span className="font-medium text-neutral-950" data-storefront-live-status>
      {status}
    </span>
  );
}

export function CartLiveSync() {
  const revalidator = useRevalidator();
  const revalidatorRef = useRef(revalidator);

  useEffect(() => {
    revalidatorRef.current = revalidator;
  }, [revalidator]);

  useEffect(() => {
    if (typeof WebSocket === "undefined") {
      publishSocketStatus("socket unavailable");
      return;
    }

    let active = true;
    let socket: WebSocket | null = null;
    const cancelConnect = runAfterPageLoad(() => {
      if (!active) {
        return;
      }
      publishSocketStatus("socket connecting");
      socket = openStorefrontSocket("/api/cart/live");

      socket.addEventListener("open", () => {
        if (active) {
          publishSocketStatus("socket open");
        }
      });
      socket.addEventListener("message", (event) => {
        if (!active) {
          return;
        }
        const status = cartSocketStatus(event.data);
        if (status) {
          publishSocketStatus(status);
        }
        if (isCartUpdateMessage(event.data)) {
          revalidatorRef.current.revalidate();
        }
      });
      socket.addEventListener("close", () => {
        if (active) {
          publishSocketStatus("socket closed");
        }
      });
      socket.addEventListener("error", () => {
        if (active) {
          publishSocketStatus("socket error");
        }
      });
    });

    return () => {
      active = false;
      cancelConnect();
      socket?.close();
    };
  }, []);

  return null;
}

function openStorefrontSocket(path: string): WebSocket {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return new WebSocket(`${protocol}//${window.location.host}${path}`);
}

const SOCKET_STATUS_EVENT = "storefront:cart-socket-status";
let currentSocketStatus = "socket starting";

function publishSocketStatus(status: string) {
  currentSocketStatus = status;
  window.dispatchEvent(new CustomEvent(SOCKET_STATUS_EVENT, { detail: status }));
}

function runAfterPageLoad(callback: () => void): () => void {
  if (document.readyState === "complete") {
    const timeout = window.setTimeout(callback, 0);
    return () => window.clearTimeout(timeout);
  }

  window.addEventListener("load", callback, { once: true });
  return () => window.removeEventListener("load", callback);
}

function cartSocketStatus(value: unknown): string | null {
  try {
    const payload = JSON.parse(String(value)) as { type?: unknown; message?: unknown };
    if (payload.type !== "storefront.cart") {
      return null;
    }
    const message = typeof payload.message === "string" ? payload.message : "";
    if (message === "connected") {
      return "socket connected";
    }
    if (message === "updated") {
      return "cart synced";
    }
    return "socket message";
  } catch {
    return null;
  }
}

function isCartUpdateMessage(value: unknown): boolean {
  try {
    const payload = JSON.parse(String(value)) as { type?: unknown; message?: unknown };
    return payload.type === "storefront.cart" && payload.message === "updated";
  } catch {
    return false;
  }
}
