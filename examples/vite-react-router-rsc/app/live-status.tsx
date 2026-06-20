"use client";

import { useEffect, useState } from "react";

export function LiveSocketStatus() {
  const [status, setStatus] = useState("socket starting");

  useEffect(() => {
    if (typeof WebSocket === "undefined") {
      setStatus("socket unavailable");
      return;
    }

    let active = true;
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(`${protocol}//${window.location.host}/live`);

    socket.addEventListener("open", () => {
      if (!active) {
        return;
      }
      setStatus("socket open");
      socket.send(`hello ${window.location.pathname}`);
    });
    socket.addEventListener("message", (event) => {
      if (active) {
        setStatus(liveStatusFromMessage(event.data));
      }
    });
    socket.addEventListener("close", () => {
      if (active) {
        setStatus("socket closed");
      }
    });
    socket.addEventListener("error", () => {
      if (active) {
        setStatus("socket error");
      }
    });

    return () => {
      active = false;
      socket.close();
    };
  }, []);

  return (
    <span className="font-medium text-neutral-950" data-storefront-live-status>
      {status}
    </span>
  );
}

function liveStatusFromMessage(value: unknown): string {
  try {
    const payload = JSON.parse(String(value)) as { message?: unknown };
    const message = typeof payload.message === "string" ? payload.message : "";
    if (message === "connected") {
      return "socket connected";
    }
    if (message.startsWith("hello ")) {
      return "socket echo";
    }
    return "socket message";
  } catch {
    return "socket message";
  }
}
