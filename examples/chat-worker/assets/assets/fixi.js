(() => {
  if (document.__fixi_mo) return;

  const send = (elt, type, detail, bubbles) => elt.dispatchEvent(
    new CustomEvent(`fx:${type}`, {
      detail,
      cancelable: true,
      bubbles: bubbles !== false,
      composed: true,
    }),
  );
  const attr = (elt, name, defaultValue) => elt.getAttribute(name) || defaultValue;
  const ignore = (elt) => elt.closest("[fx-ignore]") != null;

  const init = (elt) => {
    const options = {};
    if (elt.__fixi || ignore(elt) || !send(elt, "init", { options })) {
      return;
    }

    elt.__fixi = async (event) => {
      const requests = elt.__fixi.requests ||= new Set();
      const form = elt.form || elt.closest("form");
      const body = new FormData(form ?? undefined, event.submitter);
      if (elt.name && !event.submitter && (!form || (elt.form === form && elt.type === "submit"))) {
        body.append(elt.name, elt.value);
      }

      const abortController = new AbortController();
      const cfg = {
        trigger: event,
        action: attr(elt, "fx-action"),
        method: attr(elt, "fx-method", "GET").toUpperCase(),
        target: document.querySelector(attr(elt, "fx-target")) ?? elt,
        swap: attr(elt, "fx-swap", "outerHTML"),
        body,
        drop: requests.size,
        headers: { "FX-Request": "true" },
        abort: abortController.abort.bind(abortController),
        signal: abortController.signal,
        preventTrigger: true,
        transition: document.startViewTransition?.bind(document),
        fetch: fetch.bind(window),
      };

      const shouldContinue = send(elt, "config", { cfg, requests });
      if (cfg.preventTrigger) {
        event.preventDefault();
      }
      if (!shouldContinue || cfg.drop) {
        return;
      }

      if (/GET|DELETE/.test(cfg.method)) {
        const params = new URLSearchParams(cfg.body);
        if (params.size) {
          cfg.action += (/\?/.test(cfg.action) ? "&" : "?") + params;
        }
        cfg.body = null;
      }

      requests.add(cfg);
      try {
        if (cfg.confirm) {
          const result = await cfg.confirm();
          if (!result) {
            return;
          }
        }
        if (!send(elt, "before", { cfg, requests })) {
          return;
        }
        cfg.response = await cfg.fetch(cfg.action, cfg);
        cfg.text = await cfg.response.text();
        if (!send(elt, "after", { cfg })) {
          return;
        }
      } catch (error) {
        send(elt, "error", { cfg, error });
        return;
      } finally {
        requests.delete(cfg);
        send(elt, "finally", { cfg });
      }

      const doSwap = () => {
        if (cfg.swap instanceof Function) {
          return cfg.swap(cfg);
        }
        if (/(before|after)(begin|end)/.test(cfg.swap)) {
          cfg.target.insertAdjacentHTML(cfg.swap, cfg.text);
          return;
        }
        if (cfg.swap in cfg.target) {
          cfg.target[cfg.swap] = cfg.text;
          return;
        }
        if (cfg.swap !== "none") {
          throw cfg.swap;
        }
      };

      if (cfg.transition) {
        await cfg.transition(doSwap).finished;
      } else {
        await doSwap();
      }
      send(elt, "swapped", { cfg });
      if (!document.contains(elt)) {
        send(document, "swapped", { cfg });
      }
    };

    elt.__fixi.evt = attr(
      elt,
      "fx-trigger",
      elt.matches("form")
        ? "submit"
        : elt.matches("input:not([type=button]),select,textarea")
          ? "change"
          : "click",
    );
    elt.addEventListener(elt.__fixi.evt, elt.__fixi, options);
    send(elt, "inited", {}, false);
  };

  const process = (node) => {
    if (node.matches) {
      if (ignore(node)) {
        return;
      }
      if (node.matches("[fx-action]")) {
        init(node);
      }
    }
    if (node.querySelectorAll) {
      node.querySelectorAll("[fx-action]").forEach(init);
    }
  };

  document.__fixi_mo = new MutationObserver((records) => {
    records.forEach((record) => {
      if (record.type !== "childList") {
        return;
      }
      record.addedNodes.forEach((node) => process(node));
    });
  });

  document.addEventListener("fx:process", (event) => process(event.target));
  document.addEventListener("DOMContentLoaded", () => {
    document.__fixi_mo.observe(document.documentElement, {
      childList: true,
      subtree: true,
    });
    process(document.body);
  });
})();
