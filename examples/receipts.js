let nextReceiptId = 1;

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}

function badRequest(message) {
  return json({ ok: false, error: message }, 400);
}

function notFound(message = "receipt not found") {
  return json({ ok: false, error: message }, 404);
}

function internalError(message) {
  return json({ ok: false, error: message }, 500);
}

function makeReceiptId() {
  const id = `rcpt_${Date.now()}_${nextReceiptId}`;
  nextReceiptId += 1;
  return id;
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : NaN;
}

function normalizeItems(items) {
  if (!Array.isArray(items) || items.length === 0) {
    return { ok: false, error: "items must be a non-empty array" };
  }

  const normalized = [];
  for (const item of items) {
    const description = String(item?.description ?? "").trim();
    const quantity = toNumber(item?.quantity);
    const unitPrice = toNumber(item?.unit_price);
    if (!description) {
      return { ok: false, error: "each item requires description" };
    }
    if (!Number.isFinite(quantity) || quantity <= 0) {
      return { ok: false, error: `invalid quantity for ${description}` };
    }
    if (!Number.isFinite(unitPrice) || unitPrice < 0) {
      return { ok: false, error: `invalid unit_price for ${description}` };
    }
    const lineTotal = quantity * unitPrice;
    normalized.push({
      description,
      quantity,
      unit_price: unitPrice,
      line_total: lineTotal,
    });
  }

  return { ok: true, items: normalized };
}

function computeTotals(items, taxRate) {
  const subtotal = items.reduce((sum, item) => sum + item.line_total, 0);
  const tax = subtotal * taxRate;
  const total = subtotal + tax;
  return { subtotal, tax, total };
}

function createStore(kv) {
  return {
    async get(id) {
      const raw = await kv.get(`receipt:${id}`);
      if (!raw) {
        return null;
      }
      try {
        return JSON.parse(raw);
      } catch {
        return null;
      }
    },
    async set(id, receipt) {
      await kv.set(`receipt:${id}`, JSON.stringify(receipt));
    },
    async delete(id) {
      await kv.delete(`receipt:${id}`);
    },
    async list(limit = 20) {
      const rows = await kv.list({
        prefix: "receipt:",
        limit: Math.max(1, Math.min(100, Number(limit) || 20)),
      });
      const receipts = [];
      for (const row of rows) {
        try {
          receipts.push(JSON.parse(row.value));
        } catch {
          // Skip malformed values.
        }
      }
      return receipts;
    },
    backend: "kv",
  };
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";
    const method = request.method.toUpperCase();
    const kv = env?.RECEIPTS;
    if (!kv || typeof kv.get !== "function" || typeof kv.set !== "function") {
      return internalError("missing KV binding: RECEIPTS");
    }
    const store = createStore(kv);

    if (method === "GET" && path === "/") {
      return json({
        ok: true,
        name: "receipts worker",
        backend: store.backend,
        routes: [
          "POST /receipts",
          "GET /receipts",
          "GET /receipts/:id",
          "DELETE /receipts/:id",
        ],
      });
    }

    if (method === "POST" && path === "/receipts") {
      let payload;
      try {
        payload = await request.json();
      } catch {
        return badRequest("request body must be valid JSON");
      }

      const merchant = String(payload?.merchant ?? "").trim();
      const currency = String(payload?.currency ?? "USD").trim().toUpperCase();
      const taxRate = toNumber(payload?.tax_rate ?? 0);
      if (!merchant) {
        return badRequest("merchant is required");
      }
      if (!currency) {
        return badRequest("currency is required");
      }
      if (!Number.isFinite(taxRate) || taxRate < 0) {
        return badRequest("tax_rate must be a non-negative number");
      }

      const normalized = normalizeItems(payload?.items);
      if (!normalized.ok) {
        return badRequest(normalized.error);
      }

      const id = makeReceiptId();
      const createdAt = Date.now();
      const totals = computeTotals(normalized.items, taxRate);
      const receipt = {
        id,
        merchant,
        currency,
        tax_rate: taxRate,
        items: normalized.items,
        subtotal: totals.subtotal,
        tax: totals.tax,
        total: totals.total,
        notes: payload?.notes ? String(payload.notes) : "",
        created_at_ms: createdAt,
      };

      await store.set(id, receipt);
      return json({ ok: true, receipt }, 201);
    }

    if (method === "GET" && path === "/receipts") {
      const limit = Number(url.searchParams.get("limit") || 20);
      const receipts = await store.list(limit);
      return json({ ok: true, count: receipts.length, receipts });
    }

    if (path.startsWith("/receipts/")) {
      const id = path.slice("/receipts/".length).trim();
      if (!id) {
        return badRequest("missing receipt id");
      }

      if (method === "GET") {
        const receipt = await store.get(id);
        if (!receipt) {
          return notFound();
        }
        return json({ ok: true, receipt });
      }

      if (method === "DELETE") {
        const existing = await store.get(id);
        if (!existing) {
          return notFound();
        }
        await store.delete(id);
        return json({ ok: true, deleted: id });
      }
    }

    return json({ ok: false, error: "route not found" }, 404);
  },
};
