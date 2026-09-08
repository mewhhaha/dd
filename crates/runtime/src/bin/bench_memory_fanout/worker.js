const payloads = new Map();
function payloadFor(entity, field) {
  const id = `${entity}:${field}`;
  if (payloads.has(id)) return payloads.get(id);
  let payload;
  if (payloadKind === "repeated") {
    payload = (field === 0 ? String(entity) : id).padEnd(payloadBytes, "x").slice(0, payloadBytes);
  } else {
    let seed = (Math.imul(entity + 1, 2654435761) ^ Math.imul(field + 1, 2246822519)) >>> 0;
    const chars = new Array(payloadBytes);
    for (let index = 0; index < payloadBytes; index++) {
      seed ^= seed << 13;
      seed ^= seed >>> 17;
      seed ^= seed << 5;
      chars[index] = String.fromCharCode(33 + ((seed >>> 0) % 90));
    }
    payload = chars.join("");
  }
  // Bound the workload's own expected-value cache independently of service caches.
  if (payloads.size >= 4096) payloads.clear();
  payloads.set(id, payload);
  return payload;
}

export default {
  async fetch(request, env) {
    if (request.method === "GET") {
      if (request.url.endsWith("/profile/reset")) {
        Deno.core.ops.op_memory_profile_reset();
        return Response.json({ ok: true });
      }
      return Response.json(Deno.core.ops.op_memory_profile_take());
    }
    const { operation, sequence, entities } = await request.json();
    const results = await Promise.all(entities.map((entity) => {
      const expectedPayload = payloadFor(entity, 0);
      const method = readApi === "snapshot" && (operation === "read" || operation === "verify")
        ? "read" : "atomic";
      return env.MEMORY.get(`entity-${entity}`)[method]((tx) => {
        const state = tx.get("state");
        if (operation === "seed") {
          if (state !== null) throw new Error(`entity ${entity} was already seeded`);
          tx.put("state", { count: 0, payload: expectedPayload });
          for (let field = 1; field < keysPerEntity; field++) {
            tx.put(`field-${field}`, payloadFor(entity, field));
          }
          return { entity, count: 0, payload_valid: true };
        }
        if (!state || !Number.isSafeInteger(state.count) || state.count < 0) {
          throw new Error(`entity ${entity} has invalid state: ${JSON.stringify(state)}`);
        }
        if (state.payload !== expectedPayload) {
          throw new Error(`entity ${entity} payload does not match ${payloadBytes} initialized bytes`);
        }
        if (keysPerEntity > 1) {
          const selected = 1 + sequence % (keysPerEntity - 1);
          const first = operation === "verify" ? 1 : selected;
          const last = operation === "verify" ? keysPerEntity - 1 : selected;
          for (let field = first; field <= last; field++) {
            if (tx.get(`field-${field}`) !== payloadFor(entity, field)) {
              throw new Error(`entity ${entity} field ${field} payload changed`);
            }
          }
        }
        if (operation === "write") {
          const next = { count: state.count + 1, payload: state.payload };
          tx.put("state", next);
          return { entity, count: next.count, payload_valid: next.payload === expectedPayload };
        }
        return { entity, count: state.count, payload_valid: state.payload === expectedPayload };
      });
    }));
    return Response.json({ sequence, results });
  },
};
