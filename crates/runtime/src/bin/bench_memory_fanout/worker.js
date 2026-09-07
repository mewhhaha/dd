export default {
  async fetch(request, env) {
    const { operation, sequence, entities } = await request.json();
    const results = await Promise.all(entities.map((entity) => {
      const expectedPayload = String(entity).padEnd(payloadBytes, "x").slice(0, payloadBytes);
      return env.MEMORY.get(`entity-${entity}`).atomic((tx) => {
        const state = tx.get("state");
        if (operation === "seed") {
          if (state !== null) throw new Error(`entity ${entity} was already seeded`);
          tx.put("state", { count: 0, payload: expectedPayload });
          return { entity, count: 0, payload_valid: true };
        }
        if (!state || !Number.isSafeInteger(state.count) || state.count < 0) {
          throw new Error(`entity ${entity} has invalid state: ${JSON.stringify(state)}`);
        }
        if (state.payload !== expectedPayload) {
          throw new Error(`entity ${entity} payload does not match ${payloadBytes} initialized bytes`);
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
