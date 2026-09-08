export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = Number(url.searchParams.get("key") ?? 0);
    if (!Number.isInteger(key) || key < 0 || key >= 32) {
      return new Response("invalid SKU", { status: 400 });
    }
    if (url.pathname === "/seed") {
      await env.INVENTORY.get(`sku:${key}`).atomic((stock) => {
        stock.put("available", key + 1);
        stock.put("priceCents", 1000 + key);
        stock.put("description", "x".repeat(1024));
      });
      return new Response("ok");
    }
    if (url.pathname !== "/dashboard") {
      return new Response("not found", { status: 404 });
    }
    const products = await Promise.all(Array.from({ length: 8 }, (_, offset) => {
      const sku = (key + offset) % 32;
      return env.INVENTORY.get(`sku:${sku}`).read((stock) => ({
        sku,
        available: stock.get("available"),
        priceCents: stock.get("priceCents"),
      }));
    }));
    return Response.json({ products });
  },
};
