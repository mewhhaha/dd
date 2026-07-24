// KV-backed hit counter (README example, wasm edition).
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_kv_get(binding: string, key: string): string | null;
declare function dd_kv_set(binding: string, key: string, value: string): void;

dd_register((method: string, url: string, body: string) => {
  const current = dd_kv_get("MY_KV", "hits");
  const hits = (current === null ? 0 : parseInt(current, 10)) + 1;
  dd_kv_set("MY_KV", "hits", "" + hits);
  return {
    status: 200,
    headers: { "content-type": "text/plain; charset=utf-8" },
    body: "hits=" + hits,
  };
});
