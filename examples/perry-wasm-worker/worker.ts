// Experimental dd worker for the Perry wasm runtime (crates/wasm-host).
// Compile with: perry compile worker.ts -o worker.wasm --target wasm
//
// The host provides these three functions; everything else is plain TS.
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_header(name: string): string | null;
declare function dd_json(value: unknown): string;

dd_register((method: string, url: string, body: string) => {
  const parsed = new URL(url);
  const name = parsed.pathname === "/" ? "world" : parsed.pathname.slice(1);
  const agent = dd_header("user-agent");
  return {
    status: 200,
    headers: { "content-type": "application/json" },
    body: dd_json({
      greeting: "hello " + name,
      method: method,
      agent: agent,
      echo: body,
    }),
  };
});
