// Promise-returning handlers: dd_sleep and native fetch both hand back real
// pending promises that the engine's event loop resolves.
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_sleep(ms: number): any;

dd_register((method: string, url: string, body: string) => {
  const parsed = new URL(url);
  if (parsed.pathname === "/proxy") {
    return fetch(body)
      .then((response: any) => response.text())
      .then((text: any) => {
        return {
          status: 200,
          headers: { "x-via": "native-fetch" },
          body: "upstream said: " + text,
        };
      });
  }
  return dd_sleep(20).then(() => {
    return { status: 201, headers: { "x-async": "yes" }, body: "slept:" + method };
  });
});
