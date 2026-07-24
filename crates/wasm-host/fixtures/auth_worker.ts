// Minimal service worker reached through dd_service_fetch.
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;

dd_register((method: string, url: string, body: string) => {
  return { status: 200, headers: { "x-service": "auth" }, body: "session:ok" };
});
