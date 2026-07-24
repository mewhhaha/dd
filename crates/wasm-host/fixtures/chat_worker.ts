// Websocket chat worker: broadcasts messages to every open connection.
// Module-level state spans connections because all websocket events for a
// worker run on one dedicated instance.
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_ws_register(handlers: unknown): void;
declare function dd_ws_send(connection: number, data: string): boolean;
declare function dd_ws_close(connection: number): void;

const connections: number[] = [];

dd_register((method: string, url: string, body: string) => {
  return { status: 200, headers: {}, body: "chat:" + connections.length };
});

dd_ws_register({
  open: (connection: number, url: string) => {
    connections.push(connection);
    dd_ws_send(connection, "welcome " + connection);
  },
  message: (connection: number, data: string) => {
    if (data === "quit") {
      dd_ws_close(connection);
      return;
    }
    for (const other of connections) {
      dd_ws_send(other, connection + ": " + data);
    }
  },
  close: (connection: number) => {
    const index = connections.indexOf(connection);
    if (index >= 0) {
      connections.splice(index, 1);
    }
  },
});
