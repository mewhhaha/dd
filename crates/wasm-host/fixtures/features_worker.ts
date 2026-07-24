// Exercises classes, array sort with comparator, JSON.parse, and string ops
// through the dd wasm host bridge.
declare function dd_register(
  fetchHandler: (method: string, url: string, body: string) => unknown,
): void;
declare function dd_json(value: unknown): string;

class Greeter {
  prefix: string;
  constructor(prefix: string) {
    this.prefix = prefix;
  }
  greet(name: string): string {
    return this.prefix + ", " + name;
  }
}

dd_register((method: string, url: string, body: string) => {
  const greeter = new Greeter("hey");
  const parsed = JSON.parse(body === "" ? "{}" : body);
  const numbers = [3, 1, 2];
  numbers.sort((a: number, b: number) => a - b);
  return {
    status: 200,
    headers: { "x-greet": greeter.greet("dd") },
    body: dd_json({ sorted: numbers, parsed: parsed, upper: "abc".toUpperCase() }),
  };
});
