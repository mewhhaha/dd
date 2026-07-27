import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";
import { bundleJavyWorker } from "./build.js";

test("bundles a worker and compatibility runtime without host JavaScript imports", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "dd-javy-test-"));
  const entry = resolve(directory, "worker.js");
  try {
    await writeFile(
      entry,
      `export default {
        async fetch(request) {
          return Response.json({ pathname: new URL(request.url).pathname });
        },
      };`,
    );
    const source = await bundleJavyWorker(entry, { minify: false });

    assert.match(source, /await runWorker\(/);
    assert.match(source, /class RuntimeResponse|RuntimeResponse = class/);
    assert.doesNotMatch(source, /from ["']node:/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
