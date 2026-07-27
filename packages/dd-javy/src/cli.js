#!/usr/bin/env node

import { buildJavyWorker } from "./build.js";

const args = process.argv.slice(2);
if (args.length !== 2) {
  console.error("usage: dd-javy-build <worker.ts|worker.js> <worker.wasm>");
  process.exitCode = 2;
} else {
  try {
    const result = await buildJavyWorker(args[0], args[1]);
    console.log(`built ${result.output} (${result.bytes} bytes)`);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
