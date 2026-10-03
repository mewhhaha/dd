import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

const repoRoot = fileURLToPath(new URL("../", import.meta.url));
const packageRoot = join(repoRoot, "packages/dd-vite");
const peerRequire = createRequire(join(repoRoot, "examples/vite-react-router-rsc/package.json"));
const runFile = promisify(execFile);
const temporary = await mkdtemp(join(tmpdir(), "dd-vite-exports-"));

try {
  const archives = join(temporary, "archives");
  await runFile("pnpm", ["--dir", packageRoot, "pack", "--pack-destination", archives], { cwd: repoRoot });
  const manifest = JSON.parse(await readFile(join(packageRoot, "package.json"), "utf8"));
  const packedRoot = join(temporary, "node_modules", manifest.name);
  await mkdir(packedRoot, { recursive: true });
  const archive = join(archives, `${manifest.name.replace(/^@/, "").replace("/", "-")}-${manifest.version}.tgz`);
  await runFile("tar", ["-xzf", archive, "-C", packedRoot, "--strip-components=1"]);
  const viteRequire = createRequire(peerRequire.resolve("vite/package.json"));
  for (const peer of ["vite", "vitest", "@react-router/dev", "@vitejs/plugin-rsc", "@types/node"]) {
    const destination = join(temporary, "node_modules", peer);
    await mkdir(dirname(destination), { recursive: true });
    const resolvePeer = peer === "@types/node" ? viteRequire : peerRequire;
    await symlink(dirname(resolvePeer.resolve(`${peer}/package.json`)), destination, "dir");
  }
  await writeFile(join(temporary, "package.json"), '{"type":"module"}\n');
  const checks = [];
  for (const [index, [subpath, entry]] of Object.entries(manifest.exports).entries()) {
    await readFile(join(packedRoot, entry.types));
    const actual = await import(pathToFileURL(join(packedRoot, entry.default)));
    const specifier = manifest.name + (subpath === "." ? "" : subpath.slice(1));
    const expected = Object.keys(actual).sort().map((name) => JSON.stringify(name)).join(" | ");
    checks.push(
      `import * as entry${index} from ${JSON.stringify(specifier)};`,
      `type Keys${index} = keyof typeof entry${index};`,
      `type Expected${index} = ${expected};`,
      `const parity${index}: [Keys${index}] extends [Expected${index}] ? ([Expected${index}] extends [Keys${index}] ? true : false) : false = true;`,
      `void parity${index};`,
    );
    console.log(`${specifier}: packed JavaScript entrypoint imports successfully`);
  }
  checks.push(
    `import { DD_CONFIG_SCHEMA_VERSION } from ${JSON.stringify(manifest.name)};`,
    "const schemaVersion: 1 = DD_CONFIG_SCHEMA_VERSION; void schemaVersion;",
    "// @ts-expect-error runtime has no default export",
    `import runtimeDefault from ${JSON.stringify(`${manifest.name}/runtime`)};`,
    "// @ts-expect-error vitest exposes only the worker test helper",
    `import { ddVitePlugin } from ${JSON.stringify(`${manifest.name}/vitest`)};`,
    "// @ts-expect-error environment exposes only its default environment object",
    `import { createDdRuntime } from ${JSON.stringify(`${manifest.name}/vitest-environment`)};`,
    "void runtimeDefault; void ddVitePlugin; void createDdRuntime;",
    `import type { DdGeneratedDeploymentConfigOptions } from ${JSON.stringify(manifest.name)};`,
    "const inlineConfig: DdGeneratedDeploymentConfigOptions = { input: { name: 'inline-worker', config: { public: true } } };",
    "const asyncConfig: DdGeneratedDeploymentConfigOptions = { input: async () => ({ config: { public: true } }) };",
    "// @ts-expect-error inline config inputs reject unknown fields",
    "const misspelledConfig: DdGeneratedDeploymentConfigOptions = { input: { publik: true } };",
    "// @ts-expect-error function config inputs reject unsupported schema versions",
    "const invalidVersion: DdGeneratedDeploymentConfigOptions = { input: () => ({ schema_version: 99 }) };",
    "void inlineConfig; void asyncConfig; void misspelledConfig; void invalidVersion;",
  );
  const source = join(temporary, "exports.ts");
  await writeFile(source, `${checks.join("\n")}\n`);
  await writeFile(join(temporary, "tsconfig.json"), JSON.stringify({
    compilerOptions: { noEmit: true, strict: true, skipLibCheck: true, module: "NodeNext", moduleResolution: "NodeNext", target: "ES2022", types: ["node"] },
    files: [source],
  }));
  const compiler = join(dirname(peerRequire.resolve("typescript/package.json")), "bin/tsc");
  try {
    await runFile(process.execPath, [compiler, "-p", join(temporary, "tsconfig.json")], { cwd: temporary });
  } catch (error) {
    throw new Error(`Packed package type exports do not match JavaScript exports:\n${error.stdout ?? ""}${error.stderr ?? ""}`, { cause: error });
  }
  assert.equal((await import(pathToFileURL(join(packedRoot, "src/index.js")))).DD_CONFIG_SCHEMA_VERSION, 1);
  console.log("dd-vite: all packed type exports match JavaScript exports");
} finally {
  await rm(temporary, { recursive: true, force: true });
}
