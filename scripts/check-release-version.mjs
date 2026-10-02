import { readFile, readdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export async function checkReleaseVersion(root, tag) {
  const cargo = await readFile(join(root, "Cargo.toml"), "utf8");
  const workspace = cargo.split("[workspace.package]")[1]?.split(/\n\[/)[0];
  const version = workspace?.match(/^version\s*=\s*"([^"]+)"/m)?.[1];
  if (!version) throw new Error("Cargo.toml must declare workspace.package.version");
  if (tag && tag !== `v${version}`) {
    throw new Error(`Release tag ${tag} must match workspace version v${version}`);
  }
  const manifests = [];
  for (const entry of await readdir(join(root, "packages"), { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const file = join(root, "packages", entry.name, "package.json");
    let manifest;
    try {
      manifest = JSON.parse(await readFile(file, "utf8"));
    } catch (error) {
      if (error.code === "ENOENT") continue;
      throw error;
    }
    if (manifest.private) continue;
    if (manifest.version !== version) {
      throw new Error(`${manifest.name} version ${manifest.version} must match workspace version ${version}`);
    }
    manifests.push(manifest);
  }
  if (manifests.length === 0) throw new Error("No publishable package manifests found");
  return version;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const version = await checkReleaseVersion(root, process.argv[2]);
  console.log(`Rust and all publishable npm packages agree on ${version}${process.argv[2] ? ` (${process.argv[2]})` : ""}`);
}
