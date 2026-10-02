import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { checkReleaseVersion } from "./check-release-version.mjs";

test("release versions require matching tags, Rust and every public npm package", async () => {
  const root = await mkdtemp(join(tmpdir(), "dd-release-version-"));
  try {
    await mkdir(join(root, "packages", "runtime"), { recursive: true });
    await writeFile(join(root, "Cargo.toml"), '[workspace.package]\nversion = "0.1.0"\n[dependencies]\n');
    const manifest = join(root, "packages", "runtime", "package.json");
    await writeFile(manifest, JSON.stringify({ name: "dd-runtime", version: "0.1.0" }));
    assert.equal(await checkReleaseVersion(root, "v0.1.0"), "0.1.0");
    assert.equal(await checkReleaseVersion(root), "0.1.0");
    await assert.rejects(checkReleaseVersion(root, "v0.2.0"), /tag.*must match/);
    await assert.rejects(checkReleaseVersion(root, "0.1.0"), /tag.*must match/);
    await writeFile(manifest, JSON.stringify({ name: "dd-runtime", version: "0.2.0" }));
    await assert.rejects(checkReleaseVersion(root, "v0.1.0"), /dd-runtime version.*must match/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
