import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { cpus, tmpdir, totalmem } from "node:os";
import { join } from "node:path";

export function collectMetadata(repoRoot, env = process.env) {
  const command = (program, args) => {
    const result = spawnSync(program, args, { cwd: repoRoot, encoding: "utf8" });
    if (result.status !== 0) {
      throw new Error(`benchmark metadata command ${program} ${args.join(" ")} failed: ${result.stderr ?? result.error}`);
    }
    return result.stdout.trim();
  };
  const optionalFile = (path) => {
    try {
      return readFileSync(path, "utf8").trim();
    } catch (error) {
      if (error.code === "ENOENT") return "unavailable";
      throw error;
    }
  };
  const cgroup = optionalFile("/proc/self/cgroup").match(/^0::(.*)$/m)?.[1];
  const cgroupRoot = join("/sys/fs/cgroup", cgroup ?? "");
  return {
    git_commit: command("git", ["rev-parse", "HEAD"]),
    git_dirty: command("git", ["status", "--porcelain", "--untracked-files=normal"]) !== "",
    rustc: command("rustc", ["--version", "--verbose"]),
    cargo: command("cargo", ["--version"]),
    os: command("uname", ["-a"]),
    logical_cpus: cpus().length,
    cpu_model: cpus()[0]?.model ?? "unavailable",
    cpu_affinity: optionalFile("/proc/self/status").match(/^Cpus_allowed_list:\s*(.+)$/m)?.[1] ?? "unavailable",
    memory_bytes: totalmem(),
    memory_limit: optionalFile(join(cgroupRoot, "memory.max")),
    cpu_limit: optionalFile(join(cgroupRoot, "cpu.max")),
    disk: command("df", ["-PT", repoRoot, tmpdir()]).split("\n").slice(1).map((line) => {
      const columns = line.trim().split(/\s+/);
      return { device: columns[0], type: columns[1], mount: columns.at(-1) };
    }),
    build: Object.fromEntries(Object.entries(env).filter(([key]) =>
      /^(RUSTFLAGS|CARGO_ENCODED_RUSTFLAGS|RUSTC_WRAPPER|CARGO_BUILD_TARGET|CARGO_PROFILE_.*)$/.test(key))),
  };
}
