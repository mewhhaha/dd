"use strict";

const RUNTIME_PACKAGES = {
  "darwin:arm64": "@mewhhaha/dd-darwin-arm64",
  "darwin:x64": "@mewhhaha/dd-darwin-x64",
  "linux:arm64": "@mewhhaha/dd-linux-arm64",
  "linux:x64": "@mewhhaha/dd-linux-x64",
  "win32:x64": "@mewhhaha/dd-win32-x64",
};

function runtimePackageName(options = {}) {
  const platform = options.platform ?? process.platform;
  const arch = options.arch ?? process.arch;
  const packageName = RUNTIME_PACKAGES[`${platform}:${arch}`];
  if (!packageName) {
    throw new Error(`Unsupported dd runtime platform: ${platform}/${arch}`);
  }
  return packageName;
}

function runtimeBinaryName(options = {}) {
  const platform = options.platform ?? process.platform;
  return platform === "win32" ? "dd_dev_runtime.exe" : "dd_dev_runtime";
}

function runtimeBinaryPath(options = {}) {
  const packageName = runtimePackageName(options);
  const binaryName = runtimeBinaryName(options);
  try {
    return require.resolve(`${packageName}/bin/${binaryName}`);
  } catch (error) {
    throw new Error(
      `The optional runtime package ${packageName} is not installed or does not contain ${binaryName}: ${error.message}`,
    );
  }
}

module.exports = {
  RUNTIME_PACKAGES,
  runtimeBinaryName,
  runtimeBinaryPath,
  runtimePackageName,
};
