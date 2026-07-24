// Minimal deno_net compatibility layer for deno_fetch request/http-client modules.
// We only support the subset needed for Request construction in worker runtimes.

function loadTlsKeyPair(api, { keyFormat, cert, key } = { __proto__: null }) {
  if (keyFormat !== undefined && keyFormat !== "pem") {
    throw new TypeError(
      `If "keyFormat" is specified, it must be "pem": received "${keyFormat}"`,
    );
  }
  if (cert !== undefined && key === undefined) {
    throw new TypeError(
      `If \`cert\` is specified, \`key\` must be specified as well for \`${api}\``,
    );
  }
  if (cert === undefined && key !== undefined) {
    throw new TypeError(
      `If \`key\` is specified, \`cert\` must be specified as well for \`${api}\``,
    );
  }
  if (cert !== undefined) {
    throw new TypeError(
      `TLS client certificates are not supported in this runtime for \`${api}\``,
    );
  }
  return null;
}

export { loadTlsKeyPair };
