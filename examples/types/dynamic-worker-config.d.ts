/**
 * Dynamic worker config types for JS hover/tooling in examples.
 */

export declare class RpcTarget {}

/**
 * Env values allowed in dynamic worker config.
 * - Primitive values are exposed to the child worker env as opaque placeholders.
 * - RpcTarget values are exposed as host RPC bindings when allow_host_rpc is true.
 */
export type DynamicEnvValue =
  | string
  | number
  | boolean
  | null
  | undefined
  | RpcTarget;

/**
 * Config used by env.SANDBOX.get(id, factory).
 */
export interface DynamicWorkerConfig {
  /**
   * Full worker source code.
   * If omitted, entrypoint + modules is used.
   */
  source?: string;

  /**
   * Entrypoint module path when using modules.
   * Defaults to worker.js.
   */
  entrypoint?: string;

  /**
   * Module graph map: modulePath -> source.
   */
  modules?: Record<string, string>;

  /**
   * Child env bindings.
   */
  env?: Record<string, DynamicEnvValue>;

  /**
   * Per-invoke timeout in milliseconds.
   * Default: 5000
   * Max: 60000
   */
  timeout?: number;

  /**
   * Outbound fetch allowlist.
   * Exact host, host:port, *.suffix, or *.suffix:port.
   * Default: deny all outbound fetch.
   */
  egress_allow_hosts?: string[];

  /**
   * Allow RpcTarget env bindings.
   * Default: false
   */
  allow_host_rpc?: boolean;

  /**
   * Allow websocket upgrade responses from child.
   * Default: false
   */
  allow_websocket?: boolean;

  /**
   * Allow transport upgrade responses from child.
   * Default: false
   */
  allow_transport?: boolean;

  /**
   * Allow stateful runtime bindings like cache.
   * Default: false
   */
  allow_state_bindings?: boolean;

  /**
   * Max inbound request bytes per child invoke.
   * Default: 1048576
   */
  max_request_bytes?: number;

  /**
   * Max outbound response bytes per child invoke.
   * Default: 2097152
   */
  max_response_bytes?: number;

  /**
   * Max outbound host fetch count across child lifetime.
   * Default: 16
   */
  max_outbound_requests?: number;

  /**
   * Max concurrent inflight invokes for child.
   * Default: 32
   */
  max_concurrency?: number;
}
