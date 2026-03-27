/**
 * Dynamic worker config types for JS hover/tooling in examples.
 */

export declare class RpcTarget {}

/**
 * Env values allowed in dynamic worker config.
 * - Primitive values are exposed to the child worker env as opaque placeholders.
 * - RpcTarget values are exposed as host RPC bindings.
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
}
