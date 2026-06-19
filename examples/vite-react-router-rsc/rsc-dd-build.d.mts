import type { Plugin } from "vite";

export function buildReactRouterRsc(): Promise<void>;
export function bundleDdRscWorker(): Promise<string>;
export function hasReactRouterRscServerOutput(sinceMs?: number): Promise<boolean>;
export function readDdRscWorker(): Promise<string>;
export function writeDdRscBuildArtifacts(): Promise<void>;
export function serveReactRouterRscClientAssets(): Plugin;
