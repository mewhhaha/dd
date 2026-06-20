type ExecutionContext = unknown;

declare class URLPattern {
  constructor(input: { pathname?: string } | string, baseURL?: string);
  exec(input: string | URL): { pathname: { groups: Record<string, string | undefined> } } | null;
}

declare module "fixi-js";
