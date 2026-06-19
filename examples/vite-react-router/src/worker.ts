import {
  createRequestHandler,
  RouterContextProvider,
  UNSAFE_withComponentProps as withComponentProps,
  type ServerBuild,
} from "react-router";
import {
  createDdRequestContext,
  ddRequestContext,
  type Env,
} from "../app/dd-context";
import * as entryServer from "../app/entry.server";
import * as root from "../app/root";
import * as home from "../app/routes/home";
import * as project from "../app/routes/project";

const assets = {
  entry: {
    module: "/@vite/client",
    imports: [],
    css: [],
  },
  routes: {
    root: {
      id: "root",
      path: "",
      hasAction: false,
      hasLoader: false,
      hasClientAction: false,
      hasClientLoader: false,
      hasClientMiddleware: false,
      hasDefaultExport: true,
      hasErrorBoundary: false,
      module: "/app/root.tsx",
      imports: [],
      css: [],
      clientActionModule: undefined,
      clientLoaderModule: undefined,
      clientMiddlewareModule: undefined,
      hydrateFallbackModule: undefined,
    },
    "routes/home": {
      id: "routes/home",
      parentId: "root",
      index: true,
      hasAction: false,
      hasLoader: false,
      hasClientAction: false,
      hasClientLoader: false,
      hasClientMiddleware: false,
      hasDefaultExport: true,
      hasErrorBoundary: false,
      module: "/app/routes/home.tsx",
      imports: [],
      css: [],
      clientActionModule: undefined,
      clientLoaderModule: undefined,
      clientMiddlewareModule: undefined,
      hydrateFallbackModule: undefined,
    },
    "routes/project": {
      id: "routes/project",
      parentId: "root",
      path: "projects/:slug",
      hasAction: false,
      hasLoader: true,
      hasClientAction: false,
      hasClientLoader: false,
      hasClientMiddleware: false,
      hasDefaultExport: true,
      hasErrorBoundary: false,
      module: "/app/routes/project.tsx",
      imports: [],
      css: [],
      clientActionModule: undefined,
      clientLoaderModule: undefined,
      clientMiddlewareModule: undefined,
      hydrateFallbackModule: undefined,
    },
  },
  url: "/__manifest",
  version: "dev",
};

const build = {
  entry: {
    module: entryServer,
  },
  routes: {
    root: {
      id: "root",
      path: "",
      module: {
        ...root,
        default: withComponentProps(root.default),
      },
    },
    "routes/home": {
      id: "routes/home",
      parentId: "root",
      index: true,
      module: {
        ...home,
        default: withComponentProps(home.default),
      },
    },
    "routes/project": {
      id: "routes/project",
      parentId: "root",
      path: "projects/:slug",
      module: {
        ...project,
        default: withComponentProps(project.default),
      },
    },
  },
  assets,
  basename: "/",
  publicPath: "/",
  assetsBuildDirectory: "dist/react-router/client",
  future: {},
  ssr: true,
  isSpaMode: false,
  prerender: [],
  routeDiscovery: {
    mode: "initial",
    manifestPath: "/__manifest",
  },
  allowedActionOrigins: false,
} satisfies ServerBuild;

const handler = createRequestHandler(build, import.meta.env.MODE);

function createRouterContext(env: Env): RouterContextProvider {
  const context = new RouterContextProvider();
  context.set(ddRequestContext, createDdRequestContext(env, "vite-react-router"));
  return context;
}

function withStmHeader(response: Response, context: RouterContextProvider): Response {
  const count = context.get(ddRequestContext).lastStmCount;
  if (count === undefined) {
    return response;
  }
  const headers = new Headers(response.headers);
  headers.set("x-dd-stm-count", String(count));
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export default {
  async fetch(request: Request, env: Env) {
    const context = createRouterContext(env);
    const response = await handler(request, context);
    return withStmHeader(response, context);
  },
};
