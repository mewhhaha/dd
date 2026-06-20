export function currentRoutePath(request: Request) {
  const pathname = new URL(request.url).pathname;
  if (pathname === "/_.data") {
    return "/";
  }
  if (pathname.endsWith(".data")) {
    const routePath = pathname.slice(0, -".data".length);
    return routePath === "" ? "/" : routePath;
  }
  return pathname;
}

export function isReactRouterDataRequest(request: Request) {
  const pathname = new URL(request.url).pathname;
  return pathname === "/_.data" || pathname.endsWith(".data");
}
