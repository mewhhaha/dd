declare module "virtual:dd-react-router-rsc-server" {
  const server: {
    fetch(request: Request, env?: unknown): Promise<Response>;
  };
  export default server;
}
