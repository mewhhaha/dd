declare module "@react-router/dev/config/default-rsc-entries/entry.rsc" {
  import type { RouterContextProvider } from "react-router";

  const entry: {
    fetch(request: Request, requestContext?: RouterContextProvider): Promise<Response>;
  };

  export default entry;
}
