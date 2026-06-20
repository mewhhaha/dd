import { Link, type LoaderFunctionArgs } from "react-router";
import { getDdRequestContext } from "../dd-context";

export function meta({ params }: { params: { slug?: string } }) {
  return [{ title: `Project ${params.slug ?? "unknown"}` }];
}

export async function loader({ context, params, request }: LoaderFunctionArgs) {
  const dd = getDdRequestContext(context, request);
  return {
    slug: params.slug ?? "unknown",
    stmCount: await dd.incrementStmRequestCount(),
    workerName: dd.workerName,
  };
}

type LoaderData = Awaited<ReturnType<typeof loader>>;

export default function Project({
  loaderData,
  params,
}: {
  loaderData: LoaderData;
  params: { slug?: string };
}) {
  const slug = params.slug ?? "unknown";
  return (
    <section data-route="project" data-path={`/projects/${slug}`}>
      <h1>{slug}</h1>
      <p>
        This page is rendered by the React Router framework server build inside the dd worker
        runtime.
      </p>
      <p data-stm-count={loaderData.stmCount}>
        dd worker <code>{loaderData.workerName}</code> incremented STM in this route loader:
        {" "}
        <code>{loaderData.stmCount}</code>
      </p>
      <p>
        Request path: <code>/projects/{slug}</code>
      </p>
      <div className="actions">
        <Link className="button" to="/">
          Home
        </Link>
      </div>
    </section>
  );
}
