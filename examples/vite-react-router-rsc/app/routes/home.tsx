import { Link } from "react-router";

export function meta() {
  return [{ title: "React Router RSC on dd" }];
}

export async function ServerComponent() {
  return (
    <section data-route="home">
      <h1>RSC on dd</h1>
      <p>
        This example uses React Router RSC framework mode with server route components
        running through Vite's experimental RSC pipeline.
      </p>
      <div className="actions">
        <Link className="button primary" to="/projects/runtime">
          Open runtime route
        </Link>
        <a className="button" href="/@vite/client">
          Vite client
        </a>
      </div>
    </section>
  );
}
