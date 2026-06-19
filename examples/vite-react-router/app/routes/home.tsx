import { Link } from "react-router";

export function meta() {
  return [{ title: "React Router on dd" }];
}

export default function Home() {
  return (
    <section data-route="home">
      <h1>React Router on dd</h1>
      <p>
        This example uses React Router framework mode, then routes Vite development requests
        through the local dd runtime.
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
