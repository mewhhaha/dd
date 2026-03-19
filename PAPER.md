# Tiny Workers Platform

A tiny workers platform built in Rust with Deno/V8 isolates and Turso.

The goal is simple: deploy a single JavaScript file, route HTTP requests to it, run it in a fresh isolate, and return a response.

This is intentionally small and grug-brained.

## Scope

This project supports:

* single-file JavaScript workers
* deploying workers through an HTTP API
* mapping host + path prefixes to workers
* running each request in a fresh Deno/V8 runtime
* storing platform metadata in Turso via libSQL

This project does **not** support:

* TypeScript
* npm
* Node compatibility
* multi-file workers
* dynamic imports
* filesystem access from workers
* subprocesses
* secrets management
* warm isolate pooling
* snapshots
* cron
* queues
* websockets
* auth

## Worker contract

Each worker is a single JavaScript file.

It must export a default object with a `fetch` method.

Example:

```js
export default {
  async fetch(request, env, ctx) {
    return new Response("hello from worker");
  },
};
```

The runtime will call:

```js
default.fetch(request, env, ctx)
```

For the initial version:

* `request` is a web-standard `Request`
* `env` is an empty object
* `ctx` is a plain object with request metadata, for example:

```js
{ requestId: "..." }
```

## High-level architecture

The first version is one Rust service.

It has four small responsibilities:

* `api`: HTTP server and endpoints
* `db`: Turso/libSQL access
* `runtime`: Deno/V8 worker execution
* `common`: shared types

Suggested workspace layout:

```text
/crates
  /api
  /runtime
  /db
  /common
```

Request flow:

1. Receive an incoming HTTP request.
2. Match route using `host` + longest `path_prefix`.
3. Find the worker's active deployment in Turso.
4. Create a fresh `JsRuntime`.
5. Load the worker source as a single module.
6. Validate and invoke `default.fetch(request, env, ctx)`.
7. Convert the returned `Response` back into a Rust HTTP response.
8. Return it to the client.

## Tech stack

* Rust
* axum for HTTP
* deno_core for V8 embedding
* minimal Deno web APIs for `Request` and `Response`
* Turso Rust client

## Database schema

Use Turso to store worker metadata, deployments, and routes.

```sql
create table workers (
  id text primary key,
  name text not null unique,
  created_at integer not null
);

create table deployments (
  id text primary key,
  worker_id text not null,
  source_js text not null,
  status text not null,
  created_at integer not null,
  foreign key (worker_id) references workers(id)
);

create table active_deployments (
  worker_id text primary key,
  deployment_id text not null,
  foreign key (worker_id) references workers(id),
  foreign key (deployment_id) references deployments(id)
);

create table routes (
  id text primary key,
  host text not null,
  path_prefix text not null,
  worker_id text not null,
  unique(host, path_prefix),
  foreign key (worker_id) references workers(id)
);
```

## API

### `POST /deploy`

Deploy a worker.

Request body:

```json
{
  "name": "hello",
  "source": "export default { async fetch(request, env, ctx) { return new Response('hi') } }"
}
```

Behavior:

* create the worker record if it does not exist
* validate the source in the runtime
* ensure default export exists
* ensure default export is an object
* ensure `default.fetch` is a function
* store a new deployment
* mark the deployment active for that worker name

Response:

```json
{
  "ok": true,
  "worker": "hello",
  "deployment_id": "..."
}
```

Possible error messages:

* `Worker must export default`
* `Default export must be an object`
* `Default export must define fetch(request, env, ctx)`

### `POST /route`

Create or update a route.

Request body:

```json
{
  "host": "localhost",
  "path_prefix": "/hello",
  "worker_name": "hello"
}
```

Behavior:

* resolve `worker_name` to a worker
* store route mapping from `host + path_prefix` to worker

Response:

```json
{
  "ok": true
}
```

### Public request handling

Any normal incoming request should:

* read the request host
* find routes for that host
* choose the longest matching `path_prefix`
* resolve the active deployment
* run the worker
* return its `Response`

## Runtime design

Keep the runtime intentionally dumb.

### Supported JS shape

* one JS module only
* no imports
* no dynamic imports
* no module graph

Load the uploaded source as a single synthetic module, for example `worker:main`.

### Minimal web APIs

Expose only the minimum needed to support:

* `Request`
* `Response`
* `Headers`
* `URL`

### Runtime policy

For the MVP:

* create a fresh runtime for every request
* do not optimize startup yet
* do not pool isolates yet

That is slower, but much easier to reason about.

### Security posture

Workers should **not** get access to:

* filesystem
* subprocesses
* raw environment variables
* raw sockets
* Node APIs
* arbitrary imports

## Validation rules

When a worker is deployed:

1. compile the module
2. evaluate the module
3. read the default export
4. ensure it exists
5. ensure it is an object
6. ensure `fetch` is a function

Reject invalid workers with plain errors.

## Suggested crate responsibilities

### `crates/api`

Owns:

* axum server
* `POST /deploy`
* `POST /route`
* public request entrypoint
* route matching
* request/response serialization

### `crates/runtime`

Owns:

* Deno/V8 runtime setup
* module loading
* worker validation
* request conversion
* response conversion
* invocation of `default.fetch(request, env, ctx)`

### `crates/db`

Owns:

* Turso/libSQL connection setup
* schema initialization
* CRUD for workers
* CRUD for deployments
* CRUD for routes
* active deployment lookup

### `crates/common`

Owns:

* shared request/response types
* deployment types
* route types
* error types

## Recommended project behavior

### Route matching

Match routes by:

* exact host
* longest matching `path_prefix`

Example:

* `/` matches everything
* `/hello` beats `/`

### Worker naming

Treat worker names as stable identifiers.

A worker can have many deployments, but only one active deployment.

### Status values

Use simple deployment states:

* `ready`
* `failed`

A deployment should only become active if validation succeeds.

## Local development

## Prerequisites

* Rust toolchain
* a Turso database
* Turso database URL
* Turso auth token

## Environment variables

Use environment variables like:

```bash
export TURSO_DATABASE_URL="file:./grugd-kv.db"
export TURSO_AUTH_TOKEN="your-token"
export RUST_LOG="info"
```

You may also want:

```bash
export BIND_ADDR="127.0.0.1:3000"
```

## Run the server

```bash
cargo run -p api
```

If schema initialization is done on startup, the app should create the required tables automatically.

If migrations are separate, run them first and then start the server.

## Example worker

Save this as `hello.js`:

```js
export default {
  async fetch(request, env, ctx) {
    return new Response("hello from worker");
  },
};
```

## Deploy a worker

```bash
curl -X POST http://localhost:3000/deploy \
  -H "content-type: application/json" \
  -d @- <<'JSON'
{
  "name": "hello",
  "source": "export default { async fetch(request, env, ctx) { return new Response('hello from worker'); } }"
}
JSON
```

Expected response:

```json
{
  "ok": true,
  "worker": "hello",
  "deployment_id": "..."
}
```

## Create a route

```bash
curl -X POST http://localhost:3000/route \
  -H "content-type: application/json" \
  -d @- <<'JSON'
{
  "host": "localhost",
  "path_prefix": "/hello",
  "worker_name": "hello"
}
JSON
```

Expected response:

```json
{
  "ok": true
}
```

## Test the worker

```bash
curl http://localhost:3000/hello
```

Expected response:

```text
hello from worker
```

## Acceptance test

The MVP is complete when this works:

1. Start the server.
2. Deploy this worker:

```js
export default {
  async fetch(request, env, ctx) {
    return new Response("hello from worker");
  },
};
```

3. Create route:

   * host: `localhost`
   * path_prefix: `/hello`
   * worker_name: `hello`

4. Run:

```bash
curl http://localhost:3000/hello
```

5. Confirm the response body is:

```text
hello from worker
```

## Implementation order

A good order for implementation is:

1. Set up Rust workspace and crates.
2. Add Turso/libSQL connection and schema setup.
3. Build the runtime spike:

   * hardcoded worker source
   * hardcoded `Request`
   * execute `default.fetch`
   * get back `Response`
4. Add `POST /deploy`.
5. Add validation during deploy.
6. Add active deployment lookup.
7. Add `POST /route`.
8. Add public request routing.
9. Add basic error handling and logging.

## Nice-to-have after MVP

Only do these after the acceptance test passes:

* in-memory route cache
* per-request timeout
* request ID logging
* health endpoint
* better JSON errors

## Non-goals

Do not add these yet:

* auth
* admin UI
* secrets store
* warm isolate pools
* snapshots
* streaming request/response bodies
* background jobs
* cron
* queues
* websockets
* multi-tenant policies
* per-worker networking rules

## Notes for the implementer

Keep the first version extremely boring.

Do not over-design the runtime.
Do not add import support.
Do not add TypeScript.
Do not add clever abstractions before the request path works.

The whole point is:

* deploy one JS file
* map a route to it
* run it in a fresh isolate
* return a response

## Short implementation brief

```text
Build a tiny workers platform in Rust with axum, deno_core, and Turso.

Workers are single JS files that look like:

export default {
  async fetch(request, env, ctx) {
    return new Response("hello");
  },
};

Implement:
- POST /deploy with {name, source}
- POST /route with {host, path_prefix, worker_name}
- public HTTP routing that runs the active worker for the matched route

Store metadata in Turso:
- workers
- deployments
- active_deployments
- routes

For each request:
- match route by host + longest path_prefix
- resolve active deployment
- create a fresh JsRuntime
- load the worker as one module
- validate default export and fetch()
- call default.fetch(request, env, ctx)
- convert returned Response back to Rust HTTP response

No TypeScript, no npm, no imports, no Node compat, no filesystem, no subprocesses.

Ship a working local MVP, schema setup, sample worker, and curl-based README instructions.
```

## License

Choose whatever you want for the repo. MIT or Apache-2.0 would both be fine for a starter project.
