import { Hono } from "hono";

const app = new Hono();

app.get("/", (context) => context.text("Hello from dd + Hono"));

export default app;
