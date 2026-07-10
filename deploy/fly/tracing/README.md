# Fly.io Traces (Optional)

This optional app runs Jaeger all-in-one:

- private UI on `16686`
- OTLP HTTP/protobuf receiver on `4318`

If you choose to use this app, point `OTEL_EXPORTER_OTLP_ENDPOINT` at:

```bash
http://dd-traces-8956e096.internal:4318
```

After verifying that the private collector is reachable, also set
`DD_OTEL_COLLECTOR_VERIFIED=true` on `dd_server`. An endpoint without this
explicit verification leaves the exporter disabled and reports `unverified` in
the admin status.

The tracing app has no public Fly service. Open the UI through a private-network
connection or a temporary `flyctl proxy 16686:16686 --app <tracing-app>` tunnel.
Leave the exporter variable unset when no collector is running.

For the default single-app setup, skip this app and use
`examples/trace-hub.js` inside the worker platform instead.
