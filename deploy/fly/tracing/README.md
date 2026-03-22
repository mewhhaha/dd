# Fly.io Traces (Optional)

This optional app runs Jaeger all-in-one:

- public UI on `16686`
- OTLP gRPC receiver on `4317`

If you choose to use this app, point `OTEL_EXPORTER_OTLP_ENDPOINT` at:

```bash
http://dd-traces-8956e096.internal:4317
```

For the default single-app setup, skip this app and use `examples/trace-hub.js` inside the worker platform instead.
