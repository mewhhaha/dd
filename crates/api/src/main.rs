use common::{PlatformError, Result};
use dd_server::ServerConfig;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::TracerProvider as OTelTracerProvider;
use opentelemetry_sdk::Resource;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let otel_provider = init_tracing()?;

    let bind_public_addr =
        env::var("BIND_PUBLIC_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let public_addr: SocketAddr = bind_public_addr
        .parse()
        .map_err(|error| PlatformError::internal(format!("invalid BIND_PUBLIC_ADDR: {error}")))?;
    let bind_private_addr =
        env::var("BIND_PRIVATE_ADDR").unwrap_or_else(|_| "[::]:8081".to_string());
    let private_addr: SocketAddr = bind_private_addr
        .parse()
        .map_err(|error| PlatformError::internal(format!("invalid BIND_PRIVATE_ADDR: {error}")))?;
    let public_base_domain =
        env::var("PUBLIC_BASE_DOMAIN").unwrap_or_else(|_| "example.com".to_string());
    let public_tls_cert_path = env::var("PUBLIC_TLS_CERT_PATH").ok().map(PathBuf::from);
    let public_tls_key_path = env::var("PUBLIC_TLS_KEY_PATH").ok().map(PathBuf::from);

    let result = dd_server::run(ServerConfig {
        bind_public_addr: public_addr,
        bind_private_addr: private_addr,
        public_base_domain,
        public_tls_cert_path,
        public_tls_key_path,
        ..ServerConfig::default()
    })
    .await;

    if let Some(provider) = otel_provider {
        let _ = provider.shutdown();
    }
    result
}

fn init_tracing() -> Result<Option<OTelTracerProvider>> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    let fmt_layer = tracing_subscriber::fmt::layer();
    let endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .or_else(|| env::var("DD_OTEL_ENDPOINT").ok())
        .filter(|value| !value.trim().is_empty());
    let resource = Resource::new(vec![
        KeyValue::new("service.name", "dd-server"),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);
    let mut provider_builder = OTelTracerProvider::builder().with_resource(resource);

    if let Some(endpoint) = endpoint {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|error| {
                PlatformError::internal(format!("otlp exporter init failed: {error}"))
            })?;
        provider_builder =
            provider_builder.with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio);
    }

    let provider = provider_builder.build();
    let tracer = provider.tracer("dd");
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();
    global::set_tracer_provider(provider.clone());
    Ok(Some(provider))
}
