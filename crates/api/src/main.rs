use clap::Parser;
use common::{
    first_non_empty_trimmed, PlatformError, Result, DEFAULT_PRIVATE_BIND_ADDR,
    DEFAULT_PUBLIC_BIND_ADDR,
};
use dd_server::ServerConfig;
#[cfg(feature = "otel")]
use opentelemetry::trace::TracerProvider as _;
#[cfg(feature = "otel")]
use opentelemetry::{global, KeyValue};
#[cfg(feature = "otel")]
use opentelemetry_otlp::{Protocol, WithExportConfig};
#[cfg(feature = "otel")]
use opentelemetry_sdk::propagation::TraceContextPropagator;
#[cfg(feature = "otel")]
use opentelemetry_sdk::trace::TracerProvider as OTelTracerProvider;
#[cfg(feature = "otel")]
use opentelemetry_sdk::Resource;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "dd_server")]
#[command(about = "Single-node dd worker runtime server")]
#[command(
    after_help = "Config defaults come from env or built-in defaults.\n\nKey env vars:\n  BIND_PUBLIC_ADDR\n  BIND_PRIVATE_ADDR\n  PUBLIC_BASE_DOMAIN\n  DD_PRIVATE_TOKEN\n  PRIVATE_BEARER_TOKEN\n  DD_TOKEN_STORE_PATH\n  DD_ALLOW_INSECURE_PRIVATE_LOOPBACK\n  ALLOW_INSECURE_PRIVATE_LOOPBACK\n  PUBLIC_TLS_CERT_PATH\n  PUBLIC_TLS_KEY_PATH\n  OTEL_EXPORTER_OTLP_ENDPOINT\n  DD_OTEL_ENDPOINT"
)]
struct Cli {
    #[arg(long, env = "BIND_PUBLIC_ADDR", default_value = DEFAULT_PUBLIC_BIND_ADDR)]
    bind_public_addr: String,

    #[arg(long, env = "BIND_PRIVATE_ADDR", default_value = DEFAULT_PRIVATE_BIND_ADDR)]
    bind_private_addr: String,

    #[arg(long, env = "PUBLIC_BASE_DOMAIN", default_value = "example.com")]
    public_base_domain: String,

    #[arg(long, env = "DD_PRIVATE_TOKEN")]
    private_token: Option<String>,

    #[arg(long, env = "PRIVATE_BEARER_TOKEN")]
    private_bearer_token: Option<String>,

    #[arg(
        long,
        env = "DD_ALLOW_INSECURE_PRIVATE_LOOPBACK",
        default_value_t = false
    )]
    dd_allow_insecure_private_loopback: bool,

    #[arg(long, env = "ALLOW_INSECURE_PRIVATE_LOOPBACK", default_value_t = false)]
    allow_insecure_private_loopback: bool,

    #[arg(long, env = "PUBLIC_TLS_CERT_PATH")]
    public_tls_cert_path: Option<PathBuf>,

    #[arg(long, env = "PUBLIC_TLS_KEY_PATH")]
    public_tls_key_path: Option<PathBuf>,

    #[arg(long, env = "DD_TOKEN_STORE_PATH")]
    token_store_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let otel_provider = init_tracing()?;

    let public_addr: SocketAddr = cli
        .bind_public_addr
        .parse()
        .map_err(|error| PlatformError::internal(format!("invalid BIND_PUBLIC_ADDR: {error}")))?;
    let private_addr: SocketAddr = cli
        .bind_private_addr
        .parse()
        .map_err(|error| PlatformError::internal(format!("invalid BIND_PRIVATE_ADDR: {error}")))?;
    let private_bearer_token = first_non_empty_trimmed([
        cli.private_token.unwrap_or_default(),
        cli.private_bearer_token.unwrap_or_default(),
    ]);
    let allow_insecure_private_loopback =
        cli.dd_allow_insecure_private_loopback || cli.allow_insecure_private_loopback;

    let result = dd_server::run(ServerConfig {
        bind_public_addr: public_addr,
        bind_private_addr: private_addr,
        public_base_domain: cli.public_base_domain,
        private_bearer_token,
        allow_insecure_private_loopback,
        public_tls_cert_path: cli.public_tls_cert_path,
        public_tls_key_path: cli.public_tls_key_path,
        token_store_path: cli
            .token_store_path
            .or_else(|| env::var_os("DD_DEPLOY_TOKEN_STORE_PATH").map(PathBuf::from)),
        ..ServerConfig::default()
    })
    .await;

    shutdown_tracing(otel_provider);
    result
}

#[cfg(feature = "otel")]
fn init_tracing() -> Result<Option<OTelTracerProvider>> {
    global::set_text_map_propagator(TraceContextPropagator::new());

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    let fmt_layer = tracing_subscriber::fmt::layer();
    let endpoint = configured_otlp_http_traces_endpoint();
    let resource = Resource::new(vec![
        KeyValue::new("service.name", "dd-server"),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ]);
    let mut provider_builder = OTelTracerProvider::builder().with_resource(resource);

    if let Some(endpoint) = endpoint {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_protocol(Protocol::HttpBinary)
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

#[cfg(not(feature = "otel"))]
fn init_tracing() -> Result<Option<()>> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    let fmt_layer = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
    Ok(None)
}

#[cfg(feature = "otel")]
fn shutdown_tracing(provider: Option<OTelTracerProvider>) {
    if let Some(provider) = provider {
        let _ = provider.shutdown();
    }
}

#[cfg(not(feature = "otel"))]
fn shutdown_tracing(_provider: Option<()>) {}

#[cfg(feature = "otel")]
fn configured_otlp_http_traces_endpoint() -> Option<String> {
    env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_string())
        .or_else(|| {
            env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(|value| otlp_http_traces_endpoint(&value))
        })
        .or_else(|| {
            env::var("DD_OTEL_ENDPOINT")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(|value| otlp_http_traces_endpoint(&value))
        })
}

#[cfg(feature = "otel")]
fn otlp_http_traces_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if trimmed.ends_with("/v1/traces") {
        return trimmed.to_string();
    }

    format!("{trimmed}/v1/traces")
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "otel")]
    use super::otlp_http_traces_endpoint;
    use common::first_non_empty_trimmed;

    #[test]
    fn token_selection_uses_first_non_empty_token() {
        let token = first_non_empty_trimmed(["  primary  ", "fallback"]);
        assert_eq!(token.as_deref(), Some("primary"));
    }

    #[test]
    fn token_selection_skips_empty_preferred_token() {
        let token = first_non_empty_trimmed(["  ", "fallback"]);
        assert_eq!(token.as_deref(), Some("fallback"));
    }

    #[test]
    fn token_selection_returns_none_when_all_tokens_are_empty() {
        let token = first_non_empty_trimmed(["", " "]);
        assert!(token.is_none());
    }

    #[cfg(feature = "otel")]
    #[test]
    fn otlp_http_endpoint_appends_trace_path_to_base_endpoint() {
        assert_eq!(
            otlp_http_traces_endpoint("http://collector:4318"),
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            otlp_http_traces_endpoint("http://collector:4318/"),
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            otlp_http_traces_endpoint("http://collector:4318/otlp"),
            "http://collector:4318/otlp/v1/traces"
        );
    }

    #[cfg(feature = "otel")]
    #[test]
    fn otlp_http_endpoint_keeps_explicit_trace_path() {
        assert_eq!(
            otlp_http_traces_endpoint("http://collector:4318/v1/traces"),
            "http://collector:4318/v1/traces"
        );
    }
}
