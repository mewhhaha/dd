use clap::Parser;
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

#[derive(Parser, Debug)]
#[command(name = "dd_server")]
#[command(about = "Single-node dd worker runtime server")]
#[command(
    after_help = "Config defaults come from env or built-in defaults.\n\nKey env vars:\n  BIND_PUBLIC_ADDR\n  BIND_PRIVATE_ADDR\n  PUBLIC_BASE_DOMAIN\n  DD_PRIVATE_TOKEN\n  PRIVATE_BEARER_TOKEN\n  DD_ALLOW_INSECURE_PRIVATE_LOOPBACK\n  ALLOW_INSECURE_PRIVATE_LOOPBACK\n  PUBLIC_TLS_CERT_PATH\n  PUBLIC_TLS_KEY_PATH\n  OTEL_EXPORTER_OTLP_ENDPOINT\n  DD_OTEL_ENDPOINT"
)]
struct Cli {
    #[arg(long, env = "BIND_PUBLIC_ADDR", default_value = "0.0.0.0:8080")]
    bind_public_addr: String,

    #[arg(long, env = "BIND_PRIVATE_ADDR", default_value = "[::]:8081")]
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
    let private_bearer_token = cli.private_token.or(cli.private_bearer_token);
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
