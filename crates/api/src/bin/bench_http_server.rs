use common::{DeployCacheConfig, DeployConfig, DeployRequest};
use dd_server::ServerConfig;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use runtime::{RuntimeConfig, RuntimeServiceConfig, RuntimeStorageConfig};
use std::collections::HashMap;
use std::io::Read;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;

const CHILD_ARG: &str = "--server-child";
const PRIVATE_TOKEN: &str = "http-bench-private-token";
const PUBLIC_BASE_DOMAIN: &str = "example.com";
const TRACE_ID: &str = "0123456789abcdef0123456789abcdef";
const TRACEPARENT: &str = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
const IO_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
struct BenchSettings {
    requests: usize,
    concurrency: usize,
}

struct BenchResult {
    requests: usize,
    concurrency: usize,
    total: Duration,
    latencies: Vec<Duration>,
}

impl BenchResult {
    fn print(&mut self, name: &str) {
        self.latencies.sort_unstable();
        let total_seconds = self.total.as_secs_f64();
        let throughput = if total_seconds > 0.0 {
            self.requests as f64 / total_seconds
        } else {
            0.0
        };
        let mean = if self.latencies.is_empty() {
            0.0
        } else {
            self.latencies
                .iter()
                .map(Duration::as_secs_f64)
                .sum::<f64>()
                * 1000.0
                / self.latencies.len() as f64
        };
        println!(
            "{name:<34} requests={} concurrency={} total={:.2}ms throughput={:.2} req/s mean={:.3}ms p50={:.3}ms p95={:.3}ms p99={:.3}ms",
            self.requests,
            self.concurrency,
            self.total.as_secs_f64() * 1000.0,
            throughput,
            mean,
            percentile_ms(&self.latencies, 0.50),
            percentile_ms(&self.latencies, 0.95),
            percentile_ms(&self.latencies, 0.99),
        );
    }
}

#[derive(Clone, Copy)]
enum ExpectedResponse {
    Uncached,
    FrontCacheHit,
}

impl ExpectedResponse {
    fn host(self) -> &'static str {
        match self {
            Self::Uncached => "http-uncached.example.com",
            Self::FrontCacheHit => "http-cached.example.com",
        }
    }

    fn validate(self, response: &HttpResponse) -> Result<(), String> {
        if response.status != 200 {
            return Err(format!("expected status 200, got {}", response.status));
        }
        let expected_body = match self {
            Self::Uncached => b"uncached-ok".as_slice(),
            Self::FrontCacheHit => b"cached-ok".as_slice(),
        };
        if response.body != expected_body {
            return Err(format!(
                "unexpected response body: expected {:?}, got {:?}",
                String::from_utf8_lossy(expected_body),
                String::from_utf8_lossy(&response.body)
            ));
        }
        match (self, response.header("x-dd-cache")) {
            (Self::Uncached, None) => {}
            (Self::Uncached, Some(value)) => {
                return Err(format!(
                    "uncached response unexpectedly had x-dd-cache={value}"
                ));
            }
            (Self::FrontCacheHit, Some("HIT")) => {}
            (Self::FrontCacheHit, value) => {
                return Err(format!(
                    "front-cache response expected x-dd-cache=HIT, got {value:?}"
                ));
            }
        }
        if matches!(self, Self::FrontCacheHit) {
            if response.header("cache-control") != Some("public, max-age=3600") {
                return Err(format!(
                    "front-cache response lost cache-control, got {:?}",
                    response.header("cache-control")
                ));
            }
            if response.header("x-origin-call") != Some("1") {
                return Err(format!(
                    "front-cache response did not preserve the warmed origin response, got x-origin-call={:?}",
                    response.header("x-origin-call")
                ));
            }
        }
        match response.header("x-dd-trace-id") {
            Some(value) if value == TRACE_ID => Ok(()),
            value => Err(format!(
                "expected propagated x-dd-trace-id={TRACE_ID}, got {value:?}"
            )),
        }
    }
}

struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl HttpResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}

struct Http1Connection {
    stream: TcpStream,
    read_buffer: Vec<u8>,
}

impl Http1Connection {
    async fn connect(address: SocketAddr) -> Result<Self, String> {
        let stream = timeout(IO_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|_| format!("timed out connecting to {address}"))?
            .map_err(|error| format!("failed connecting to {address}: {error}"))?;
        stream
            .set_nodelay(true)
            .map_err(|error| format!("failed enabling client TCP_NODELAY: {error}"))?;
        Ok(Self {
            stream,
            read_buffer: Vec::with_capacity(4096),
        })
    }

    async fn request(
        &mut self,
        method: &str,
        path: &str,
        host: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<HttpResponse, String> {
        let mut request =
            format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: keep-alive\r\n");
        for (name, value) in headers {
            request.push_str(name);
            request.push_str(": ");
            request.push_str(value);
            request.push_str("\r\n");
        }
        if !body.is_empty() {
            request.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        request.push_str("\r\n");

        timeout(IO_TIMEOUT, self.stream.write_all(request.as_bytes()))
            .await
            .map_err(|_| "timed out writing HTTP/1 request headers".to_string())?
            .map_err(|error| format!("failed writing HTTP/1 request headers: {error}"))?;
        if !body.is_empty() {
            timeout(IO_TIMEOUT, self.stream.write_all(body))
                .await
                .map_err(|_| "timed out writing HTTP/1 request body".to_string())?
                .map_err(|error| format!("failed writing HTTP/1 request body: {error}"))?;
        }
        self.read_response().await
    }

    async fn read_response(&mut self) -> Result<HttpResponse, String> {
        let header_end = loop {
            if let Some(position) = find_bytes(&self.read_buffer, b"\r\n\r\n") {
                break position + 4;
            }
            self.read_more().await?;
        };
        let header_bytes = &self.read_buffer[..header_end - 4];
        let header_text = std::str::from_utf8(header_bytes)
            .map_err(|error| format!("HTTP/1 response headers were not UTF-8: {error}"))?;
        let mut lines = header_text.split("\r\n");
        let status_line = lines
            .next()
            .ok_or_else(|| "HTTP/1 response had no status line".to_string())?;
        let mut status_parts = status_line.split_whitespace();
        let version = status_parts.next().unwrap_or_default();
        if !version.starts_with("HTTP/1.") {
            return Err(format!("expected HTTP/1 response, got {status_line:?}"));
        }
        let status = status_parts
            .next()
            .ok_or_else(|| format!("missing status code in {status_line:?}"))?
            .parse::<u16>()
            .map_err(|error| format!("invalid status line {status_line:?}: {error}"))?;
        let mut headers = HashMap::new();
        for line in lines {
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| format!("malformed HTTP/1 response header {line:?}"))?;
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }

        let (body, consumed) = if let Some(length) = headers.get("content-length") {
            let length = length
                .parse::<usize>()
                .map_err(|error| format!("invalid content-length {length:?}: {error}"))?;
            self.ensure_buffered(header_end + length).await?;
            (
                self.read_buffer[header_end..header_end + length].to_vec(),
                header_end + length,
            )
        } else if headers
            .get("transfer-encoding")
            .is_some_and(|value| value.to_ascii_lowercase().contains("chunked"))
        {
            self.read_chunked_body(header_end).await?
        } else if status == 204 || status == 304 {
            (Vec::new(), header_end)
        } else {
            return Err("HTTP/1 response had neither content-length nor chunked encoding".into());
        };
        self.read_buffer.drain(..consumed);
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }

    async fn read_chunked_body(&mut self, mut cursor: usize) -> Result<(Vec<u8>, usize), String> {
        let mut body = Vec::new();
        loop {
            let line_end = self.read_line_end(cursor).await?;
            let size_text = std::str::from_utf8(&self.read_buffer[cursor..line_end])
                .map_err(|error| format!("chunk size was not UTF-8: {error}"))?;
            let size_text = size_text.split(';').next().unwrap_or_default().trim();
            let size = usize::from_str_radix(size_text, 16)
                .map_err(|error| format!("invalid HTTP/1 chunk size {size_text:?}: {error}"))?;
            cursor = line_end + 2;
            if size == 0 {
                loop {
                    let trailer_end = self.read_line_end(cursor).await?;
                    let empty = trailer_end == cursor;
                    cursor = trailer_end + 2;
                    if empty {
                        return Ok((body, cursor));
                    }
                }
            }
            self.ensure_buffered(cursor + size + 2).await?;
            body.extend_from_slice(&self.read_buffer[cursor..cursor + size]);
            cursor += size;
            if self.read_buffer.get(cursor..cursor + 2) != Some(b"\r\n") {
                return Err("HTTP/1 chunk was not terminated by CRLF".into());
            }
            cursor += 2;
        }
    }

    async fn read_line_end(&mut self, cursor: usize) -> Result<usize, String> {
        loop {
            if let Some(position) = find_bytes(&self.read_buffer[cursor..], b"\r\n") {
                return Ok(cursor + position);
            }
            self.read_more().await?;
        }
    }

    async fn ensure_buffered(&mut self, length: usize) -> Result<(), String> {
        while self.read_buffer.len() < length {
            self.read_more().await?;
        }
        Ok(())
    }

    async fn read_more(&mut self) -> Result<(), String> {
        let read = timeout(IO_TIMEOUT, self.stream.read_buf(&mut self.read_buffer))
            .await
            .map_err(|_| "timed out reading HTTP/1 response".to_string())?
            .map_err(|error| format!("failed reading HTTP/1 response: {error}"))?;
        if read == 0 {
            return Err("server closed HTTP/1 connection before response completed".into());
        }
        Ok(())
    }
}

struct ServerProcess {
    child: Child,
    output: Arc<Mutex<Vec<u8>>>,
    readers: Vec<JoinHandle<()>>,
}

impl ServerProcess {
    fn spawn(
        public_addr: SocketAddr,
        private_addr: SocketAddr,
        store_dir: &Path,
    ) -> Result<Self, String> {
        let mut child = Command::new(
            std::env::current_exe()
                .map_err(|error| format!("failed resolving benchmark executable: {error}"))?,
        )
        .arg(CHILD_ARG)
        .env("DD_HTTP_BENCH_PUBLIC_ADDR", public_addr.to_string())
        .env("DD_HTTP_BENCH_PRIVATE_ADDR", private_addr.to_string())
        .env("DD_HTTP_BENCH_STORE_DIR", store_dir)
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed starting benchmark server child: {error}"))?;
        let output = Arc::new(Mutex::new(Vec::new()));
        let mut readers = Vec::new();
        if let Some(stdout) = child.stdout.take() {
            readers.push(capture_output(stdout, Arc::clone(&output)));
        }
        if let Some(stderr) = child.stderr.take() {
            readers.push(capture_output(stderr, Arc::clone(&output)));
        }
        Ok(Self {
            child,
            output,
            readers,
        })
    }

    fn ensure_running(&mut self) -> Result<(), String> {
        match self
            .child
            .try_wait()
            .map_err(|error| format!("failed checking benchmark server child: {error}"))?
        {
            None => Ok(()),
            Some(status) => Err(format!(
                "benchmark server child exited early with {status}:\n{}",
                output_tail(&self.output, 16 * 1024)
            )),
        }
    }

    fn stop(mut self) -> Result<String, String> {
        if self
            .child
            .try_wait()
            .map_err(|error| format!("failed checking benchmark server child: {error}"))?
            .is_none()
        {
            self.child
                .kill()
                .map_err(|error| format!("failed stopping benchmark server child: {error}"))?;
            self.child
                .wait()
                .map_err(|error| format!("failed waiting for benchmark server child: {error}"))?;
        }
        for reader in self.readers {
            reader
                .join()
                .map_err(|_| "benchmark output reader thread panicked".to_string())?;
        }
        let bytes = self
            .output
            .lock()
            .map_err(|_| "benchmark output buffer lock was poisoned".to_string())?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

fn capture_output<R>(mut reader: R, output: Arc<Mutex<Vec<u8>>>) -> JoinHandle<()>
where
    R: Read + Send + 'static,
{
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 8192];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(length) => {
                    if let Ok(mut destination) = output.lock() {
                        destination.extend_from_slice(&buffer[..length]);
                    } else {
                        return;
                    }
                }
            }
        }
    })
}

fn output_tail(output: &Arc<Mutex<Vec<u8>>>, max_bytes: usize) -> String {
    let Ok(output) = output.lock() else {
        return "<output buffer poisoned>".into();
    };
    let start = output.len().saturating_sub(max_bytes);
    String::from_utf8_lossy(&output[start..]).into_owned()
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.as_slice() == [CHILD_ARG] {
        return run_server_child().await;
    }
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--list") {
        println!("scenario http1-server/uncached-small");
        println!("scenario http1-server/front-cache-hit");
        return Ok(());
    }
    if !args.is_empty() {
        return Err(format!("unknown argument: {}", args.join(" ")));
    }
    run_parent().await
}

async fn run_parent() -> Result<(), String> {
    let settings = BenchSettings {
        requests: env_usize("DD_BENCH_REQUESTS", 4_000),
        concurrency: env_usize("DD_BENCH_CONCURRENCY", 128),
    };
    let public_addr = unused_loopback_address()?;
    let mut private_addr = unused_loopback_address()?;
    while private_addr == public_addr {
        private_addr = unused_loopback_address()?;
    }
    let store_dir = std::env::temp_dir().join(format!(
        "dd-http1-server-bench-{}-{}",
        std::process::id(),
        Uuid::new_v4()
    ));
    tokio::fs::create_dir_all(&store_dir)
        .await
        .map_err(|error| format!("failed creating temporary benchmark store: {error}"))?;

    let mut server = match ServerProcess::spawn(public_addr, private_addr, &store_dir) {
        Ok(server) => server,
        Err(error) => {
            let _ = tokio::fs::remove_dir_all(&store_dir).await;
            return Err(error);
        }
    };
    let benchmark_result =
        run_against_server(&mut server, public_addr, private_addr, settings).await;
    let server_output = server.stop();
    let cleanup_result = tokio::fs::remove_dir_all(&store_dir).await;

    if let Err(error) = benchmark_result {
        let output = server_output.unwrap_or_else(|stop_error| stop_error);
        return Err(format!(
            "{error}\nserver output:\n{}",
            tail(&output, 16 * 1024)
        ));
    }
    let server_output = server_output?;
    validate_server_output(&server_output)?;
    cleanup_result.map_err(|error| {
        format!(
            "failed removing temporary benchmark store {}: {error}",
            store_dir.display()
        )
    })?;
    Ok(())
}

async fn run_against_server(
    server: &mut ServerProcess,
    public_addr: SocketAddr,
    private_addr: SocketAddr,
    settings: BenchSettings,
) -> Result<(), String> {
    wait_until_ready(server, private_addr).await?;
    deploy_workers(private_addr).await?;
    warm_front_cache(public_addr).await?;

    let mut uncached = run_scenario(public_addr, settings, ExpectedResponse::Uncached).await?;
    uncached.print("http1-server/uncached-small");
    server.ensure_running()?;

    let mut cached = run_scenario(public_addr, settings, ExpectedResponse::FrontCacheHit).await?;
    cached.print("http1-server/front-cache-hit");
    server.ensure_running()?;
    Ok(())
}

async fn wait_until_ready(
    server: &mut ServerProcess,
    private_addr: SocketAddr,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        server.ensure_running()?;
        if let Ok(mut connection) = Http1Connection::connect(private_addr).await
            && let Ok(response) = connection
                .request("GET", "/healthz", "localhost", &[], &[])
                .await
            && response.status == 200
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("timed out waiting for benchmark server readiness".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn deploy_workers(private_addr: SocketAddr) -> Result<(), String> {
    let uncached = DeployRequest {
        name: "http-uncached".to_string(),
        source: r#"
export default {
  fetch() {
    return new Response("uncached-ok", {
      headers: { "content-type": "text/plain; charset=utf-8" },
    });
  },
};
"#
        .to_string(),
        config: DeployConfig {
            public: true,
            ..DeployConfig::default()
        },
        assets: Vec::new(),
        server_modules: Vec::new(),
        asset_headers: None,
        temporary: false,
    };
    let cached = DeployRequest {
        name: "http-cached".to_string(),
        source: r#"
let calls = 0;
export default {
  fetch() {
    calls += 1;
    return new Response("cached-ok", {
      headers: {
        "cache-control": "public, max-age=3600",
        "content-type": "text/plain; charset=utf-8",
        "x-origin-call": String(calls),
      },
    });
  },
};
"#
        .to_string(),
        config: DeployConfig {
            public: true,
            cache: DeployCacheConfig { enabled: true },
            ..DeployConfig::default()
        },
        assets: Vec::new(),
        server_modules: Vec::new(),
        asset_headers: None,
        temporary: false,
    };
    let authorization = format!("Bearer {PRIVATE_TOKEN}");
    for deployment in [uncached, cached] {
        let body = serde_json::to_vec(&deployment)
            .map_err(|error| format!("failed encoding deployment: {error}"))?;
        let mut connection = Http1Connection::connect(private_addr).await?;
        let response = connection
            .request(
                "POST",
                "/v1/deploy",
                "localhost",
                &[
                    ("authorization", authorization.as_str()),
                    ("content-type", "application/json"),
                ],
                &body,
            )
            .await?;
        if response.status != 200 {
            return Err(format!(
                "deployment failed with status {}: {}",
                response.status,
                String::from_utf8_lossy(&response.body)
            ));
        }
    }
    Ok(())
}

async fn warm_front_cache(public_addr: SocketAddr) -> Result<(), String> {
    let mut connection = Http1Connection::connect(public_addr).await?;
    let response = connection
        .request(
            "GET",
            "/",
            ExpectedResponse::FrontCacheHit.host(),
            &[("traceparent", TRACEPARENT)],
            &[],
        )
        .await?;
    if response.status != 200 || response.body != b"cached-ok" {
        return Err(format!(
            "front-cache warmup returned status {} body {:?}",
            response.status,
            String::from_utf8_lossy(&response.body)
        ));
    }
    if response.header("x-dd-cache") != Some("MISS") {
        return Err(format!(
            "front-cache warmup expected x-dd-cache=MISS, got {:?}",
            response.header("x-dd-cache")
        ));
    }
    if response.header("cache-control") != Some("public, max-age=3600")
        || response.header("x-origin-call") != Some("1")
    {
        return Err(format!(
            "front-cache warmup did not preserve origin cache headers: cache-control={:?} x-origin-call={:?}",
            response.header("cache-control"),
            response.header("x-origin-call")
        ));
    }
    if response.header("x-dd-trace-id") != Some(TRACE_ID) {
        return Err(format!(
            "front-cache warmup expected x-dd-trace-id={TRACE_ID}, got {:?}",
            response.header("x-dd-trace-id")
        ));
    }
    Ok(())
}

async fn run_scenario(
    address: SocketAddr,
    settings: BenchSettings,
    expected: ExpectedResponse,
) -> Result<BenchResult, String> {
    let concurrency = settings.concurrency.min(settings.requests).max(1);
    let mut connections = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        connections.push(Http1Connection::connect(address).await?);
    }
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);
    for mut connection in connections {
        let next = Arc::clone(&next);
        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= settings.requests {
                    return Ok::<_, String>(latencies);
                }
                let request_started = Instant::now();
                let response = connection
                    .request(
                        "GET",
                        "/",
                        expected.host(),
                        &[("traceparent", TRACEPARENT)],
                        &[],
                    )
                    .await?;
                expected.validate(&response)?;
                latencies.push(request_started.elapsed());
            }
        }));
    }
    let mut latencies = Vec::with_capacity(settings.requests);
    for task in tasks {
        latencies.extend(
            task.await
                .map_err(|error| format!("HTTP/1 benchmark task panicked: {error}"))??,
        );
    }
    let total = started.elapsed();
    if latencies.len() != settings.requests {
        return Err(format!(
            "expected {} timings, collected {}",
            settings.requests,
            latencies.len()
        ));
    }
    Ok(BenchResult {
        requests: settings.requests,
        concurrency,
        total,
        latencies,
    })
}

async fn run_server_child() -> Result<(), String> {
    let _provider = init_info_tracing()?;
    let public_addr = env_socket_addr("DD_HTTP_BENCH_PUBLIC_ADDR")?;
    let private_addr = env_socket_addr("DD_HTTP_BENCH_PRIVATE_ADDR")?;
    let store_dir = PathBuf::from(
        std::env::var_os("DD_HTTP_BENCH_STORE_DIR")
            .ok_or_else(|| "DD_HTTP_BENCH_STORE_DIR is required in server child".to_string())?,
    );
    let storage = RuntimeStorageConfig {
        store_dir: store_dir.clone(),
        database_url: format!("file:{}/dd-http-bench.db", store_dir.display()),
        worker_store_enabled: false,
        ..RuntimeStorageConfig::default()
    };
    let min_isolates = env_usize("DD_BENCH_MIN_ISOLATES", 8);
    let max_isolates = env_usize("DD_BENCH_MAX_ISOLATES", 8).max(min_isolates);
    let runtime = RuntimeConfig {
        min_isolates,
        // Both benchmark workers stay deployed so each scenario can use its
        // requested per-worker pool without starving the other worker.
        max_global_isolates: max_isolates.saturating_mul(2),
        max_isolates,
        max_inflight_per_isolate: env_usize("DD_BENCH_MAX_INFLIGHT", 16),
        ..RuntimeConfig::default()
    };
    dd_server::run(ServerConfig {
        bind_public_addr: public_addr,
        bind_private_addr: private_addr,
        public_base_domain: PUBLIC_BASE_DOMAIN.to_string(),
        private_bearer_token: Some(PRIVATE_TOKEN.to_string()),
        token_store_path: Some(store_dir.join("tokens.json")),
        runtime: RuntimeServiceConfig { runtime, storage },
        ..ServerConfig::default()
    })
    .await
    .map_err(|error| format!("benchmark server failed: {error}"))
}

fn init_info_tracing() -> Result<SdkTracerProvider, String> {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let provider = SdkTracerProvider::builder().build();
    let tracer = provider.tracer("dd-http-server-benchmark");
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()
        .map_err(|error| format!("failed initializing INFO tracing: {error}"))?;
    global::set_tracer_provider(provider.clone());
    Ok(provider)
}

fn validate_server_output(output: &str) -> Result<(), String> {
    let uppercase = output.to_ascii_uppercase();
    if uppercase.contains("WHOPPER_") || uppercase.contains("WHOPPER ") {
        return Err(format!(
            "benchmark server emitted forbidden WHOPPER probe output:\n{}",
            tail(output, 16 * 1024)
        ));
    }
    let lowercase = output.to_ascii_lowercase();
    if lowercase.contains("panicked at") || lowercase.contains("thread panicked") {
        return Err(format!(
            "benchmark server panicked under concurrent INFO tracing:\n{}",
            tail(output, 16 * 1024)
        ));
    }
    Ok(())
}

fn unused_loopback_address() -> Result<SocketAddr, String> {
    let listener = StdTcpListener::bind("127.0.0.1:0")
        .map_err(|error| format!("failed reserving loopback benchmark port: {error}"))?;
    listener
        .local_addr()
        .map_err(|error| format!("failed reading loopback benchmark address: {error}"))
}

fn env_socket_addr(name: &str) -> Result<SocketAddr, String> {
    std::env::var(name)
        .map_err(|_| format!("{name} is required in server child"))?
        .parse()
        .map_err(|error| format!("invalid {name}: {error}"))
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn percentile_ms(latencies: &[Duration], percentile: f64) -> f64 {
    if latencies.is_empty() {
        return 0.0;
    }
    let index = ((latencies.len() - 1) as f64 * percentile).round() as usize;
    latencies[index].as_secs_f64() * 1000.0
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn tail(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut start = value.len() - max_bytes;
    while !value.is_char_boundary(start) {
        start += 1;
    }
    &value[start..]
}

fn print_help() {
    println!("dd real HTTP/1 server benchmark");
    println!();
    println!("Usage:");
    println!("  cargo run -p dd_server --bin bench_http_server --release");
    println!();
    println!("Environment:");
    println!("  DD_BENCH_REQUESTS       requests per scenario (default 4000)");
    println!("  DD_BENCH_CONCURRENCY    persistent HTTP/1 connections (default 128)");
    println!("  DD_BENCH_MIN_ISOLATES   prewarmed isolates (default 8)");
    println!("  DD_BENCH_MAX_ISOLATES   isolate limit (default 8)");
    println!("  DD_BENCH_MAX_INFLIGHT   requests per isolate (default 16)");
}

#[cfg(test)]
mod tests {
    use super::{find_bytes, percentile_ms, validate_server_output};
    use std::time::Duration;

    #[test]
    fn byte_search_finds_http_delimiter() {
        assert_eq!(find_bytes(b"headers\r\n\r\nbody", b"\r\n\r\n"), Some(7));
        assert_eq!(find_bytes(b"headers", b"\r\n\r\n"), None);
    }

    #[test]
    fn percentile_uses_sorted_distribution_index() {
        let latencies = [
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_millis(3),
        ];
        assert_eq!(percentile_ms(&latencies, 0.50), 2.0);
        assert_eq!(percentile_ms(&latencies, 0.99), 3.0);
    }

    #[test]
    fn output_guard_rejects_panics_and_debug_probes() {
        assert!(validate_server_output("normal info log").is_ok());
        assert!(validate_server_output("thread panicked at boom").is_err());
        assert!(validate_server_output("WHOPPER_QUERY probe").is_err());
    }
}
