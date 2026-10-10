use super::*;
use dd_v8::{OpError, ToJsBuffer};
use reqwest::dns::{Name, Resolve, Resolving};
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;

#[derive(Debug, Serialize)]
pub(crate) struct HttpPrepareResult {
    ok: bool,
    method: String,
    url: String,
    headers_handle: u32,
    body_handle: u32,
    client_rid: u32,
    error: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct HttpUrlCheckResult {
    ok: bool,
    url: String,
    client_rid: u32,
    error: String,
}

type PreparedHttpFetchRequest = (
    reqwest::Method,
    reqwest::Url,
    Vec<(String, String)>,
    Vec<u8>,
    Arc<AtomicBool>,
    Arc<Notify>,
    u32,
);

type EgressDnsFuture = Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'static>>;

trait EgressDnsResolver: Send + Sync {
    fn resolve(&self, host: &str, port: u16) -> EgressDnsFuture;
}

#[derive(Debug)]
struct SystemEgressDnsResolver;

impl EgressDnsResolver for SystemEgressDnsResolver {
    fn resolve(&self, host: &str, port: u16) -> EgressDnsFuture {
        let host = host.to_string();
        Box::pin(async move {
            tokio::net::lookup_host((host.as_str(), port))
                .await
                .map(|addresses| addresses.collect())
        })
    }
}

#[derive(Debug)]
struct PinnedDnsResolver {
    host: String,
    addresses: Vec<SocketAddr>,
}

impl Resolve for PinnedDnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let expected = self.host.clone();
        let addresses = self.addresses.clone();
        Box::pin(async move {
            if !name.as_str().eq_ignore_ascii_case(&expected) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "pinned DNS client cannot resolve a different host",
                )
                .into());
            }
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

#[derive(Debug)]
struct ResolvedEgressTarget {
    host: String,
    addresses: Vec<SocketAddr>,
}

async fn prepare_http_fetch_request(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
    method: &str,
    url: &str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> std::result::Result<PreparedHttpFetchRequest, String> {
    let (execution, canceled, canceled_notify) = http_fetch_context(state, request_context_handle)?;
    if canceled.load(Ordering::SeqCst) {
        canceled_notify.notify_waiters();
        return Err("host fetch request canceled".to_string());
    }

    let method = reqwest::Method::from_bytes(method.trim().to_ascii_uppercase().as_bytes())
        .map_err(|error| format!("invalid host fetch method: {error}"))?;

    let parsed_url =
        reqwest::Url::parse(url).map_err(|error| format!("invalid host fetch URL: {error}"))?;
    if !is_egress_url_allowed(&parsed_url, execution.egress_allow_hosts.as_ref()) {
        return Err(format!(
            "egress origin is not allowed: {}",
            parsed_url.origin().ascii_serialization()
        ));
    }
    let resolved = resolve_egress_target(
        &parsed_url,
        execution.egress_allow_hosts.as_ref(),
        &SystemEgressDnsResolver,
    )
    .await?;

    let headers = headers
        .into_iter()
        .filter_map(|(name, value)| {
            let trimmed = name.trim().to_string();
            if trimmed.eq_ignore_ascii_case("host")
                || trimmed.eq_ignore_ascii_case("content-length")
            {
                return None;
            }
            Some((trimmed, value))
        })
        .collect::<Vec<_>>();
    let client_rid = install_pinned_http_client(state, resolved)?;

    Ok((
        method,
        parsed_url,
        headers,
        body,
        canceled,
        canceled_notify,
        client_rid,
    ))
}

async fn check_http_fetch_url(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
    url: &str,
) -> std::result::Result<(String, u32), String> {
    let (execution, canceled, canceled_notify) = http_fetch_context(state, request_context_handle)?;
    if canceled.load(Ordering::SeqCst) {
        canceled_notify.notify_waiters();
        return Err("host fetch request canceled".to_string());
    }
    let parsed_url =
        reqwest::Url::parse(url).map_err(|error| format!("invalid host fetch URL: {error}"))?;
    let resolved = resolve_egress_target(
        &parsed_url,
        execution.egress_allow_hosts.as_ref(),
        &SystemEgressDnsResolver,
    )
    .await?;
    let client_rid = install_pinned_http_client(state, resolved)?;
    Ok((parsed_url.to_string(), client_rid))
}

async fn resolve_egress_target(
    url: &reqwest::Url,
    allow_hosts: &[EgressAllowHost],
    resolver: &dyn EgressDnsResolver,
) -> std::result::Result<ResolvedEgressTarget, String> {
    let host = normalized_egress_host(url).ok_or_else(|| {
        format!(
            "egress origin is not allowed: {}",
            url.origin().ascii_serialization()
        )
    })?;
    let request_port = url.port_or_known_default().ok_or_else(|| {
        format!(
            "egress origin is not allowed: {}",
            url.origin().ascii_serialization()
        )
    })?;
    let default_port = default_port_for_scheme(url.scheme()).ok_or_else(|| {
        format!(
            "egress origin is not allowed: {}",
            url.origin().ascii_serialization()
        )
    })?;
    let mut matched = allow_hosts
        .iter()
        .filter(|allowed| allowed.matches(&host, request_port, default_port))
        .peekable();
    if matched.peek().is_none() {
        return Err(format!(
            "egress origin is not allowed: {}",
            url.origin().ascii_serialization()
        ));
    }
    let allow_private = matched.any(|allowed| allowed.allow_private);

    let mut addresses = if let Ok(address) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(address, request_port)]
    } else {
        resolver
            .resolve(&host, request_port)
            .await
            .map_err(|error| format!("failed to resolve egress host {host}: {error}"))?
    };
    if addresses.is_empty() {
        return Err(format!("egress host {host} resolved to no addresses"));
    }

    let mut seen = HashSet::new();
    addresses.retain_mut(|address| {
        address.set_port(request_port);
        seen.insert(*address)
    });
    for address in &addresses {
        if !allow_private && !is_public_egress_address(normalize_egress_ip(address.ip())) {
            return Err(format!(
                "egress host {host} resolved to non-public address {}",
                address.ip()
            ));
        }
    }

    Ok(ResolvedEgressTarget { host, addresses })
}

/// Builds a client that can only reach the validated addresses of the
/// validated host. Every fetch gets its own client and connection pool, so
/// no connection made under one request's egress rules serves another.
fn install_pinned_http_client(
    state: &Rc<RefCell<OpState>>,
    target: ResolvedEgressTarget,
) -> std::result::Result<u32, String> {
    let client = reqwest::Client::builder()
        .dns_resolver(Arc::new(PinnedDnsResolver {
            host: target.host,
            addresses: target.addresses,
        }))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|error| format!("failed to create pinned host fetch client: {error}"))?;
    let mut state = state.borrow_mut();
    if !state.has::<HttpClients>() {
        state.put(HttpClients::default());
    }
    Ok(state.borrow_mut::<HttpClients>().insert(client))
}

/// Pinned clients waiting for the fetch they were made for.
#[derive(Default)]
pub(crate) struct HttpClients {
    next: u32,
    clients: HashMap<u32, reqwest::Client>,
}

impl HttpClients {
    fn insert(&mut self, client: reqwest::Client) -> u32 {
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next != 0 && !self.clients.contains_key(&self.next) {
                self.clients.insert(self.next, client);
                return self.next;
            }
        }
    }
}

/// Response bodies of host fetches, read chunk by chunk from JavaScript.
#[derive(Default)]
pub(crate) struct HttpResponseBodies {
    next: u32,
    bodies: HashMap<u32, reqwest::Response>,
}

impl HttpResponseBodies {
    fn insert(&mut self, response: reqwest::Response) -> u32 {
        loop {
            self.next = self.next.wrapping_add(1);
            if self.next != 0 && !self.bodies.contains_key(&self.next) {
                self.bodies.insert(self.next, response);
                return self.next;
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct HttpFetchResult {
    ok: bool,
    status: u16,
    status_text: String,
    headers: Vec<(String, String)>,
    body_handle: u32,
    error: String,
}

impl HttpFetchResult {
    fn failed(error: String) -> Self {
        Self {
            ok: false,
            status: 0,
            status_text: String::new(),
            headers: Vec::new(),
            body_handle: 0,
            error,
        }
    }
}

/// Sends a host fetch through the pinned client from `op_http_prepare` or
/// `op_http_check_url`. Redirects are left to JavaScript, which revalidates
/// each hop.
pub(crate) async fn op_http_fetch(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    client_handle: u32,
    method: String,
    url: String,
    headers_handle: u32,
    body_handle: u32,
) -> HttpFetchResult {
    match send_http_fetch(
        &state,
        request_context_handle,
        client_handle,
        &method,
        &url,
        headers_handle,
        body_handle,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => HttpFetchResult::failed(error),
    }
}

async fn send_http_fetch(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
    client_handle: u32,
    method: &str,
    url: &str,
    headers_handle: u32,
    body_handle: u32,
) -> std::result::Result<HttpFetchResult, String> {
    let (client, headers, body) = {
        let mut op_state = state.borrow_mut();
        let client = op_state
            .try_borrow_mut::<HttpClients>()
            .and_then(|clients| clients.clients.remove(&client_handle));
        let headers = op_state
            .borrow_mut::<HttpPreparedHeaders>()
            .take(headers_handle)
            .unwrap_or_default();
        let body = op_state
            .borrow_mut::<HttpPreparedBodies>()
            .take(body_handle)
            .unwrap_or_default();
        (client, headers, body)
    };
    let client = client.ok_or_else(|| "host fetch client is unavailable".to_string())?;
    let (_, canceled, canceled_notify) = http_fetch_context(state, request_context_handle)?;

    let method = reqwest::Method::from_bytes(method.as_bytes())
        .map_err(|error| format!("invalid host fetch method: {error}"))?;
    let mut request = client.request(method, url);
    for (name, value) in headers {
        let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|error| format!("invalid host fetch header name {name:?}: {error}"))?;
        let value = reqwest::header::HeaderValue::from_bytes(value.as_bytes())
            .map_err(|error| format!("invalid host fetch header value: {error}"))?;
        request = request.header(name, value);
    }
    if !body.is_empty() {
        request = request.body(body);
    }

    let canceled_wait = canceled_notify.notified();
    if canceled.load(Ordering::SeqCst) {
        return Err("host fetch request canceled".to_string());
    }
    let response = tokio::select! {
        response = request.send() => response.map_err(|error| format!("host fetch failed: {error}"))?,
        _ = canceled_wait => return Err("host fetch request canceled".to_string()),
    };

    let status = response.status();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    let body_handle = {
        let mut op_state = state.borrow_mut();
        if !op_state.has::<HttpResponseBodies>() {
            op_state.put(HttpResponseBodies::default());
        }
        op_state.borrow_mut::<HttpResponseBodies>().insert(response)
    };
    Ok(HttpFetchResult {
        ok: true,
        status: status.as_u16(),
        status_text: status.canonical_reason().unwrap_or_default().to_string(),
        headers,
        body_handle,
        error: String::new(),
    })
}

/// The next chunk of a host fetch response body, or `null` at its end.
pub(crate) async fn op_http_response_read(
    state: Rc<RefCell<OpState>>,
    body_handle: u32,
) -> std::result::Result<Option<ToJsBuffer>, OpError> {
    let response = state
        .borrow_mut()
        .try_borrow_mut::<HttpResponseBodies>()
        .and_then(|bodies| bodies.bodies.remove(&body_handle));
    let Some(mut response) = response else {
        return Ok(None);
    };
    let chunk = response.chunk().await.map_err(|error| {
        OpError::type_error(format!("error reading a host fetch body: {error}"))
    })?;
    if let Some(chunk) = &chunk {
        if let Some(bodies) = state.borrow_mut().try_borrow_mut::<HttpResponseBodies>() {
            bodies.bodies.insert(body_handle, response);
        }
        return Ok(Some(chunk.to_vec().into()));
    }
    Ok(None)
}

pub(crate) fn op_http_response_close(state: &mut OpState, body_handle: u32) {
    if let Some(bodies) = state.try_borrow_mut::<HttpResponseBodies>() {
        bodies.bodies.remove(&body_handle);
    }
}

fn normalized_egress_host(url: &reqwest::Url) -> Option<String> {
    let host = url.host_str()?.trim().to_ascii_lowercase();
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(&host);
    Some(
        host.parse::<IpAddr>()
            .map(|address| address.to_string())
            .unwrap_or_else(|_| host.to_string()),
    )
}

fn normalize_egress_ip(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(address) => address
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(address)),
        address => address,
    }
}

fn http_fetch_context(
    state: &Rc<RefCell<OpState>>,
    request_context_handle: u32,
) -> std::result::Result<(RequestExecutionContext, Arc<AtomicBool>, Arc<Notify>), String> {
    if request_context_handle == 0 {
        return Err("host fetch request context handle must not be empty".to_string());
    }
    let context = {
        let state_ref = state.borrow();
        state_ref
            .borrow::<RequestSecretContexts>()
            .get(request_context_handle)
            .map(|context| {
                (
                    context.execution.clone(),
                    context.canceled.clone(),
                    context.canceled_notify.clone(),
                )
            })
    };
    context.ok_or_else(|| "host fetch context is unavailable (request likely canceled)".to_string())
}

pub(crate) async fn op_http_prepare(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    method: String,
    url: String,
    headers_handle: u32,
    body_handle: u32,
) -> HttpPrepareResult {
    let (headers, body) = {
        let mut op_state = state.borrow_mut();
        let headers = op_state
            .borrow_mut::<HttpPreparedHeaders>()
            .take(headers_handle)
            .unwrap_or_default();
        let body = op_state
            .borrow_mut::<HttpPreparedBodies>()
            .take(body_handle)
            .unwrap_or_default();
        (headers, body)
    };
    match prepare_http_fetch_request(
        &state,
        request_context_handle,
        &method,
        &url,
        headers,
        body.to_vec(),
    )
    .await
    {
        Ok((method, url, headers, body, _, _, client_rid)) => {
            let (headers_handle, body_handle) = {
                let mut op_state = state.borrow_mut();
                let headers_handle = op_state.borrow_mut::<HttpPreparedHeaders>().insert(headers);
                let body_handle = op_state.borrow_mut::<HttpPreparedBodies>().insert(body);
                (headers_handle, body_handle)
            };
            HttpPrepareResult {
                ok: true,
                method: method.as_str().to_string(),
                url: url.to_string(),
                headers_handle,
                body_handle,
                client_rid,
                error: String::new(),
            }
        }
        Err(error) => HttpPrepareResult {
            ok: false,
            method: String::new(),
            url: String::new(),
            headers_handle: 0,
            body_handle: 0,
            client_rid: 0,
            error,
        },
    }
}

pub(crate) fn op_http_take_prepared_body(state: &mut OpState, body_handle: u32) -> ToJsBuffer {
    state
        .borrow_mut::<HttpPreparedBodies>()
        .take(body_handle)
        .map(|body| body.to_vec())
        .unwrap_or_default()
        .into()
}

pub(crate) fn op_http_store_prepared_body(state: &mut OpState, body: JsBuffer) -> u32 {
    state
        .borrow_mut::<HttpPreparedBodies>()
        .insert(Bytes::copy_from_slice(body.as_ref()))
}

pub(crate) fn op_http_store_prepared_headers(
    state: &mut OpState,
    headers: Vec<(String, String)>,
) -> u32 {
    state.borrow_mut::<HttpPreparedHeaders>().insert(headers)
}

pub(crate) fn op_http_take_prepared_headers(
    state: &mut OpState,
    headers_handle: u32,
) -> Vec<(String, String)> {
    state
        .borrow_mut::<HttpPreparedHeaders>()
        .take(headers_handle)
        .unwrap_or_default()
}

pub(crate) async fn op_http_check_url(
    state: Rc<RefCell<OpState>>,
    request_context_handle: u32,
    url: String,
) -> HttpUrlCheckResult {
    match check_http_fetch_url(&state, request_context_handle, &url).await {
        Ok((url, client_rid)) => HttpUrlCheckResult {
            ok: true,
            url,
            client_rid,
            error: String::new(),
        },
        Err(error) => HttpUrlCheckResult {
            ok: false,
            url: String::new(),
            client_rid: 0,
            error,
        },
    }
}

pub(crate) fn is_egress_url_allowed(url: &reqwest::Url, allow_hosts: &[EgressAllowHost]) -> bool {
    if allow_hosts.is_empty() {
        return false;
    }
    let Some(host) = normalized_egress_host(url) else {
        return false;
    };
    let literal_address = host.parse::<std::net::IpAddr>().ok();
    let Some(request_port) = url.port_or_known_default() else {
        return false;
    };
    let Some(default_port) = default_port_for_scheme(url.scheme()) else {
        return false;
    };
    allow_hosts.iter().any(|allowed| {
        allowed.matches(&host, request_port, default_port)
            && literal_address.is_none_or(|address| {
                is_public_egress_address(normalize_egress_ip(address)) || allowed.allow_private
            })
    })
}

fn is_public_egress_address(address: std::net::IpAddr) -> bool {
    match address {
        std::net::IpAddr::V4(address) => {
            let octets = address.octets();
            !(address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_broadcast()
                || address.is_documentation()
                || address.is_unspecified()
                || address.is_multicast()
                || octets[0] == 0
                || octets[0] >= 224
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 198 && (18..=19).contains(&octets[1]))
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
                || (octets[0] == 192 && octets[1] == 88 && octets[2] == 99))
        }
        std::net::IpAddr::V6(address) => {
            let segments = address.segments();
            // Restrict untrusted destinations to conventional global-unicast space. This
            // deliberately excludes transition/local-use prefixes whose embedded IPv4 address
            // could otherwise provide another route to loopback or private networks.
            (segments[0] & 0xe000) == 0x2000
                && !(segments[0] == 0x2001 && segments[1] <= 0x01ff)
                && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
                && segments[0] != 0x2002
                && !(segments[0] == 0x3fff && (segments[1] & 0xf000) == 0)
        }
    }
}

fn default_port_for_scheme(scheme: &str) -> Option<u16> {
    match scheme {
        "http" => Some(80),
        "https" => Some(443),
        _ => None,
    }
}

#[cfg(test)]
mod egress_dns_tests {
    use super::*;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct FixedResolver {
        addresses: Vec<SocketAddr>,
    }

    impl EgressDnsResolver for FixedResolver {
        fn resolve(&self, _host: &str, _port: u16) -> EgressDnsFuture {
            let addresses = self.addresses.clone();
            Box::pin(async move { Ok(addresses) })
        }
    }

    #[derive(Debug)]
    struct RebindingResolver {
        calls: Arc<AtomicUsize>,
    }

    impl EgressDnsResolver for RebindingResolver {
        fn resolve(&self, _host: &str, port: u16) -> EgressDnsFuture {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                let address = if call == 0 {
                    IpAddr::from([93, 184, 216, 34])
                } else {
                    IpAddr::from([127, 0, 0, 1])
                };
                Ok(vec![SocketAddr::new(address, port)])
            })
        }
    }

    fn allow(rule: &str) -> Vec<EgressAllowHost> {
        vec![parse_egress_allow_host(rule).expect("allow rule should parse")]
    }

    #[tokio::test]
    async fn public_dns_answers_are_accepted_and_pinned_to_the_request_port() {
        let url = reqwest::Url::parse("https://api.example.com/resource").expect("url");
        let resolved = resolve_egress_target(
            &url,
            &allow("api.example.com"),
            &FixedResolver {
                addresses: vec!["93.184.216.34:0".parse().expect("address")],
            },
        )
        .await
        .expect("public answer should pass");

        assert_eq!(resolved.host, "api.example.com");
        assert_eq!(
            resolved.addresses,
            vec!["93.184.216.34:443".parse().unwrap()]
        );
    }

    #[tokio::test]
    async fn public_to_loopback_rebinding_cannot_replace_the_pinned_answer() {
        let calls = Arc::new(AtomicUsize::new(0));
        let url = reqwest::Url::parse("https://api.example.com/resource").expect("url");
        let resolved = resolve_egress_target(
            &url,
            &allow("api.example.com"),
            &RebindingResolver {
                calls: calls.clone(),
            },
        )
        .await
        .expect("first public answer should pass");
        let pinned = PinnedDnsResolver {
            host: resolved.host,
            addresses: resolved.addresses,
        };

        let connection_addresses = pinned
            .resolve(Name::from_str("api.example.com").expect("name"))
            .await
            .expect("pinned host should resolve")
            .collect::<Vec<_>>();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            connection_addresses,
            vec!["93.184.216.34:443".parse().unwrap()]
        );
    }

    #[tokio::test]
    async fn redirect_hop_revalidates_and_rejects_a_rebound_hostname() {
        let calls = Arc::new(AtomicUsize::new(0));
        let resolver = RebindingResolver {
            calls: calls.clone(),
        };
        let url = reqwest::Url::parse("https://api.example.com/resource").expect("url");

        resolve_egress_target(&url, &allow("api.example.com"), &resolver)
            .await
            .expect("initial public answer should pass");
        let error = resolve_egress_target(&url, &allow("api.example.com"), &resolver)
            .await
            .expect_err("redirect hop must reject a rebound loopback answer");

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(error.contains("non-public address 127.0.0.1"));
    }

    #[tokio::test]
    async fn wildcard_rules_still_validate_and_pin_the_requested_hostname() {
        let url = reqwest::Url::parse("https://edge.api.example.com/resource").expect("url");
        let resolved = resolve_egress_target(
            &url,
            &allow("*.example.com"),
            &FixedResolver {
                addresses: vec!["93.184.216.34:443".parse().expect("address")],
            },
        )
        .await
        .expect("wildcard child should pass");

        assert_eq!(resolved.host, "edge.api.example.com");
        let pinned = PinnedDnsResolver {
            host: resolved.host,
            addresses: resolved.addresses,
        };
        assert!(
            pinned
                .resolve(Name::from_str("unrelated.example.com").expect("name"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn mixed_public_and_private_dns_answers_are_rejected() {
        let url = reqwest::Url::parse("https://api.example.com/resource").expect("url");
        let error = resolve_egress_target(
            &url,
            &allow("api.example.com"),
            &FixedResolver {
                addresses: vec![
                    "93.184.216.34:443".parse().expect("public address"),
                    "127.0.0.1:443".parse().expect("private address"),
                ],
            },
        )
        .await
        .expect_err("mixed answer must be denied");

        assert!(error.contains("non-public address 127.0.0.1"));
    }

    #[tokio::test]
    async fn trusted_private_dns_rule_accepts_private_answers() {
        let url = reqwest::Url::parse("http://internal.example:8080/").expect("url");
        let resolved = resolve_egress_target(
            &url,
            &allow("private:internal.example:8080"),
            &FixedResolver {
                addresses: vec!["10.0.0.8:8080".parse().expect("private address")],
            },
        )
        .await
        .expect("trusted private answer should pass");

        assert_eq!(resolved.addresses, vec!["10.0.0.8:8080".parse().unwrap()]);
    }

    #[tokio::test]
    async fn ipv4_mapped_loopback_dns_answer_is_rejected() {
        let url = reqwest::Url::parse("https://api.example.com/").expect("url");
        let error = resolve_egress_target(
            &url,
            &allow("api.example.com"),
            &FixedResolver {
                addresses: vec!["[::ffff:127.0.0.1]:443".parse().expect("mapped address")],
            },
        )
        .await
        .expect_err("mapped loopback must be denied");

        assert!(error.contains("non-public address"));
    }

    #[tokio::test]
    async fn public_ipv6_dns_answer_is_accepted() {
        let url = reqwest::Url::parse("https://api.example.com/").expect("url");
        let resolved = resolve_egress_target(
            &url,
            &allow("api.example.com"),
            &FixedResolver {
                addresses: vec![
                    "[2606:4700:4700::1111]:443"
                        .parse()
                        .expect("public IPv6 address"),
                ],
            },
        )
        .await
        .expect("public IPv6 should pass");

        assert_eq!(resolved.addresses.len(), 1);
    }

    #[tokio::test]
    async fn non_public_and_transition_ipv6_answers_are_rejected() {
        let url = reqwest::Url::parse("https://api.example.com/").expect("url");
        for address in [
            "[fec0::1]:443",
            "[::7f00:1]:443",
            "[64:ff9b::7f00:1]:443",
            "[2002:7f00:1::]:443",
            "[3fff::1]:443",
        ] {
            let error = resolve_egress_target(
                &url,
                &allow("api.example.com"),
                &FixedResolver {
                    addresses: vec![address.parse().expect("IPv6 address")],
                },
            )
            .await
            .expect_err("non-public IPv6 answer must be denied");
            assert!(error.contains("non-public address"), "{address}: {error}");
        }
    }

    #[tokio::test]
    async fn pinned_resolver_refuses_a_second_hostname() {
        let resolver = PinnedDnsResolver {
            host: "api.example.com".to_string(),
            addresses: vec!["93.184.216.34:443".parse().expect("address")],
        };
        let expected = resolver
            .resolve(Name::from_str("api.example.com").expect("name"))
            .await
            .expect("expected hostname should resolve")
            .collect::<Vec<_>>();
        assert_eq!(expected, resolver.addresses);
        assert!(
            resolver
                .resolve(Name::from_str("rebound.example.com").expect("name"))
                .await
                .is_err()
        );
    }
}
