use base64::Engine;
use common::{DeployAsset, PlatformError, Result};
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub struct AssetBundle {
    assets: HashMap<String, StaticAsset>,
    header_rules: Vec<HeaderRule>,
}

#[derive(Clone, Debug)]
struct StaticAsset {
    body: Vec<u8>,
    etag: String,
    content_type: String,
}

#[derive(Clone, Debug)]
struct HeaderRule {
    host_matcher: Option<PatternMatcher>,
    path_matcher: PatternMatcher,
    actions: Vec<HeaderAction>,
}

#[derive(Clone, Debug)]
enum HeaderAction {
    Set { name: String, value: String },
    Remove { name: String },
}

#[derive(Clone, Debug)]
struct PatternMatcher {
    regex: Regex,
}

struct HeaderEntry {
    name: String,
    value: String,
    from_rule: bool,
}

pub struct AssetRequest<'a> {
    pub method: &'a str,
    pub host: Option<&'a str>,
    pub path: &'a str,
    pub headers: &'a [(String, String)],
}

pub struct AssetResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub fn compile_asset_bundle(
    assets: &[DeployAsset],
    asset_headers: Option<&str>,
) -> Result<AssetBundle> {
    let mut compiled_assets = HashMap::new();
    for asset in assets {
        let normalized_path = normalize_asset_path(&asset.path)?;
        if normalized_path == "/_headers" {
            return Err(PlatformError::bad_request(
                "asset path /_headers is reserved for static asset headers",
            ));
        }
        if compiled_assets.contains_key(&normalized_path) {
            return Err(PlatformError::bad_request(format!(
                "duplicate asset path: {normalized_path}"
            )));
        }

        let body = base64::engine::general_purpose::STANDARD
            .decode(asset.content_base64.as_bytes())
            .map_err(|error| {
                PlatformError::bad_request(format!(
                    "asset {} has invalid base64 content: {error}",
                    asset.path
                ))
            })?;
        let etag = format!("\"{}\"", hex_digest(Sha256::digest(&body).as_slice()));
        let content_type = mime_guess::from_path(&normalized_path)
            .first_or_octet_stream()
            .essence_str()
            .to_string();
        compiled_assets.insert(
            normalized_path,
            StaticAsset {
                body,
                etag,
                content_type,
            },
        );
    }

    let header_rules = match asset_headers {
        Some(raw) if !raw.trim().is_empty() => parse_header_rules(raw)?,
        _ => Vec::new(),
    };

    Ok(AssetBundle {
        assets: compiled_assets,
        header_rules,
    })
}

pub fn resolve_asset(bundle: &AssetBundle, request: AssetRequest<'_>) -> Option<AssetResponse> {
    if !request.method.eq_ignore_ascii_case("GET") && !request.method.eq_ignore_ascii_case("HEAD") {
        return None;
    }
    let asset = bundle.assets.get(request.path)?;
    let mut headers = vec![
        HeaderEntry {
            name: "content-type".to_string(),
            value: asset.content_type.clone(),
            from_rule: false,
        },
        HeaderEntry {
            name: "cache-control".to_string(),
            value: "public, max-age=0, must-revalidate".to_string(),
            from_rule: false,
        },
        HeaderEntry {
            name: "etag".to_string(),
            value: asset.etag.clone(),
            from_rule: false,
        },
        HeaderEntry {
            name: "content-length".to_string(),
            value: asset.body.len().to_string(),
            from_rule: false,
        },
    ];

    let normalized_host = request.host.map(normalize_request_host);
    for rule in &bundle.header_rules {
        let Some(captures) = rule.capture(normalized_host.as_deref(), request.path) else {
            continue;
        };
        for action in &rule.actions {
            match action {
                HeaderAction::Set { name, value } => {
                    let resolved = substitute_header_value(value, &captures);
                    apply_header_set(&mut headers, name, &resolved);
                }
                HeaderAction::Remove { name } => {
                    headers.retain(|header| !header.name.eq_ignore_ascii_case(name));
                }
            }
        }
    }

    let not_modified = request.headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("if-none-match")
            && value
                .split(',')
                .any(|candidate| candidate.trim() == asset.etag || candidate.trim() == "*")
    });
    if not_modified {
        if let Some(content_length) = headers
            .iter_mut()
            .find(|header| header.name.eq_ignore_ascii_case("content-length"))
        {
            content_length.value = "0".to_string();
            content_length.from_rule = false;
        }
        return Some(AssetResponse {
            status: 304,
            headers: headers.into_iter().map(into_pair).collect(),
            body: Vec::new(),
        });
    }

    let body = if request.method.eq_ignore_ascii_case("HEAD") {
        Vec::new()
    } else {
        asset.body.clone()
    };
    Some(AssetResponse {
        status: 200,
        headers: headers.into_iter().map(into_pair).collect(),
        body,
    })
}

fn into_pair(header: HeaderEntry) -> (String, String) {
    (header.name, header.value)
}

fn apply_header_set(headers: &mut Vec<HeaderEntry>, name: &str, value: &str) {
    if let Some(existing) = headers
        .iter_mut()
        .find(|header| header.name.eq_ignore_ascii_case(name))
    {
        if existing.from_rule {
            existing.value.push_str(", ");
            existing.value.push_str(value);
        } else {
            existing.value = value.to_string();
            existing.from_rule = true;
        }
        return;
    }

    headers.push(HeaderEntry {
        name: name.to_ascii_lowercase(),
        value: value.to_string(),
        from_rule: true,
    });
}

fn parse_header_rules(raw: &str) -> Result<Vec<HeaderRule>> {
    let mut rules = Vec::new();
    let mut current_pattern = None::<(usize, String, Vec<HeaderAction>)>;

    for (index, line) in raw.lines().enumerate() {
        let line_no = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let is_indented = line.starts_with(' ') || line.starts_with('\t');
        if !is_indented {
            if let Some((pattern_line, pattern, actions)) = current_pattern.take() {
                rules.push(compile_header_rule(pattern_line, &pattern, actions)?);
            }
            current_pattern = Some((line_no, trimmed.to_string(), Vec::new()));
            continue;
        }

        let Some((_, _, actions)) = current_pattern.as_mut() else {
            return Err(PlatformError::bad_request(format!(
                "_headers line {line_no} has a header action without a matching path rule"
            )));
        };

        if let Some(name) = trimmed.strip_prefix("! ") {
            let name = name.trim();
            if name.is_empty() {
                return Err(PlatformError::bad_request(format!(
                    "_headers line {line_no} removes an empty header name"
                )));
            }
            actions.push(HeaderAction::Remove {
                name: name.to_string(),
            });
            continue;
        }

        let Some((name, value)) = trimmed.split_once(':') else {
            return Err(PlatformError::bad_request(format!(
                "_headers line {line_no} must be `Name: value` or `! Name`"
            )));
        };
        let name = name.trim();
        if name.is_empty() {
            return Err(PlatformError::bad_request(format!(
                "_headers line {line_no} has an empty header name"
            )));
        }
        actions.push(HeaderAction::Set {
            name: name.to_string(),
            value: value.trim().to_string(),
        });
    }

    if let Some((pattern_line, pattern, actions)) = current_pattern.take() {
        rules.push(compile_header_rule(pattern_line, &pattern, actions)?);
    }

    Ok(rules)
}

fn compile_header_rule(
    line_no: usize,
    pattern: &str,
    actions: Vec<HeaderAction>,
) -> Result<HeaderRule> {
    if actions.is_empty() {
        return Err(PlatformError::bad_request(format!(
            "_headers line {line_no} defines a path rule without any headers"
        )));
    }

    let mut seen_placeholders = HashSet::new();
    let mut saw_splat = false;
    let (host_matcher, path_matcher) = if let Some(rest) = pattern.strip_prefix("https://") {
        let (host, path) = match rest.split_once('/') {
            Some((host, suffix)) => (host, format!("/{}", suffix)),
            None => (rest, "/".to_string()),
        };
        if host.is_empty() {
            return Err(PlatformError::bad_request(format!(
                "_headers line {line_no} is missing a host in absolute rule {pattern:?}"
            )));
        }
        if host_pattern_has_port(host) {
            return Err(PlatformError::bad_request(format!(
                "_headers line {line_no} absolute rule must not include a port: {pattern:?}"
            )));
        }
        if path.contains('?') || path.contains('#') {
            return Err(PlatformError::bad_request(format!(
                "_headers line {line_no} absolute rule must not include query or fragment: {pattern:?}"
            )));
        }
        (
            Some(compile_pattern_matcher(
                host,
                HostOrPath::Host,
                &mut seen_placeholders,
                &mut saw_splat,
                line_no,
            )?),
            compile_pattern_matcher(
                &path,
                HostOrPath::Path,
                &mut seen_placeholders,
                &mut saw_splat,
                line_no,
            )?,
        )
    } else {
        if !pattern.starts_with('/') {
            return Err(PlatformError::bad_request(format!(
                "_headers line {line_no} must start with `/` or `https://`: {pattern:?}"
            )));
        }
        (
            None,
            compile_pattern_matcher(
                pattern,
                HostOrPath::Path,
                &mut seen_placeholders,
                &mut saw_splat,
                line_no,
            )?,
        )
    };

    Ok(HeaderRule {
        host_matcher,
        path_matcher,
        actions,
    })
}

enum HostOrPath {
    Host,
    Path,
}

fn compile_pattern_matcher(
    pattern: &str,
    kind: HostOrPath,
    seen_placeholders: &mut HashSet<String>,
    saw_splat: &mut bool,
    line_no: usize,
) -> Result<PatternMatcher> {
    let mut regex = String::from("^");
    let bytes = pattern.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        let current = bytes[index] as char;
        if current == '*' {
            if *saw_splat {
                return Err(PlatformError::bad_request(format!(
                    "_headers line {line_no} may only use one `*` splat per rule"
                )));
            }
            *saw_splat = true;
            regex.push_str("(?P<splat>.*)");
            index += 1;
            continue;
        }

        if current == ':' {
            let Some(next) = bytes.get(index + 1).copied().map(char::from) else {
                regex.push_str(&regex::escape(":"));
                index += 1;
                continue;
            };
            if !next.is_ascii_alphabetic() {
                regex.push_str(&regex::escape(":"));
                index += 1;
                continue;
            }

            let start = index + 1;
            let mut end = start + 1;
            while end < bytes.len() {
                let value = bytes[end] as char;
                if value.is_ascii_alphanumeric() || value == '_' {
                    end += 1;
                } else {
                    break;
                }
            }
            let name = &pattern[start..end];
            if !seen_placeholders.insert(name.to_string()) {
                return Err(PlatformError::bad_request(format!(
                    "_headers line {line_no} repeats placeholder :{name}"
                )));
            }
            let capture = match kind {
                HostOrPath::Host => "[^./]+",
                HostOrPath::Path => "[^/]+",
            };
            regex.push_str(&format!("(?P<{name}>{capture})"));
            index = end;
            continue;
        }

        regex.push_str(&regex::escape(&current.to_string()));
        index += 1;
    }

    regex.push('$');
    let regex = Regex::new(&regex).map_err(|error| {
        PlatformError::internal(format!(
            "failed to compile _headers rule regex for {pattern:?}: {error}"
        ))
    })?;
    Ok(PatternMatcher { regex })
}

impl HeaderRule {
    fn capture(&self, host: Option<&str>, path: &str) -> Option<HashMap<String, String>> {
        let mut values = HashMap::new();
        if let Some(host_matcher) = &self.host_matcher {
            let captures = host_matcher.capture(host?)?;
            values.extend(captures);
        }
        let path_captures = self.path_matcher.capture(path)?;
        values.extend(path_captures);
        Some(values)
    }
}

impl PatternMatcher {
    fn capture(&self, value: &str) -> Option<HashMap<String, String>> {
        let captures = self.regex.captures(value)?;
        let mut out = HashMap::new();
        for name in self.regex.capture_names().flatten() {
            if let Some(value) = captures.name(name) {
                out.insert(name.to_string(), value.as_str().to_string());
            }
        }
        Some(out)
    }
}

fn substitute_header_value(value: &str, captures: &HashMap<String, String>) -> String {
    let mut out = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        let current = bytes[index] as char;
        if current != ':' {
            out.push(current);
            index += 1;
            continue;
        }

        let start = index + 1;
        let mut end = start;
        while end < bytes.len() {
            let value = bytes[end] as char;
            if value.is_ascii_alphanumeric() || value == '_' {
                end += 1;
            } else {
                break;
            }
        }
        if end == start {
            out.push(':');
            index += 1;
            continue;
        }

        let name = &value[start..end];
        if let Some(replacement) = captures.get(name) {
            out.push_str(replacement);
        } else {
            out.push(':');
            out.push_str(name);
        }
        index = end;
    }

    out
}

fn normalize_asset_path(path: &str) -> Result<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(PlatformError::bad_request("asset path must not be empty"));
    }
    if !trimmed.starts_with('/') {
        return Err(PlatformError::bad_request(format!(
            "asset path must start with '/': {trimmed}"
        )));
    }
    if trimmed.contains('?') || trimmed.contains('#') {
        return Err(PlatformError::bad_request(format!(
            "asset path must not include query or fragment: {trimmed}"
        )));
    }

    let mut segments = Vec::new();
    for segment in trimmed.split('/') {
        if segment.is_empty() {
            continue;
        }
        if segment == "." || segment == ".." {
            return Err(PlatformError::bad_request(format!(
                "asset path must not contain traversal segments: {trimmed}"
            )));
        }
        segments.push(segment);
    }

    if segments.is_empty() {
        return Err(PlatformError::bad_request("asset path must not be root"));
    }

    Ok(format!("/{}", segments.join("/")))
}

fn normalize_request_host(host: &str) -> String {
    host.split(':')
        .next()
        .unwrap_or(host)
        .trim()
        .to_ascii_lowercase()
}

fn host_pattern_has_port(host: &str) -> bool {
    let Some((prefix, suffix)) = host.rsplit_once(':') else {
        return false;
    };
    !prefix.is_empty() && !suffix.is_empty() && suffix.chars().all(|char| char.is_ascii_digit())
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{compile_asset_bundle, resolve_asset, AssetRequest};
    use base64::Engine;
    use common::DeployAsset;

    fn single_asset() -> DeployAsset {
        DeployAsset {
            path: "/a.js".to_string(),
            content_base64: base64::engine::general_purpose::STANDARD.encode(b"console.log('a');"),
        }
    }

    #[test]
    fn serves_asset_with_defaults() {
        let bundle = compile_asset_bundle(&[single_asset()], None).expect("bundle");
        let response = resolve_asset(
            &bundle,
            AssetRequest {
                method: "GET",
                host: Some("example.com"),
                path: "/a.js",
                headers: &[],
            },
        )
        .expect("asset should match");
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"console.log('a');");
        assert!(response
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("etag")));
    }

    #[test]
    fn header_rules_can_merge_and_remove_defaults() {
        let bundle = compile_asset_bundle(
            &[single_asset()],
            Some(
                "/a.js\n  Cache-Control: public, max-age=3600\n  X-One: hello\n/*\n  X-Two: :splat\n/a.js\n  ! Content-Type\n",
            ),
        )
        .expect("bundle");
        let response = resolve_asset(
            &bundle,
            AssetRequest {
                method: "GET",
                host: Some("example.com"),
                path: "/a.js",
                headers: &[],
            },
        )
        .expect("asset should match");
        assert!(response
            .headers
            .iter()
            .any(|(name, value)| name == "cache-control" && value.contains("max-age=3600")));
        assert!(response
            .headers
            .iter()
            .all(|(name, _)| !name.eq_ignore_ascii_case("content-type")));
        assert!(response
            .headers
            .iter()
            .any(|(name, value)| name == "x-two" && value == "a.js"));
    }

    #[test]
    fn absolute_host_rule_uses_host_and_placeholders() {
        let bundle = compile_asset_bundle(
            &[single_asset()],
            Some("https://:sub.example.com/a.js\n  X-Host: :sub\n"),
        )
        .expect("bundle");
        let response = resolve_asset(
            &bundle,
            AssetRequest {
                method: "GET",
                host: Some("foo.example.com:443"),
                path: "/a.js",
                headers: &[],
            },
        )
        .expect("asset should match");
        assert!(response
            .headers
            .iter()
            .any(|(name, value)| name == "x-host" && value == "foo"));
    }

    #[test]
    fn invalid_duplicate_placeholder_is_rejected() {
        let error = compile_asset_bundle(
            &[single_asset()],
            Some("https://:sub.:sub.example.com/a.js\n  X-Test: nope\n"),
        )
        .expect_err("duplicate placeholder should fail");
        assert!(error.to_string().contains("repeats placeholder"));
    }

    #[test]
    fn absolute_host_rule_rejects_explicit_ports() {
        let error = compile_asset_bundle(
            &[single_asset()],
            Some("https://example.com:8443/a.js\n  X-Test: nope\n"),
        )
        .expect_err("port should fail");
        assert!(error.to_string().contains("must not include a port"));
    }
}
