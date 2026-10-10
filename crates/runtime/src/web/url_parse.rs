// URL parsing for `URL` and `URLSearchParams`, ported from deno_web's
// url.rs (Copyright 2018-2026 the Deno authors, MIT license).
//
// The parse ops write the URL's component offsets into the `Uint32Array`
// they are given and return a status; a URL whose serialization differs from
// its input leaves the serialization for `op_url_get_serialization`.

use dd_v8::builtins::{buffer_bytes, write_u32s};
use dd_v8::{OpError, OpState, runtime_op_state, v8};
use url::{Url, form_urlencoded, quirks};

const STATUS_OK: u32 = 0;
const STATUS_OK_SERIALIZATION: u32 = 1;
const STATUS_ERR: u32 = 2;
const NO_PORT: u32 = 65536;

struct UrlSerialization(String);

fn string_arg(scope: &mut v8::PinScope<'_, '_>, value: v8::Local<v8::Value>) -> Option<String> {
    value
        .to_string(scope)
        .map(|value| value.to_rust_string_lossy(scope))
}

fn finish(
    scope: &mut v8::PinScope<'_, '_>,
    href: &str,
    url: Url,
    buf: v8::Local<v8::Value>,
    no_port: u32,
) -> u32 {
    let components = quirks::internal_components(&url);
    write_u32s(
        buf,
        &[
            components.scheme_end,
            components.username_end,
            components.host_start,
            components.host_end,
            components.port.map(u32::from).unwrap_or(no_port),
            components.path_start,
            components.query_start.unwrap_or(0),
            components.fragment_start.unwrap_or(0),
        ],
    );
    let serialization: String = url.into();
    if serialization == href {
        STATUS_OK
    } else {
        runtime_op_state(scope)
            .borrow_mut()
            .put(UrlSerialization(serialization));
        STATUS_OK_SERIALIZATION
    }
}

/// `op_url_parse(href, componentsBuf) -> status`
pub(super) fn op_url_parse<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let Some(href) = string_arg(scope, args.get(0)) else {
        return;
    };
    let buf = args.get(1);
    if let Some(components) = parse_simple_special_url(&href) {
        write_u32s(buf, &components);
        return rv.set_uint32(STATUS_OK);
    }
    let status = match Url::parse(&href) {
        Ok(url) => finish(scope, &href, url, buf, 0),
        Err(_) => STATUS_ERR,
    };
    rv.set_uint32(status);
}

/// `op_url_parse_with_base(href, baseHref, componentsBuf) -> status`
pub(super) fn op_url_parse_with_base<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let (Some(href), Some(base)) = (
        string_arg(scope, args.get(0)),
        string_arg(scope, args.get(1)),
    ) else {
        return;
    };
    let status = match Url::parse(&base) {
        Ok(base) => match Url::options().base_url(Some(&base)).parse(&href) {
            Ok(url) => finish(scope, &href, url, args.get(2), 0),
            Err(_) => STATUS_ERR,
        },
        Err(_) => STATUS_ERR,
    };
    rv.set_uint32(status);
}

/// `op_url_reparse(href, setter, value, componentsBuf) -> status`
pub(super) fn op_url_reparse<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let (Some(href), Some(value)) = (
        string_arg(scope, args.get(0)),
        string_arg(scope, args.get(2)),
    ) else {
        return;
    };
    let setter = args.get(1).uint32_value(scope).unwrap_or(u32::MAX);
    let Ok(mut url) = Url::parse(&href) else {
        return rv.set_uint32(STATUS_ERR);
    };
    let value = value.as_str();
    let result = match setter {
        0 => {
            quirks::set_hash(&mut url, value);
            Ok(())
        }
        1 => quirks::set_host(&mut url, value),
        2 => quirks::set_hostname(&mut url, value),
        3 => quirks::set_password(&mut url, value),
        4 => {
            quirks::set_pathname(&mut url, value);
            Ok(())
        }
        5 => quirks::set_port(&mut url, value),
        6 => quirks::set_protocol(&mut url, value),
        7 => {
            quirks::set_search(&mut url, value);
            Ok(())
        }
        8 => quirks::set_username(&mut url, value),
        _ => Err(()),
    };
    let status = match result {
        Ok(()) => finish(scope, &href, url, args.get(3), NO_PORT),
        Err(()) => STATUS_ERR,
    };
    rv.set_uint32(status);
}

pub(super) fn op_url_get_serialization(state: &mut OpState) -> String {
    state
        .try_take::<UrlSerialization>()
        .map(|value| value.0)
        .unwrap_or_default()
}

/// `op_url_parse_search_params(string)` or `op_url_parse_search_params(null, bytes)`
pub(super) fn op_url_parse_search_params<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let input = if args.get(0).is_string() {
        args.get(0).to_rust_string_lossy(scope).into_bytes()
    } else if let Some(bytes) = buffer_bytes(args.get(1)) {
        bytes
    } else {
        let error = OpError::type_error("invalid parameters").to_exception(scope);
        scope.throw_exception(error);
        return;
    };
    let pairs = form_urlencoded::parse(&input)
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    match dd_v8::serde_v8::to_v8(scope, &pairs) {
        Ok(value) => rv.set(value),
        Err(error) => {
            let error = OpError::from(error).to_exception(scope);
            scope.throw_exception(error);
        }
    }
}

pub(super) fn op_url_stringify_search_params(
    _state: &mut OpState,
    pairs: Vec<(String, String)>,
) -> String {
    form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish()
}

// Keep in sync with parseSimpleSpecialUrl() in 00_url.js.
fn parse_simple_special_url(href: &str) -> Option<[u32; 8]> {
    let bytes = href.as_bytes();
    let (scheme_end, default_port) = if bytes.starts_with(b"http://") {
        (4, 80u32)
    } else if bytes.starts_with(b"https://") {
        (5, 443u32)
    } else {
        return None;
    };

    let host_start = scheme_end + 3;
    let mut path_start = host_start;
    while path_start < bytes.len() && bytes[path_start] != b'/' {
        match bytes[path_start] {
            b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-' | b':' => path_start += 1,
            _ => return None,
        }
    }
    if path_start == host_start || path_start == bytes.len() {
        return None;
    }

    let mut host_end = path_start;
    let mut port = NO_PORT;
    for i in host_start..path_start {
        if bytes[i] == b':' {
            if i == host_start || i + 1 == path_start {
                return None;
            }
            host_end = i;
            port = 0;
            if i + 2 < path_start && bytes[i + 1] == b'0' {
                return None;
            }
            for &byte in &bytes[i + 1..path_start] {
                if !byte.is_ascii_digit() {
                    return None;
                }
                port = port.checked_mul(10)?.checked_add(u32::from(byte - b'0'))?;
                if port > 65535 {
                    return None;
                }
            }
            if port == default_port {
                return None;
            }
            break;
        }
    }
    if !simple_special_host_is_canonical(&bytes[host_start..host_end]) {
        return None;
    }

    let mut query_start = 0u32;
    for i in path_start..bytes.len() {
        if bytes[i] == b'.'
            && i > path_start
            && bytes[i - 1] == b'/'
            && (i + 1 == bytes.len() || matches!(bytes[i + 1], b'/' | b'?' | b'.'))
        {
            return None;
        }
        if query_start != 0 && bytes[i] == b'\'' {
            return None;
        }
        match bytes[i] {
            b'a'..=b'z'
            | b'A'..=b'Z'
            | b'0'..=b'9'
            | b'/'
            | b'.'
            | b'_'
            | b'~'
            | b'-'
            | b'!'
            | b'$'
            | b'&'
            | b'\''
            | b'('
            | b')'
            | b'*'
            | b'+'
            | b','
            | b';'
            | b'='
            | b':'
            | b'@' => {}
            b'?' if query_start == 0 => query_start = i as u32,
            _ => return None,
        }
    }

    Some([
        scheme_end as u32,
        host_start as u32,
        host_start as u32,
        host_end as u32,
        port,
        path_start as u32,
        query_start,
        0,
    ])
}

fn simple_special_host_is_canonical(host: &[u8]) -> bool {
    if host.is_empty() || host[0] == b'.' || host[host.len() - 1] == b'.' {
        return false;
    }

    if host
        .iter()
        .all(|byte| byte.is_ascii_digit() || *byte == b'.')
    {
        let mut dots = 0;
        let mut part = 0u32;
        let mut part_len = 0;
        for (i, &byte) in host.iter().enumerate() {
            if byte == b'.' {
                if part_len == 0 || part > 255 || (part_len > 1 && host[i - part_len] == b'0') {
                    return false;
                }
                dots += 1;
                part = 0;
                part_len = 0;
                continue;
            }
            part = part * 10 + u32::from(byte - b'0');
            part_len += 1;
            if part_len > 3 {
                return false;
            }
        }
        return dots == 3
            && part_len != 0
            && part <= 255
            && (part_len == 1 || host[host.len() - part_len] != b'0');
    }

    if host[0].is_ascii_digit() {
        return false;
    }

    let mut label_len = 0;
    let mut label_start = 0;
    let mut final_label_all_digits = true;
    for (i, &byte) in host.iter().enumerate() {
        match byte {
            b'a'..=b'z' | b'0'..=b'9' | b'-' => {
                if !byte.is_ascii_digit() {
                    final_label_all_digits = false;
                }
                label_len += 1;
            }
            b'.' => {
                if label_len == 0 {
                    return false;
                }
                label_len = 0;
                label_start = i + 1;
                final_label_all_digits = true;
            }
            _ => return false,
        }
        if i == label_start + 3 && &host[label_start..=i] == b"xn--" {
            return false;
        }
    }
    label_len != 0
        && !final_label_all_digits
        && !(label_len >= 2 && &host[label_start..label_start + 2] == b"0x")
}
