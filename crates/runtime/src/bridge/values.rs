//! Value-domain helpers behind the bridge: promises, JSON, URL, dates,
//! encodings, and the minimal regex engine. Split from the dispatcher for
//! navigability; semantics unchanged.

use super::fail;
use crate::heap::{HostValue, ObjectData, PromiseState};
use crate::nanbox::{JsValue, TAG_NULL, decode, encode, encode_bool, encode_number};
use crate::state::{HostState, Microtask};
use std::time::{SystemTime, UNIX_EPOCH};
use wasmtime::Caller;

/// Chain a callback onto a promise (`p.then(cb)`): allocate the downstream
/// promise and either queue the reaction as a microtask (already resolved)
/// or register it for later. Non-promise receivers pass through unchanged.
pub(super) fn promise_then(state: &mut HostState, parent: u64, callback_bits: u64) -> u64 {
    let JsValue::Handle(parent_id) = decode(parent) else {
        return parent;
    };
    let resolved_value = match state.heap.handle(parent_id) {
        Some(HostValue::Promise {
            state: PromiseState::Resolved(v),
            ..
        }) => Some(*v),
        Some(HostValue::Promise { .. }) => None,
        _ => return parent,
    };
    let downstream = state.heap.alloc(HostValue::Promise {
        state: PromiseState::Pending,
        reactions: Vec::new(),
    });
    match resolved_value {
        Some(value) => state.microtasks.push_back(Microtask {
            callback_bits,
            value_bits: value,
            downstream,
        }),
        None => {
            if let Some(HostValue::Promise { reactions, .. }) = state.heap.handle_mut(parent_id) {
                reactions.push(crate::heap::PromiseReaction {
                    callback_bits,
                    downstream,
                });
            }
        }
    }
    encode(JsValue::Handle(downstream))
}

/// Resolve a promise and queue its reactions as microtasks.
pub fn resolve_promise(state: &mut HostState, promise_bits: u64, value_bits: u64) {
    let JsValue::Handle(id) = decode(promise_bits) else {
        return;
    };
    let reactions = match state.heap.handle_mut(id) {
        Some(HostValue::Promise {
            state: promise_state,
            reactions,
        }) => {
            if *promise_state != PromiseState::Pending {
                return;
            }
            *promise_state = PromiseState::Resolved(value_bits);
            std::mem::take(reactions)
        }
        _ => return,
    };
    for reaction in reactions {
        state.microtasks.push_back(Microtask {
            callback_bits: reaction.callback_bits,
            value_bits,
            downstream: reaction.downstream,
        });
    }
}

/// Record a guest-visible exception the way the JS glue does: only inside a
/// `try` block; otherwise the failure surfaces as `undefined` plus a log line.
pub(super) fn throw_in_guest(caller: &mut Caller<'_, HostState>, message: &str) {
    let state = caller.data_mut();
    if state.try_depth > 0 {
        let message_bits = state.heap.intern_bits(message.to_string());
        let error = state.heap.alloc_bits(HostValue::Error { message_bits });
        state.pending_exception = Some(error);
    } else {
        tracing::warn!(target: "perry_worker", "uncaught in worker: {message}");
    }
}

pub(super) fn epoch_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

pub(super) fn date_ms(state: &HostState, bits: u64) -> f64 {
    match decode(bits) {
        JsValue::Handle(id) => match state.heap.handle(id) {
            Some(HostValue::Date(ms)) => *ms,
            _ => f64::NAN,
        },
        _ => state.heap.to_number(bits),
    }
}

/// Civil date-time in UTC decomposed from epoch milliseconds, via the
/// days-from-civil algorithm (Howard Hinnant's date algorithms).
pub(super) struct CivilDateTime {
    pub(super) year: i64,
    pub(super) month: u32,
    pub(super) day: u32,
    pub(super) weekday: u32,
    pub(super) hour: u32,
    pub(super) minute: u32,
    pub(super) second: u32,
    pub(super) millisecond: u32,
}

impl CivilDateTime {
    pub(super) fn from_ms(ms: f64) -> Self {
        let total_ms = ms as i64;
        let days = total_ms.div_euclid(86_400_000);
        let in_day = total_ms.rem_euclid(86_400_000);
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let year = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
        let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
        let year = if month <= 2 { year + 1 } else { year };
        let weekday = ((days % 7 + 7 + 4) % 7) as u32;
        CivilDateTime {
            year,
            month,
            day,
            weekday,
            hour: (in_day / 3_600_000) as u32,
            minute: (in_day / 60_000 % 60) as u32,
            second: (in_day / 1000 % 60) as u32,
            millisecond: (in_day % 1000) as u32,
        }
    }
}

pub(super) fn iso_from_ms(ms: f64) -> String {
    let civil = CivilDateTime::from_ms(ms);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        civil.year,
        civil.month,
        civil.day,
        civil.hour,
        civil.minute,
        civil.second,
        civil.millisecond
    )
}

pub(super) fn url_object(state: &mut HostState, parsed: &url::Url) -> u64 {
    let pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let search_params = state.heap.alloc_bits(HostValue::SearchParams(pairs));
    let mut props: Vec<(String, u64)> = Vec::new();
    let put = |state: &mut HostState, props: &mut Vec<(String, u64)>, key: &str, value: String| {
        let bits = state.heap.intern_bits(value);
        props.push((key.to_string(), bits));
    };
    put(state, &mut props, "href", parsed.to_string());
    put(
        state,
        &mut props,
        "protocol",
        format!("{}:", parsed.scheme()),
    );
    put(
        state,
        &mut props,
        "hostname",
        parsed.host_str().unwrap_or("").to_string(),
    );
    put(
        state,
        &mut props,
        "port",
        parsed.port().map(|p| p.to_string()).unwrap_or_default(),
    );
    put(state, &mut props, "pathname", parsed.path().to_string());
    put(
        state,
        &mut props,
        "search",
        parsed.query().map(|q| format!("?{q}")).unwrap_or_default(),
    );
    put(
        state,
        &mut props,
        "hash",
        parsed
            .fragment()
            .map(|f| format!("#{f}"))
            .unwrap_or_default(),
    );
    put(
        state,
        &mut props,
        "origin",
        parsed.origin().ascii_serialization(),
    );
    props.push(("searchParams".to_string(), search_params));
    state.heap.alloc_bits(HostValue::Object(ObjectData {
        class: Some("URL".to_string()),
        props,
    }))
}

pub(super) fn form_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

pub(super) fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err(format!("odd-length hex string ({} chars)", text.len()));
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

/// Convert a heap value to JSON. Returns `None` for values JS omits
/// (`undefined`, functions), matching `JSON.stringify` semantics.
pub(super) fn heap_to_json(
    state: &HostState,
    bits: u64,
    depth: usize,
) -> Result<Option<serde_json::Value>, wasmtime::Error> {
    if depth > 128 {
        return Err(fail(
            "json_stringify",
            "value nesting exceeds 128 levels (cycle?)",
        ));
    }
    let value = match decode(bits) {
        JsValue::Undefined => return Ok(None),
        JsValue::Null => serde_json::Value::Null,
        JsValue::Bool(b) => serde_json::Value::Bool(b),
        JsValue::Number(n) => {
            // JS prints integral numbers without a decimal point.
            if n == n.trunc() && n.abs() <= i64::MAX as f64 {
                serde_json::Value::Number(serde_json::Number::from(n as i64))
            } else {
                serde_json::Number::from_f64(n)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            }
        }
        JsValue::Str(id) => {
            serde_json::Value::String(state.heap.string(id).unwrap_or("").to_string())
        }
        JsValue::Handle(id) => match state.heap.handle(id) {
            Some(HostValue::Array(items)) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(
                        heap_to_json(state, *item, depth + 1)?.unwrap_or(serde_json::Value::Null),
                    );
                }
                serde_json::Value::Array(out)
            }
            Some(HostValue::Object(object)) => {
                let mut out = serde_json::Map::new();
                for (key, prop_bits) in &object.props {
                    if key == "__class__" {
                        continue;
                    }
                    if let Some(v) = heap_to_json(state, *prop_bits, depth + 1)? {
                        out.insert(key.clone(), v);
                    }
                }
                serde_json::Value::Object(out)
            }
            Some(HostValue::Date(ms)) => serde_json::Value::String(iso_from_ms(*ms)),
            Some(HostValue::Closure { .. }) => return Ok(None),
            Some(HostValue::MapValue(_)) | Some(HostValue::SetValue(_)) => {
                serde_json::Value::Object(serde_json::Map::new())
            }
            Some(HostValue::SearchParams(_)) | Some(HostValue::Regexp { .. }) => {
                serde_json::Value::Object(serde_json::Map::new())
            }
            Some(HostValue::Error { message_bits }) => {
                serde_json::Value::String(state.heap.display(*message_bits))
            }
            Some(HostValue::Buffer(_)) | Some(HostValue::Promise { .. }) | None => {
                serde_json::Value::Object(serde_json::Map::new())
            }
        },
    };
    Ok(Some(value))
}

pub(super) fn json_to_heap(state: &mut HostState, value: &serde_json::Value) -> u64 {
    match value {
        serde_json::Value::Null => TAG_NULL,
        serde_json::Value::Bool(b) => encode_bool(*b),
        serde_json::Value::Number(n) => encode_number(n.as_f64().unwrap_or(f64::NAN)),
        serde_json::Value::String(s) => state.heap.intern_bits(s.clone()),
        serde_json::Value::Array(items) => {
            let bits: Vec<u64> = items.iter().map(|item| json_to_heap(state, item)).collect();
            state.heap.alloc_bits(HostValue::Array(bits))
        }
        serde_json::Value::Object(map) => {
            let props: Vec<(String, u64)> = map
                .iter()
                .map(|(key, item)| (key.clone(), json_to_heap(state, item)))
                .collect();
            state
                .heap
                .alloc_bits(HostValue::Object(ObjectData { class: None, props }))
        }
    }
}

/// Serialize a heap value with host-side `JSON.stringify` semantics; entry
/// point for the `ffi.dd_json` helper.
pub fn json_stringify_bits(
    state: &HostState,
    bits: u64,
) -> Result<Option<String>, wasmtime::Error> {
    match heap_to_json(state, bits, 0)? {
        Some(value) => Ok(Some(
            serde_json::to_string(&value).map_err(|e| fail("dd_json", e))?,
        )),
        None => Ok(None),
    }
}

/// Decode a heap value to JSON (`None` for `undefined`/functions), used by
/// the tvar persistence in `dd_memory_atomic`.
pub fn heap_to_json_value(
    state: &HostState,
    bits: u64,
) -> Result<Option<serde_json::Value>, wasmtime::Error> {
    heap_to_json(state, bits, 0)
}

/// Materialize a JSON value into the heap, used to hand persisted tvars back
/// to the guest.
pub fn json_value_to_heap(state: &mut HostState, value: &serde_json::Value) -> u64 {
    json_to_heap(state, value)
}

/// Extremely small regex support: Perry's `regexp_test` goes through the host,
/// and full JS regex is out of scope for the experiment. Literal text plus
/// `^`/`$` anchors and `.` are handled; anything else is rejected loudly.
pub(super) struct LiteRegex {
    anchored_start: bool,
    anchored_end: bool,
    case_insensitive: bool,
    pattern: Vec<Option<char>>,
}

pub(super) fn regex_lite_compile(pattern: &str) -> Result<LiteRegex, String> {
    let case_insensitive = pattern.starts_with("(?i)");
    let body = pattern.strip_prefix("(?i)").unwrap_or(pattern);
    let anchored_start = body.starts_with('^');
    let body = body.strip_prefix('^').unwrap_or(body);
    let anchored_end = body.ends_with('$') && !body.ends_with("\\$");
    let body = if anchored_end {
        &body[..body.len() - 1]
    } else {
        body
    };
    let mut chars = body.chars().peekable();
    let mut out = Vec::new();
    while let Some(c) = chars.next() {
        match c {
            '.' => out.push(None),
            '\\' => match chars.next() {
                Some(escaped) => out.push(Some(escaped)),
                None => return Err("trailing backslash".to_string()),
            },
            '[' | ']' | '(' | ')' | '{' | '}' | '*' | '+' | '?' | '|' => {
                return Err(format!("unsupported regex construct {c:?}"));
            }
            other => out.push(Some(other)),
        }
    }
    Ok(LiteRegex {
        anchored_start,
        anchored_end,
        case_insensitive,
        pattern: out,
    })
}

impl LiteRegex {
    pub(super) fn is_match(&self, text: &str) -> bool {
        let haystack: Vec<char> = if self.case_insensitive {
            text.to_lowercase().chars().collect()
        } else {
            text.chars().collect()
        };
        let needle: Vec<Option<char>> = if self.case_insensitive {
            self.pattern
                .iter()
                .map(|c| c.map(|c| c.to_lowercase().next().unwrap_or(c)))
                .collect()
        } else {
            self.pattern.clone()
        };
        let fits = |start: usize| -> bool {
            if start + needle.len() > haystack.len() {
                return false;
            }
            needle
                .iter()
                .zip(&haystack[start..])
                .all(|(want, have)| want.is_none_or(|c| c == *have))
        };
        match (self.anchored_start, self.anchored_end) {
            (true, true) => haystack.len() == needle.len() && fits(0),
            (true, false) => fits(0),
            (false, true) => haystack.len() >= needle.len() && fits(haystack.len() - needle.len()),
            (false, false) => (0..=haystack.len().saturating_sub(needle.len())).any(fits),
        }
    }
}
