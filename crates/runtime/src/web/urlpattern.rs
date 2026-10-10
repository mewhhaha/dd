// `URLPattern` parsing and matching, ported from deno_web's urlpattern.rs
// (Copyright 2018-2026 the Deno authors, MIT license).

use dd_v8::builtins::write_u32s;
use dd_v8::{OpError, OpState, serde_v8, v8};
use serde::Serialize;
use urlpattern::quirks::{self, StringOrInit, UrlPatternInit};

/// Echoes the offending pattern with a caret under the failing character, and
/// a hint for the common mistake of a `:` that does not start a named group.
fn enrich_error(error: urlpattern::Error, input: &StringOrInit) -> OpError {
    let message = error.to_string();
    let position = match error {
        urlpattern::Error::Tokenizer(_, position) => Some(position),
        _ => None,
    };
    let (pattern, caret) = match input {
        StringOrInit::String(pattern) => (Some(("URLPattern", pattern.as_str())), None),
        StringOrInit::Init(init) => (single_init_component(init), position),
    };
    let Some((name, pattern)) = pattern else {
        return OpError::type_error(message);
    };
    let mut out = format!("Failed to parse {name} from \"{pattern}\": {message}");
    if let Some(position) = caret {
        out.push_str("\n\n  ");
        out.push_str(pattern);
        out.push_str("\n  ");
        out.extend(std::iter::repeat_n(' ', position));
        out.push('^');
    }
    if message.contains("invalid name") {
        out.push_str(
            "\n\n  hint: \":\" starts a named group and must be followed by a name \
             (a letter or \"_\", then letters, digits or \"_\"). To match a literal \
             \":\", escape it as \"\\:\".",
        );
    }
    OpError::type_error(out)
}

fn single_init_component(init: &UrlPatternInit) -> Option<(&'static str, &str)> {
    let components: [(&'static str, &Option<String>); 8] = [
        ("protocol", &init.protocol),
        ("username", &init.username),
        ("password", &init.password),
        ("hostname", &init.hostname),
        ("port", &init.port),
        ("pathname", &init.pathname),
        ("search", &init.search),
        ("hash", &init.hash),
    ];
    let mut set = components
        .iter()
        .filter_map(|(name, value)| value.as_deref().map(|value| (*name, value)));
    match (set.next(), set.next()) {
        (Some(only), None) => Some(only),
        _ => None,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Component {
    pattern_string: String,
    regexp_string: String,
    group_name_list: Vec<String>,
}

impl From<quirks::UrlPatternComponent> for Component {
    fn from(component: quirks::UrlPatternComponent) -> Self {
        Self {
            pattern_string: component.pattern_string,
            regexp_string: component.regexp_string,
            group_name_list: component.group_name_list,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ParsedPattern {
    protocol: Component,
    username: Component,
    password: Component,
    hostname: Component,
    port: Component,
    pathname: Component,
    search: Component,
    hash: Component,
    has_regexp_groups: bool,
}

pub(super) fn op_urlpattern_parse(
    _state: &mut OpState,
    input: StringOrInit,
    base_url: Option<String>,
    options: urlpattern::UrlPatternOptions,
) -> Result<ParsedPattern, OpError> {
    let init = quirks::process_construct_pattern_input(input.clone(), base_url.as_deref())
        .map_err(|error| enrich_error(error, &input))?;
    let pattern =
        quirks::parse_pattern(init, options).map_err(|error| enrich_error(error, &input))?;
    Ok(ParsedPattern {
        protocol: pattern.protocol.into(),
        username: pattern.username.into(),
        password: pattern.password.into(),
        hostname: pattern.hostname.into(),
        port: pattern.port.into(),
        pathname: pattern.pathname.into(),
        search: pattern.search.into(),
        hash: pattern.hash.into(),
        has_regexp_groups: pattern.has_regexp_groups,
    })
}

/// `op_urlpattern_process_match_input(input, baseURL, offsetsBuf)`: the eight
/// URL components concatenated, with their start offsets (and the total
/// length) written to the 9-word buffer; `null` when the input is no URL.
pub(super) fn op_urlpattern_process_match_input<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let result = (|| -> Result<Option<(String, [u32; 9])>, OpError> {
        let input: StringOrInit = serde_v8::from_v8(scope, args.get(0))?;
        let base_url: Option<String> = serde_v8::from_v8(scope, args.get(1))?;
        let Some((input, _)) = quirks::process_match_input(input, base_url.as_deref())
            .map_err(|error| OpError::type_error(error.to_string()))?
        else {
            return Ok(None);
        };
        let Some(input) = quirks::parse_match_input(input) else {
            return Ok(None);
        };
        let fields = [
            &input.protocol,
            &input.username,
            &input.password,
            &input.hostname,
            &input.port,
            &input.pathname,
            &input.search,
            &input.hash,
        ];
        let mut offsets = [0u32; 9];
        let mut concat = String::with_capacity(fields.iter().map(|field| field.len()).sum());
        let mut offset = 0u32;
        for (index, field) in fields.iter().enumerate() {
            offsets[index] = offset;
            offset += field.len() as u32;
            concat.push_str(field);
        }
        offsets[8] = offset;
        Ok(Some((concat, offsets)))
    })();
    match result {
        Ok(Some((concat, offsets))) => {
            write_u32s(args.get(2), &offsets);
            match v8::String::new(scope, &concat) {
                Some(concat) => rv.set(concat.into()),
                None => rv.set_null(),
            }
        }
        Ok(None) => rv.set_null(),
        Err(error) => {
            let error = error.to_exception(scope);
            scope.throw_exception(error);
        }
    }
}
