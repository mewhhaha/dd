//! The web platform layer: the scripts behind `URL`, `fetch`'s classes,
//! streams, encoding, events, and WebCrypto (vendored from Deno and owned by
//! dd), and the ops they call.

use dd_v8::{OpDecl, OpError, op_async, op_raw, op_sync, v8};

mod blob;
mod crypto;
mod encoding;
mod time;
mod url_parse;
mod urlpattern;

macro_rules! ext_scripts {
    ($($specifier:literal => $path:literal),* $(,)?) => {
        /// The web layer's scripts, loaded on first use by
        /// `core.loadExtScript(specifier)`. Each evaluates to its exports.
        const EXT_SCRIPTS: &[(&str, &str)] = &[$(($specifier, include_str!($path))),*];
    };
}

ext_scripts! {
    "ext:deno_webidl/00_webidl.js" => "../../js/vendor/deno_webidl/00_webidl.js",
    "ext:deno_web/00_infra.js" => "../../js/vendor/deno_web/00_infra.js",
    "ext:deno_web/00_url.js" => "../../js/vendor/deno_web/00_url.js",
    "ext:deno_web/01_console.js" => "../../js/web/console.js",
    "ext:deno_web/01_dom_exception.js" => "../../js/vendor/deno_web/01_dom_exception.js",
    "ext:deno_web/01_mimesniff.js" => "../../js/vendor/deno_web/01_mimesniff.js",
    "ext:deno_web/01_urlpattern.js" => "../../js/vendor/deno_web/01_urlpattern.js",
    "ext:deno_web/02_event.js" => "../../js/vendor/deno_web/02_event.js",
    "ext:deno_web/02_structured_clone.js" => "../../js/vendor/deno_web/02_structured_clone.js",
    "ext:deno_web/03_abort_signal.js" => "../../js/vendor/deno_web/03_abort_signal.js",
    "ext:deno_web/06_streams.js" => "../../js/vendor/deno_web/06_streams.js",
    "ext:deno_web/08_text_encoding.js" => "../../js/vendor/deno_web/08_text_encoding.js",
    "ext:deno_web/09_file.js" => "../../js/vendor/deno_web/09_file.js",
    "ext:deno_web/12_location.js" => "../../js/vendor/deno_web/12_location.js",
    "ext:deno_web/13_message_port.js" => "../../js/vendor/deno_web/13_message_port.js",
    "ext:deno_web/15_performance.js" => "../../js/vendor/deno_web/15_performance.js",
    "ext:deno_fetch/20_headers.js" => "../../js/vendor/deno_fetch/20_headers.js",
    "ext:deno_fetch/21_formdata.js" => "../../js/vendor/deno_fetch/21_formdata.js",
    "ext:deno_fetch/22_body.js" => "../../js/vendor/deno_fetch/22_body.js",
    "ext:deno_fetch/22_http_client.js" => "../../js/web/http_client.js",
    "ext:deno_fetch/23_request.js" => "../../js/vendor/deno_fetch/23_request.js",
    "ext:deno_fetch/23_response.js" => "../../js/vendor/deno_fetch/23_response.js",
    "ext:deno_crypto/00_crypto.js" => "../../js/vendor/deno_crypto/00_crypto.js",
}

pub(crate) fn ops() -> Vec<OpDecl> {
    vec![
        op_raw!(op_load_ext_script),
        op_raw!(op_url_parse),
        op_raw!(op_url_parse_with_base),
        op_raw!(op_url_reparse),
        op_sync!(op_url_get_serialization),
        op_raw!(op_url_parse_search_params),
        op_sync!(op_url_stringify_search_params),
        op_sync!(op_urlpattern_parse),
        op_raw!(op_urlpattern_process_match_input),
        op_sync!(op_encoding_normalize_label),
        op_raw!(op_encoding_decode_utf8),
        op_raw!(op_encoding_decode_utf8_ascii_only),
        op_sync!(op_encoding_decode_single),
        op_sync!(op_encoding_new_decoder),
        op_sync!(op_encoding_decode),
        op_sync!(op_encoding_drop_decoder),
        op_raw!(op_encoding_encode_into),
        op_sync!(op_base64_decode),
        op_sync!(op_base64_encode_from_buffer),
        op_sync!(op_blob_create_part),
        op_sync!(op_blob_slice_part),
        op_async!(op_blob_read_part),
        op_sync!(op_blob_remove_part),
        op_sync!(op_blob_clone_part),
        op_sync!(op_blob_create_object_url),
        op_sync!(op_blob_revoke_object_url),
        op_sync!(op_blob_from_object_url),
        op_sync!(op_now),
        op_sync!(op_time_origin),
    ]
    .into_iter()
    .chain(crypto::ops())
    .collect()
}

use blob::*;
use encoding::*;
use time::*;
use url_parse::*;
use urlpattern::*;

/// `op_load_ext_script(specifier)`: runs one of [`EXT_SCRIPTS`] and returns
/// its completion value. `core.loadExtScript` caches the result.
fn op_load_ext_script<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: &v8::FunctionCallbackArguments<'s>,
    rv: &mut v8::ReturnValue<'s, v8::Value>,
) {
    let specifier = args.get(0).to_rust_string_lossy(scope);
    let Some((_, source)) = EXT_SCRIPTS.iter().find(|(name, _)| *name == specifier) else {
        let error = OpError::type_error(format!("unknown web layer script: {specifier}"))
            .to_exception(scope);
        scope.throw_exception(error);
        return;
    };
    let (Some(name), Some(source)) = (
        v8::String::new(scope, &specifier),
        v8::String::new(scope, source),
    ) else {
        return;
    };
    let origin = v8::ScriptOrigin::new(
        scope,
        name.into(),
        0,
        0,
        false,
        -1,
        None,
        false,
        false,
        false,
        None,
    );
    let Some(script) = v8::Script::compile(scope, source, Some(&origin)) else {
        return;
    };
    if let Some(exports) = script.run(scope) {
        rv.set(exports);
    }
}
