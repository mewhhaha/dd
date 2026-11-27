//! The web platform layer: the scripts behind `URL`, `fetch`'s classes,
//! streams, encoding, events, and WebCrypto (vendored from Deno and owned by
//! dd), and the ops they call.

use dd_v8::{OpDecl, op_async, op_sync};

mod blob;
mod crypto;
mod encoding;
mod time;
mod url_parse;
mod urlpattern;

macro_rules! ext_scripts {
    ($($path:literal),* $(,)?) => {
        /// The web layer's scripts, precompiled into the AOT snapshot bundle
        /// and loaded on first use by `core.loadExtScript(specifier)`.
        pub(crate) const EXT_SCRIPTS: &[(&str, &str)] =
            &[$((concat!("ext:", $path), include_str!(concat!("../../js/", $path)))),*];
    };
}

ext_scripts! {
    "vendor/webidl/00_webidl.js",
    "vendor/web/00_infra.js",
    "vendor/web/00_url.js",
    "web/console.js",
    "vendor/web/01_dom_exception.js",
    "vendor/web/01_mimesniff.js",
    "vendor/web/01_urlpattern.js",
    "vendor/web/02_event.js",
    "vendor/web/02_structured_clone.js",
    "vendor/web/03_abort_signal.js",
    "vendor/web/06_streams.js",
    "vendor/web/08_text_encoding.js",
    "vendor/web/09_file.js",
    "vendor/web/15_performance.js",
    "vendor/fetch/20_headers.js",
    "vendor/fetch/21_formdata.js",
    "vendor/fetch/22_body.js",
    "vendor/fetch/23_request.js",
    "vendor/fetch/23_response.js",
    "vendor/crypto/00_crypto.js",
}

pub(crate) fn ops() -> Vec<OpDecl> {
    vec![
        OpDecl::builtin_js("op_load_ext_script"),
        op_sync!(op_url_parse),
        op_sync!(op_url_parse_with_base),
        op_sync!(op_url_reparse),
        op_sync!(op_url_get_serialization),
        op_sync!(op_url_parse_search_params),
        op_sync!(op_url_stringify_search_params),
        op_sync!(op_urlpattern_parse),
        op_sync!(op_urlpattern_process_match_input),
        op_sync!(op_encoding_normalize_label),
        op_sync!(op_encoding_decode_utf8),
        op_sync!(op_encoding_decode_utf8_ascii_only),
        op_sync!(op_encoding_decode_single),
        op_sync!(op_encoding_new_decoder),
        op_sync!(op_encoding_decode),
        op_sync!(op_encoding_drop_decoder),
        OpDecl::builtin_js("op_encoding_encode_into"),
        op_sync!(op_base64_decode),
        op_sync!(op_base64_encode_from_buffer),
        op_sync!(op_blob_create_part),
        op_sync!(op_blob_slice_part),
        op_async!(op_blob_read_part),
        op_sync!(op_blob_remove_part),
        op_sync!(op_blob_clone_part),
        op_sync!(op_blob_create_object_url),
        op_sync!(op_now),
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
