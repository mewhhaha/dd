//! ES modules: AST-based parsing (`boa_parser` / `boa_ast`), dependency graph
//! loading through [`ModuleLoader`], import attributes (`json`, `text`, `bytes`),
//! `import.meta`, live getter-backed exports, and dynamic `import()`.

use crate::error::Error;
use crate::inspector::InspectorCore;
use crate::instrument;
use crate::serde_v8::WireValue;
use boa_ast::declaration::{ExportDeclaration, ImportKind, ModuleRequest, ReExportKind};
use boa_ast::operations::bound_names;
use boa_ast::scope::Scope;
use boa_ast::{ModuleItem, ModuleItemSourceSyntax};
use boa_interner::Interner;
use boa_parser::{Parser, Source};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub type ModuleId = usize;

/// What a module evaluates to, chosen by the import's `type` attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModuleType {
    JavaScript,
    /// `with { type: "json" }`: the parsed JSON as the default export.
    Json,
    /// `with { type: "text" }`: the source as a string default export.
    Text,
    /// `with { type: "bytes" }`: the source as a `Uint8Array` default export.
    Bytes,
}

impl ModuleType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::JavaScript => "javascript",
            Self::Json => "json",
            Self::Text => "text",
            Self::Bytes => "bytes",
        }
    }

    pub(crate) fn from_attr(attr: &str) -> Result<Self, String> {
        match attr {
            "" | "javascript" => Ok(Self::JavaScript),
            "json" => Ok(Self::Json),
            "text" => Ok(Self::Text),
            "bytes" => Ok(Self::Bytes),
            other => Err(format!("unsupported module type \"{other}\"")),
        }
    }

    pub(crate) fn kind_code(self) -> i32 {
        match self {
            Self::JavaScript => 0,
            Self::Json => 1,
            Self::Text => 2,
            Self::Bytes => 3,
        }
    }
}

impl fmt::Display for ModuleType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub enum ModuleCode {
    String(String),
    Bytes(Arc<[u8]>),
}

pub struct ModuleSource {
    pub module_type: ModuleType,
    pub code: ModuleCode,
}

impl ModuleSource {
    pub fn new(module_type: ModuleType, code: ModuleCode) -> Self {
        Self { module_type, code }
    }
}

pub trait ModuleLoader {
    /// Resolves `specifier`, imported by the module named `referrer`, to the
    /// name of the module it refers to.
    fn resolve(&self, specifier: &str, referrer: &str) -> Result<String, String>;

    /// Loads the module named `name` for an import that requested
    /// `requested_type`.
    fn load(&self, name: &str, requested_type: ModuleType) -> Result<ModuleSource, String>;
}

/// A loader for runtimes that load no modules.
pub struct NoModules;

impl ModuleLoader for NoModules {
    fn resolve(&self, specifier: &str, _referrer: &str) -> Result<String, String> {
        Err(format!("module loading is not supported: {specifier}"))
    }

    fn load(&self, name: &str, _requested_type: ModuleType) -> Result<ModuleSource, String> {
        Err(format!("module loading is not supported: {name}"))
    }
}

#[derive(Clone)]
pub(crate) enum CompiledPayload {
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Clone)]
pub(crate) struct ModuleRecord {
    pub(crate) id: ModuleId,
    pub(crate) name: String,
    pub(crate) module_type: ModuleType,
    pub(crate) is_main: bool,
    pub(crate) payload: CompiledPayload,
    pub(crate) dep_ids: Vec<ModuleId>,
    pub(crate) registered_in_js: bool,
}

impl ModuleRecord {
    pub(crate) fn deps_json(&self) -> String {
        let mut s = String::from("[");
        for (i, id) in self.dep_ids.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            s.push_str(&id.to_string());
        }
        s.push(']');
        s
    }

    pub(crate) fn to_wire(&self) -> WireValue {
        let payload = match &self.payload {
            CompiledPayload::String(s) => WireValue::String(s.clone()),
            CompiledPayload::Bytes(b) => WireValue::Bytes(b.clone()),
        };
        WireValue::Object(vec![
            ("id".to_string(), WireValue::I32(self.id as i32)),
            ("name".to_string(), WireValue::String(self.name.clone())),
            (
                "kind".to_string(),
                WireValue::I32(self.module_type.kind_code()),
            ),
            ("isMain".to_string(), WireValue::Bool(self.is_main)),
            ("payload".to_string(), payload),
            ("depsJson".to_string(), WireValue::String(self.deps_json())),
        ])
    }
}

#[derive(Default)]
pub(crate) struct ModuleMap {
    pub(crate) modules: Vec<ModuleRecord>,
    pub(crate) ids: HashMap<(String, ModuleType), ModuleId>,
    pub(crate) main: Option<ModuleId>,
}

impl ModuleMap {
    pub(crate) fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }

    pub(crate) fn clear(&mut self) {
        *self = Self::default();
    }
}

struct ParsedJsModule {
    requests: Vec<(String, ModuleType)>,
    transformed_body: String,
}

fn code_utf8<'a>(name: &str, code: &'a ModuleCode) -> Result<&'a str, Error> {
    let bytes = match code {
        ModuleCode::String(s) => s.as_bytes(),
        ModuleCode::Bytes(b) => b,
    };
    std::str::from_utf8(bytes).map_err(|_| Error::new(format!("module is not valid UTF-8: {name}")))
}

fn extract_request_type(
    req: &ModuleRequest,
    interner: &Interner,
    referrer: &str,
) -> Result<ModuleType, Error> {
    for attr in req.attributes() {
        let key = interner.resolve_expect(attr.key()).to_string();
        if key == "type" {
            let val = interner.resolve_expect(attr.value()).to_string();
            return ModuleType::from_attr(&val)
                .map_err(|e| Error::new(format!("{e} imported by {referrer}")));
        }
    }
    Ok(ModuleType::JavaScript)
}

fn js_str_lit(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Parses an ES module with `boa_parser` and transforms it into a strict async
/// module factory body accepting `(__dd_imp, __dd_exp, __dd_exp_star, __dd_meta, __dd_dyn_import)`.
fn transform_js_module(
    name: &str,
    source: &str,
    inspector: Option<&InspectorCore>,
) -> Result<ParsedJsModule, Error> {
    let mut interner = Interner::new();
    let scope = Scope::new_global();
    let mut parser = Parser::new(Source::from_bytes(source));
    let module = parser
        .parse_module(&scope, &mut interner)
        .map_err(|err| Error::new(format!("Uncaught SyntaxError: {err}")))?;

    let utf16_units: Vec<u16> = source.encode_utf16().collect();
    // Map UTF-16 code unit index -> UTF-8 byte offset in `source`.
    let mut u16_to_byte = Vec::with_capacity(utf16_units.len() + 1);
    for (byte_idx, ch) in source.char_indices() {
        let n = ch.len_utf16();
        for _ in 0..n {
            u16_to_byte.push(byte_idx);
        }
    }
    u16_to_byte.push(source.len());

    let to_byte = |pos: usize| -> usize {
        u16_to_byte
            .get(pos)
            .copied()
            .unwrap_or(source.len())
            .min(source.len())
    };

    let mut requests: Vec<(String, ModuleType)> = Vec::new();
    let mut req_indices: HashMap<(String, ModuleType), usize> = HashMap::new();
    let mut add_request = |req: &ModuleRequest, interner: &Interner| -> Result<usize, Error> {
        let spec = interner.resolve_expect(req.specifier().sym()).to_string();
        let req_ty = extract_request_type(req, interner, name)?;
        let key = (spec.clone(), req_ty);
        if let Some(&idx) = req_indices.get(&key) {
            return Ok(idx);
        }
        let idx = requests.len();
        requests.push((spec, req_ty));
        req_indices.insert(key, idx);
        Ok(idx)
    };

    // Edits to apply to `source` as `(byte_start, byte_end, replacement)`.
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut import_prelude = String::new();
    let mut hoisted_exports = String::new();
    let mut epilogue_exports = String::new();

    let items = module.items().items();
    let syntaxes = module.items().source_syntax();
    debug_assert_eq!(items.len(), syntaxes.len());

    let erase_span = |start: usize, end: usize| -> String {
        let slice = &source[start..end];
        let mut out = String::with_capacity(slice.len());
        for ch in slice.chars() {
            if ch == '\n' || ch == '\r' {
                out.push(ch);
            } else {
                out.push(' ');
            }
        }
        out
    };

    for (item, syntax) in items.iter().zip(syntaxes.iter()) {
        match item {
            ModuleItem::ImportDeclaration(import_decl) => {
                let dep_idx = add_request(import_decl.request(), &interner)?;
                if let Some(def) = import_decl.default() {
                    let local = interner.resolve_expect(def.sym()).to_string();
                    import_prelude
                        .push_str(&format!("const {local} = __dd_imp({dep_idx}).default;\n"));
                }
                match import_decl.kind() {
                    ImportKind::DefaultOrUnnamed => {}
                    ImportKind::Namespaced { binding } => {
                        let local = interner.resolve_expect(binding.sym()).to_string();
                        import_prelude.push_str(&format!("const {local} = __dd_imp({dep_idx});\n"));
                    }
                    ImportKind::Named { names } => {
                        for spec in names.iter() {
                            let local = interner.resolve_expect(spec.binding().sym()).to_string();
                            let export_name =
                                interner.resolve_expect(spec.export_name()).to_string();
                            import_prelude.push_str(&format!(
                                "const {local} = __dd_imp({dep_idx})[{}];\n",
                                js_str_lit(&export_name)
                            ));
                        }
                    }
                }
                if let ModuleItemSourceSyntax::Declaration(span) = syntax {
                    let s = to_byte(span.start().pos());
                    let e = to_byte(span.end().pos());
                    edits.push((s, e, erase_span(s, e)));
                }
            }
            ModuleItem::ExportDeclaration(export_decl) => match export_decl.as_ref() {
                ExportDeclaration::ReExport { kind, request } => {
                    let dep_idx = add_request(request.module_request(), &interner)?;
                    match kind {
                        ReExportKind::Namespaced { name: None } => {
                            hoisted_exports.push_str(&format!("__dd_exp_star({dep_idx});\n"));
                        }
                        ReExportKind::Namespaced { name: Some(alias) } => {
                            let alias_s = interner.resolve_expect(*alias).to_string();
                            hoisted_exports.push_str(&format!(
                                "__dd_exp({}, () => __dd_imp({dep_idx}));\n",
                                js_str_lit(&alias_s)
                            ));
                        }
                        ReExportKind::Named { names } => {
                            for spec in names.iter() {
                                let exported = interner.resolve_expect(spec.alias()).to_string();
                                let imported =
                                    interner.resolve_expect(spec.private_name()).to_string();
                                hoisted_exports.push_str(&format!(
                                    "__dd_exp({}, () => __dd_imp({dep_idx})[{}]);\n",
                                    js_str_lit(&exported),
                                    js_str_lit(&imported)
                                ));
                            }
                        }
                    }
                    if let ModuleItemSourceSyntax::Declaration(span) = syntax {
                        let s = to_byte(span.start().pos());
                        let e = to_byte(span.end().pos());
                        edits.push((s, e, erase_span(s, e)));
                    }
                }
                ExportDeclaration::List(specs) => {
                    for spec in specs.iter() {
                        let exported = interner.resolve_expect(spec.alias()).to_string();
                        let local = interner.resolve_expect(spec.private_name()).to_string();
                        epilogue_exports.push_str(&format!(
                            "__dd_exp({}, () => {local});\n",
                            js_str_lit(&exported)
                        ));
                    }
                    if let ModuleItemSourceSyntax::Declaration(span) = syntax {
                        let s = to_byte(span.start().pos());
                        let e = to_byte(span.end().pos());
                        edits.push((s, e, erase_span(s, e)));
                    }
                }
                ExportDeclaration::VarStatement(var_decl) => {
                    for sym in bound_names(var_decl) {
                        let id = interner.resolve_expect(sym).to_string();
                        epilogue_exports
                            .push_str(&format!("__dd_exp({}, () => {id});\n", js_str_lit(&id)));
                    }
                    if let ModuleItemSourceSyntax::ExportKeyword(span) = syntax {
                        let s = to_byte(span.start().pos());
                        let e = to_byte(span.end().pos());
                        edits.push((s, e, erase_span(s, e)));
                    }
                }
                ExportDeclaration::Declaration(decl) => {
                    for sym in bound_names(decl) {
                        let id = interner.resolve_expect(sym).to_string();
                        epilogue_exports
                            .push_str(&format!("__dd_exp({}, () => {id});\n", js_str_lit(&id)));
                    }
                    if let ModuleItemSourceSyntax::ExportKeyword(span) = syntax {
                        let s = to_byte(span.start().pos());
                        let e = to_byte(span.end().pos());
                        edits.push((s, e, erase_span(s, e)));
                    }
                }
                ExportDeclaration::DefaultFunctionDeclaration(f) => {
                    let fname = interner.resolve_expect(f.name().sym()).to_string();
                    if let ModuleItemSourceSyntax::DefaultExport { keywords, .. } = syntax {
                        let s = to_byte(keywords.start().pos());
                        let e = to_byte(keywords.end().pos());
                        if fname == "default" {
                            edits.push((s, e, "const __dd_default = ".to_string()));
                            epilogue_exports
                                .push_str("__dd_exp(\"default\", () => __dd_default);\n");
                        } else {
                            edits.push((s, e, erase_span(s, e)));
                            hoisted_exports
                                .push_str(&format!("__dd_exp(\"default\", () => {fname});\n"));
                        }
                    }
                }
                ExportDeclaration::DefaultAsyncFunctionDeclaration(f) => {
                    let fname = interner.resolve_expect(f.name().sym()).to_string();
                    if let ModuleItemSourceSyntax::DefaultExport { keywords, .. } = syntax {
                        let s = to_byte(keywords.start().pos());
                        let e = to_byte(keywords.end().pos());
                        if fname == "default" {
                            edits.push((s, e, "const __dd_default = ".to_string()));
                            epilogue_exports
                                .push_str("__dd_exp(\"default\", () => __dd_default);\n");
                        } else {
                            edits.push((s, e, erase_span(s, e)));
                            hoisted_exports
                                .push_str(&format!("__dd_exp(\"default\", () => {fname});\n"));
                        }
                    }
                }
                ExportDeclaration::DefaultGeneratorDeclaration(f) => {
                    let fname = interner.resolve_expect(f.name().sym()).to_string();
                    if let ModuleItemSourceSyntax::DefaultExport { keywords, .. } = syntax {
                        let s = to_byte(keywords.start().pos());
                        let e = to_byte(keywords.end().pos());
                        if fname == "default" {
                            edits.push((s, e, "const __dd_default = ".to_string()));
                            epilogue_exports
                                .push_str("__dd_exp(\"default\", () => __dd_default);\n");
                        } else {
                            edits.push((s, e, erase_span(s, e)));
                            hoisted_exports
                                .push_str(&format!("__dd_exp(\"default\", () => {fname});\n"));
                        }
                    }
                }
                ExportDeclaration::DefaultAsyncGeneratorDeclaration(f) => {
                    let fname = interner.resolve_expect(f.name().sym()).to_string();
                    if let ModuleItemSourceSyntax::DefaultExport { keywords, .. } = syntax {
                        let s = to_byte(keywords.start().pos());
                        let e = to_byte(keywords.end().pos());
                        if fname == "default" {
                            edits.push((s, e, "const __dd_default = ".to_string()));
                            epilogue_exports
                                .push_str("__dd_exp(\"default\", () => __dd_default);\n");
                        } else {
                            edits.push((s, e, erase_span(s, e)));
                            hoisted_exports
                                .push_str(&format!("__dd_exp(\"default\", () => {fname});\n"));
                        }
                    }
                }
                ExportDeclaration::DefaultClassDeclaration(c) => {
                    let cname = interner.resolve_expect(c.name().sym()).to_string();
                    if let ModuleItemSourceSyntax::DefaultExport { keywords, .. } = syntax {
                        let s = to_byte(keywords.start().pos());
                        let e = to_byte(keywords.end().pos());
                        if cname == "default" {
                            edits.push((s, e, "const __dd_default = ".to_string()));
                            epilogue_exports
                                .push_str("__dd_exp(\"default\", () => __dd_default);\n");
                        } else {
                            edits.push((s, e, erase_span(s, e)));
                            epilogue_exports
                                .push_str(&format!("__dd_exp(\"default\", () => {cname});\n"));
                        }
                    }
                }
                ExportDeclaration::DefaultAssignmentExpression(_) => {
                    if let ModuleItemSourceSyntax::DefaultExport { keywords, .. } = syntax {
                        let s = to_byte(keywords.start().pos());
                        let e = to_byte(keywords.end().pos());
                        edits.push((s, e, "const __dd_default = ".to_string()));
                        epilogue_exports.push_str("__dd_exp(\"default\", () => __dd_default);\n");
                    }
                }
            },
            ModuleItem::StatementListItem(_) => {}
        }
    }

    edits.sort_by_key(|(s, _, _)| *s);
    let mut rewritten = String::with_capacity(source.len() + 256);
    let mut cursor = 0usize;
    for (s, e, rep) in edits {
        if s >= cursor && e <= source.len() {
            rewritten.push_str(&source[cursor..s]);
            rewritten.push_str(&rep);
            cursor = e;
        }
    }
    rewritten.push_str(&source[cursor..]);

    // Rewrite `import.meta` and dynamic `import(` outside string/comment literals.
    let rewritten = rewrite_import_meta_and_dynamic_import(&rewritten);
    // When DevTools inspector is enabled, register the module script and instrument
    // `<rewritten>` BEFORE prepending import/hoisted lines so every statement gets
    // its exact original 0-based line number!
    let rewritten = if let Some(insp) = inspector {
        let script_id = insp.register_script(name, source);
        instrument::instrument_js(script_id, &rewritten)
    } else {
        rewritten
    };

    let mut body = String::with_capacity(
        rewritten.len()
            + import_prelude.len()
            + hoisted_exports.len()
            + epilogue_exports.len()
            + 64,
    );
    body.push_str("\"use strict\";\n");
    body.push_str(&import_prelude);
    body.push_str(&hoisted_exports);
    body.push_str(&rewritten);
    body.push('\n');
    body.push_str(&epilogue_exports);

    Ok(ParsedJsModule {
        requests,
        transformed_body: body,
    })
}

/// Rewrites `import.meta` -> `__dd_meta` and `import(` -> `__dd_dyn_import(`
/// while skipping string literals, template text, and JS comments.
fn rewrite_import_meta_and_dynamic_import(src: &str) -> String {
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len + 32);
    let mut i = 0usize;
    let mut prev_ident_char = false;

    let is_id_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$';

    while i < len {
        let b = bytes[i];
        // Line comment
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'/' {
            let start = i;
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            out.push_str(&src[start..i]);
            prev_ident_char = false;
            continue;
        }
        // Block comment
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            let start = i;
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < len {
                i += 2;
            }
            out.push_str(&src[start..i]);
            prev_ident_char = false;
            continue;
        }
        // String literals (' or ")
        if b == b'\'' || b == b'"' {
            let quote = b;
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(len);
                } else if bytes[i] == quote {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
            out.push_str(&src[start..i]);
            prev_ident_char = false;
            continue;
        }
        // Template literal (simple scan; skips static parts, scans ${...} naturally? Keep intact unless ${)
        if b == b'`' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(len);
                } else if bytes[i] == b'`' {
                    i += 1;
                    break;
                } else if bytes[i] == b'$' && i + 1 < len && bytes[i + 1] == b'{' {
                    // Flush static chunk up to `${` and scan expression normally!
                    i += 2;
                    out.push_str(&src[start..i]);
                    let mut depth = 1usize;
                    let expr_start = i;
                    while i < len && depth > 0 {
                        if bytes[i] == b'{' {
                            depth += 1;
                        } else if bytes[i] == b'}' {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        i += 1;
                    }
                    let rewritten_expr =
                        rewrite_import_meta_and_dynamic_import(&src[expr_start..i]);
                    out.push_str(&rewritten_expr);
                    if i < len {
                        out.push('}');
                        i += 1;
                    }
                    // Continue template after `}`
                    let cont_start = i;
                    while i < len {
                        if bytes[i] == b'\\' {
                            i = (i + 2).min(len);
                        } else if bytes[i] == b'`' {
                            i += 1;
                            break;
                        } else {
                            i += 1;
                        }
                    }
                    out.push_str(&src[cont_start..i]);
                    // Jump out of template loop
                    break;
                } else {
                    i += 1;
                }
            }
            if !out.ends_with('`') && i > start && i <= len && bytes[i - 1] == b'`' {
                out.push_str(&src[start..i]);
            }
            prev_ident_char = false;
            continue;
        }

        // Check for `import` keyword (not preceded by `.` or identifier char).
        if !prev_ident_char
            && bytes[i..].starts_with(b"import")
            && (i + 6 == len || !is_id_byte(bytes[i + 6]))
        {
            // Check not preceded by `.` (ignoring whitespace).
            let trimmed_prev = out.trim_end();
            let after_dot = trimmed_prev.ends_with('.');
            if !after_dot {
                let mut j = i + 6;
                while j < len && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                // `import.meta`
                if j < len && bytes[j] == b'.' {
                    let mut k = j + 1;
                    while k < len && bytes[k].is_ascii_whitespace() {
                        k += 1;
                    }
                    if bytes[k..].starts_with(b"meta")
                        && (k + 4 == len || !is_id_byte(bytes[k + 4]))
                    {
                        out.push_str("__dd_meta");
                        i = k + 4;
                        prev_ident_char = true;
                        continue;
                    }
                }
                // Dynamic `import(`
                if j < len && bytes[j] == b'(' {
                    out.push_str("__dd_dyn_import");
                    i += 6;
                    prev_ident_char = true;
                    continue;
                }
            }
        }

        let ch = src[i..].chars().next().unwrap();
        out.push(ch);
        prev_ident_char = ch.is_ascii_alphanumeric() || ch == '_' || ch == '$';
        i += ch.len_utf8();
    }
    out
}

/// Rewrites dynamic `import(` calls inside a classic script to `__dd_dyn_import(`.
/// Returns `None` when the script contains no dynamic `import(` calls.
pub(crate) fn rewrite_script_dynamic_import(src: &str) -> Option<String> {
    if !src.contains("import") {
        return None;
    }
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len + 16);
    let mut i = 0;
    let mut prev_ident_char = false;
    let mut replaced = false;

    let is_id_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$';

    while i < len {
        let b = bytes[i];
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'/' {
            let start = i;
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            out.push_str(&src[start..i]);
            continue;
        }
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            let start = i;
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(len);
            out.push_str(&src[start..i]);
            continue;
        }
        if b == b'\'' || b == b'"' {
            let quote = b;
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(len);
                } else if bytes[i] == quote {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
            out.push_str(&src[start..i]);
            prev_ident_char = false;
            continue;
        }
        if !prev_ident_char
            && bytes[i..].starts_with(b"import")
            && (i + 6 == len || !is_id_byte(bytes[i + 6]))
        {
            let trimmed_prev = out.trim_end();
            if !trimmed_prev.ends_with('.') {
                let mut j = i + 6;
                while j < len && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j < len && bytes[j] == b'(' {
                    out.push_str("__dd_dyn_import");
                    i += 6;
                    prev_ident_char = true;
                    replaced = true;
                    continue;
                }
            }
        }
        let ch = src[i..].chars().next().unwrap();
        out.push(ch);
        prev_ident_char = ch.is_ascii_alphanumeric() || ch == '_' || ch == '$';
        i += ch.len_utf8();
    }
    if replaced { Some(out) } else { None }
}

/// Loads `name` and every static dependency recursively into `modules`,
/// returning `(root_id, newly_added_module_ids)`.
pub(crate) fn load_graph(
    modules: &mut ModuleMap,
    loader: &dyn ModuleLoader,
    name: &str,
    module_type: ModuleType,
    code: Option<ModuleCode>,
    main: bool,
    inspector: Option<&InspectorCore>,
) -> Result<(ModuleId, Vec<ModuleId>), Error> {
    let mut newly_added = Vec::new();
    let root = load_recursive(
        modules,
        loader,
        name,
        module_type,
        code,
        main,
        inspector,
        &mut newly_added,
    )?;
    Ok((root, newly_added))
}

#[allow(clippy::too_many_arguments)]
fn load_recursive(
    modules: &mut ModuleMap,
    loader: &dyn ModuleLoader,
    name: &str,
    module_type: ModuleType,
    code: Option<ModuleCode>,
    main: bool,
    inspector: Option<&InspectorCore>,
    newly_added: &mut Vec<ModuleId>,
) -> Result<ModuleId, Error> {
    if let Some(&id) = modules.ids.get(&(name.to_string(), module_type)) {
        if main {
            if modules.main.is_some_and(|existing| existing != id) {
                return Err(Error::new(format!(
                    "a main module is already loaded; cannot load {name} as main"
                )));
            }
            modules.main = Some(id);
            modules.modules[id].is_main = true;
        }
        return Ok(id);
    }

    let code = match code {
        Some(c) => c,
        None => {
            let source = loader.load(name, module_type).map_err(Error::new)?;
            if source.module_type != module_type {
                return Err(Error::new(format!(
                    "requested module type {module_type} does not match {} module: {name}",
                    source.module_type
                )));
            }
            source.code
        }
    };

    let (payload, requests) = match module_type {
        ModuleType::JavaScript => {
            let text = code_utf8(name, &code)?;
            let parsed = transform_js_module(name, text, inspector)?;
            (
                CompiledPayload::String(parsed.transformed_body),
                parsed.requests,
            )
        }
        ModuleType::Json => {
            let text = code_utf8(name, &code)?;
            (CompiledPayload::String(text.to_string()), Vec::new())
        }
        ModuleType::Text => {
            let text = code_utf8(name, &code)?;
            (CompiledPayload::String(text.to_string()), Vec::new())
        }
        ModuleType::Bytes => {
            let bytes = match code {
                ModuleCode::String(s) => s.into_bytes(),
                ModuleCode::Bytes(b) => b.to_vec(),
            };
            (CompiledPayload::Bytes(bytes), Vec::new())
        }
    };

    let id = modules.modules.len();
    if main {
        if modules.main.is_some_and(|existing| existing != id) {
            return Err(Error::new(format!(
                "a main module is already loaded; cannot load {name} as main"
            )));
        }
        modules.main = Some(id);
    }
    modules.modules.push(ModuleRecord {
        id,
        name: name.to_string(),
        module_type,
        is_main: main,
        payload,
        dep_ids: Vec::new(),
        registered_in_js: false,
    });
    modules.ids.insert((name.to_string(), module_type), id);
    newly_added.push(id);

    let mut dep_ids = Vec::with_capacity(requests.len());
    for (specifier, req_type) in requests {
        let resolved = loader.resolve(&specifier, name).map_err(Error::new)?;
        let child_id = load_recursive(
            modules,
            loader,
            &resolved,
            req_type,
            None,
            false,
            inspector,
            newly_added,
        )?;
        dep_ids.push(child_id);
    }
    modules.modules[id].dep_ids = dep_ids;
    Ok(id)
}
