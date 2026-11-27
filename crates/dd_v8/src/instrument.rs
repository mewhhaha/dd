//! Lightweight JS token-based debugger instrumenter for `dd_v8` inspector mode.
//!
//! Only invoked when `JsRuntime::enable_inspector` is active (zero overhead in
//! normal worker execution). Preserves exact 0-based line numbers by never
//! inserting newlines into instrumented statements.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TokKind {
    Ident,
    Number,
    String,
    TemplateFull,
    TemplateHead,
    TemplateMiddle,
    TemplateTail,
    Regex,
    LBrace,
    RBrace,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Semi,
    Comma,
    Colon,
    Dot,
    Question,
    Arrow,
    IncDec,
    Assign,
    Op,
}

#[derive(Clone, Copy, Debug)]
struct Token<'a> {
    kind: TokKind,
    text: &'a str,
    start: usize,
    end: usize,
    line: u32,
    col: u32,
    newline_before: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BraceKind {
    CodeBlock,
    ObjectOrPattern,
    ClassBody,
    SwitchBody,
    TemplateExpr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ParenKind {
    If,
    While(u32, u32),
    For(u32, u32),
    Switch,
    Catch,
    With,
    Normal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Container {
    TopLevel,
    Brace(BraceKind),
    Paren(ParenKind),
    Bracket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ControlHeader {
    IfOrElse,
    Loop(u32, u32),
}

struct LineTable {
    starts: Vec<usize>,
}

impl LineTable {
    fn new(src: &str) -> Self {
        let mut starts = vec![0usize];
        for (i, b) in src.bytes().enumerate() {
            if b == b'\n' {
                starts.push(i + 1);
            }
        }
        Self { starts }
    }

    fn line_col(&self, byte_pos: usize) -> (u32, u32) {
        let idx = match self.starts.binary_search(&byte_pos) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };
        let col = byte_pos.saturating_sub(self.starts[idx]);
        (idx as u32, col as u32)
    }
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b'$' || b >= 0x80
}

fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

fn prev_allows_regex(prev: Option<&Token<'_>>) -> bool {
    let Some(t) = prev else {
        return true;
    };
    match t.kind {
        TokKind::LBrace
        | TokKind::LParen
        | TokKind::LBracket
        | TokKind::Semi
        | TokKind::Comma
        | TokKind::Colon
        | TokKind::Question
        | TokKind::Arrow
        | TokKind::Assign
        | TokKind::Op => true,
        TokKind::Ident => matches!(
            t.text,
            "return"
                | "throw"
                | "case"
                | "in"
                | "of"
                | "instanceof"
                | "typeof"
                | "void"
                | "delete"
                | "new"
                | "yield"
                | "await"
                | "else"
                | "do"
        ),
        _ => false,
    }
}

fn tokenize<'a>(src: &'a str, lines: &LineTable) -> Vec<Token<'a>> {
    let bytes = src.as_bytes();
    let len = bytes.len();
    let mut out = Vec::with_capacity(len / 4 + 8);
    let mut i = 0usize;
    let mut newline_before = false;
    // Track brace depths of active template `${...}` interpolations so `}` resumes template lexing.
    let mut tmpl_brace_stack: Vec<usize> = Vec::new();
    let mut brace_depth: usize = 0;

    while i < len {
        let b = bytes[i];
        // Whitespace
        if b == b' ' || b == b'\t' || b == 0x0b || b == 0x0c {
            i += 1;
            continue;
        }
        if b == b'\n' || b == b'\r' {
            newline_before = true;
            i += 1;
            continue;
        }
        // Line comment
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'/' {
            i += 2;
            while i < len && bytes[i] != b'\n' && bytes[i] != b'\r' {
                i += 1;
            }
            continue;
        }
        // Block comment
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                if bytes[i] == b'\n' || bytes[i] == b'\r' {
                    newline_before = true;
                }
                i += 1;
            }
            if i + 1 < len {
                i += 2;
            }
            continue;
        }

        let start = i;
        let (line, col) = lines.line_col(start);
        let nl = newline_before;
        newline_before = false;

        // Closing `}` that resumes a template literal
        if b == b'}' && tmpl_brace_stack.last().copied() == Some(brace_depth) {
            tmpl_brace_stack.pop();
            brace_depth = brace_depth.saturating_sub(1);
            i += 1;
            let mut is_middle = false;
            while i < len {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(len);
                } else if bytes[i] == b'`' {
                    i += 1;
                    break;
                } else if bytes[i] == b'$' && i + 1 < len && bytes[i + 1] == b'{' {
                    i += 2;
                    is_middle = true;
                    brace_depth += 1;
                    tmpl_brace_stack.push(brace_depth);
                    break;
                } else {
                    i += 1;
                }
            }
            let kind = if is_middle {
                TokKind::TemplateMiddle
            } else {
                TokKind::TemplateTail
            };
            out.push(Token {
                kind,
                text: &src[start..i],
                start,
                end: i,
                line,
                col,
                newline_before: nl,
            });
            continue;
        }

        // String literal
        if b == b'\'' || b == b'"' {
            let quote = b;
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
            out.push(Token {
                kind: TokKind::String,
                text: &src[start..i],
                start,
                end: i,
                line,
                col,
                newline_before: nl,
            });
            continue;
        }

        // Template literal
        if b == b'`' {
            i += 1;
            let mut is_head = false;
            while i < len {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(len);
                } else if bytes[i] == b'`' {
                    i += 1;
                    break;
                } else if bytes[i] == b'$' && i + 1 < len && bytes[i + 1] == b'{' {
                    i += 2;
                    is_head = true;
                    brace_depth += 1;
                    tmpl_brace_stack.push(brace_depth);
                    break;
                } else {
                    i += 1;
                }
            }
            let kind = if is_head {
                TokKind::TemplateHead
            } else {
                TokKind::TemplateFull
            };
            out.push(Token {
                kind,
                text: &src[start..i],
                start,
                end: i,
                line,
                col,
                newline_before: nl,
            });
            continue;
        }

        // Regex literal
        if b == b'/' && prev_allows_regex(out.last()) {
            i += 1;
            let mut in_class = false;
            while i < len {
                let c = bytes[i];
                if c == b'\\' {
                    i = (i + 2).min(len);
                } else if c == b'[' {
                    in_class = true;
                    i += 1;
                } else if c == b']' {
                    in_class = false;
                    i += 1;
                } else if c == b'/' && !in_class {
                    i += 1;
                    while i < len && bytes[i].is_ascii_alphabetic() {
                        i += 1;
                    }
                    break;
                } else if c == b'\n' || c == b'\r' {
                    break;
                } else {
                    i += 1;
                }
            }
            out.push(Token {
                kind: TokKind::Regex,
                text: &src[start..i],
                start,
                end: i,
                line,
                col,
                newline_before: nl,
            });
            continue;
        }

        // Identifier / keyword
        if is_ident_start(b) {
            i += 1;
            while i < len && is_ident_cont(bytes[i]) {
                i += 1;
            }
            out.push(Token {
                kind: TokKind::Ident,
                text: &src[start..i],
                start,
                end: i,
                line,
                col,
                newline_before: nl,
            });
            continue;
        }

        // Number
        if b.is_ascii_digit() || (b == b'.' && i + 1 < len && bytes[i + 1].is_ascii_digit()) {
            i += 1;
            while i < len {
                let c = bytes[i];
                if (c.is_ascii_alphanumeric() || c == b'_' || c == b'.')
                    || ((c == b'+' || c == b'-')
                        && i > start
                        && (bytes[i - 1] == b'e' || bytes[i - 1] == b'E'))
                {
                    i += 1;
                } else {
                    break;
                }
            }
            out.push(Token {
                kind: TokKind::Number,
                text: &src[start..i],
                start,
                end: i,
                line,
                col,
                newline_before: nl,
            });
            continue;
        }

        // Multi-char & single-char punctuation
        let kind = match b {
            b'{' => {
                brace_depth += 1;
                i += 1;
                TokKind::LBrace
            }
            b'}' => {
                brace_depth = brace_depth.saturating_sub(1);
                i += 1;
                TokKind::RBrace
            }
            b'(' => {
                i += 1;
                TokKind::LParen
            }
            b')' => {
                i += 1;
                TokKind::RParen
            }
            b'[' => {
                i += 1;
                TokKind::LBracket
            }
            b']' => {
                i += 1;
                TokKind::RBracket
            }
            b';' => {
                i += 1;
                TokKind::Semi
            }
            b',' => {
                i += 1;
                TokKind::Comma
            }
            b':' => {
                i += 1;
                TokKind::Colon
            }
            b'.' => {
                if i + 2 < len && bytes[i + 1] == b'.' && bytes[i + 2] == b'.' {
                    i += 3;
                    TokKind::Op
                } else {
                    i += 1;
                    TokKind::Dot
                }
            }
            b'?' => {
                if i + 1 < len && bytes[i + 1] == b'.' {
                    i += 2;
                    TokKind::Dot
                } else if i + 2 < len && bytes[i + 1] == b'?' && bytes[i + 2] == b'=' {
                    i += 3;
                    TokKind::Assign
                } else if i + 1 < len && bytes[i + 1] == b'?' {
                    i += 2;
                    TokKind::Op
                } else {
                    i += 1;
                    TokKind::Question
                }
            }
            b'=' => {
                if i + 1 < len && bytes[i + 1] == b'>' {
                    i += 2;
                    TokKind::Arrow
                } else if i + 2 < len && bytes[i + 1] == b'=' && bytes[i + 2] == b'=' {
                    i += 3;
                    TokKind::Op
                } else if i + 1 < len && bytes[i + 1] == b'=' {
                    i += 2;
                    TokKind::Op
                } else {
                    i += 1;
                    TokKind::Assign
                }
            }
            b'+' | b'-' => {
                if i + 1 < len && bytes[i + 1] == b {
                    i += 2;
                    TokKind::IncDec
                } else if i + 1 < len && bytes[i + 1] == b'=' {
                    i += 2;
                    TokKind::Assign
                } else {
                    i += 1;
                    TokKind::Op
                }
            }
            b'*' | b'/' | b'%' | b'^' => {
                if b == b'*' && i + 2 < len && bytes[i + 1] == b'*' && bytes[i + 2] == b'=' {
                    i += 3;
                    TokKind::Assign
                } else if b == b'*' && i + 1 < len && bytes[i + 1] == b'*' {
                    i += 2;
                    TokKind::Op
                } else if i + 1 < len && bytes[i + 1] == b'=' {
                    i += 2;
                    TokKind::Assign
                } else {
                    i += 1;
                    TokKind::Op
                }
            }
            b'&' | b'|' => {
                if i + 2 < len && bytes[i + 1] == b && bytes[i + 2] == b'=' {
                    i += 3;
                    TokKind::Assign
                } else if i + 1 < len && bytes[i + 1] == b {
                    i += 2;
                    TokKind::Op
                } else if i + 1 < len && bytes[i + 1] == b'=' {
                    i += 2;
                    TokKind::Assign
                } else {
                    i += 1;
                    TokKind::Op
                }
            }
            b'<' | b'>' => {
                if b == b'>'
                    && i + 3 < len
                    && bytes[i + 1] == b'>'
                    && bytes[i + 2] == b'>'
                    && bytes[i + 3] == b'='
                {
                    i += 4;
                    TokKind::Assign
                } else if b == b'>' && i + 2 < len && bytes[i + 1] == b'>' && bytes[i + 2] == b'>' {
                    i += 3;
                    TokKind::Op
                } else if i + 2 < len && bytes[i + 1] == b && bytes[i + 2] == b'=' {
                    i += 3;
                    TokKind::Assign
                } else if i + 1 < len && (bytes[i + 1] == b || bytes[i + 1] == b'=') {
                    i += 2;
                    TokKind::Op
                } else {
                    i += 1;
                    TokKind::Op
                }
            }
            b'!' => {
                if i + 2 < len && bytes[i + 1] == b'=' && bytes[i + 2] == b'=' {
                    i += 3;
                    TokKind::Op
                } else if i + 1 < len && bytes[i + 1] == b'=' {
                    i += 2;
                    TokKind::Op
                } else {
                    i += 1;
                    TokKind::Op
                }
            }
            _ => {
                i += 1;
                TokKind::Op
            }
        };
        out.push(Token {
            kind,
            text: &src[start..i],
            start,
            end: i,
            line,
            col,
            newline_before: nl,
        });
    }
    out
}

fn can_end_stmt(t: &Token<'_>) -> bool {
    match t.kind {
        TokKind::Ident => !matches!(
            t.text,
            "return"
                | "throw"
                | "yield"
                | "await"
                | "new"
                | "typeof"
                | "void"
                | "delete"
                | "in"
                | "of"
                | "instanceof"
                | "const"
                | "let"
                | "var"
                | "function"
                | "class"
                | "async"
                | "if"
                | "else"
                | "while"
                | "for"
                | "do"
                | "switch"
                | "case"
                | "try"
                | "catch"
                | "finally"
                | "with"
                | "export"
                | "import"
        ),
        TokKind::Number
        | TokKind::String
        | TokKind::TemplateFull
        | TokKind::TemplateTail
        | TokKind::Regex
        | TokKind::RParen
        | TokKind::RBracket
        | TokKind::IncDec => true,
        _ => false,
    }
}

fn continues_expr(t: &Token<'_>) -> bool {
    match t.kind {
        TokKind::Dot
        | TokKind::Question
        | TokKind::Colon
        | TokKind::Comma
        | TokKind::Arrow
        | TokKind::Assign
        | TokKind::Op
        | TokKind::LParen
        | TokKind::LBracket
        | TokKind::TemplateFull
        | TokKind::TemplateHead => true,
        TokKind::Ident => matches!(t.text, "in" | "instanceof" | "of"),
        _ => false,
    }
}

/// Instruments a classic JS script or transformed module body for CDP debugging.
///
/// Inserts:
/// - `__dd_dbg_s(scriptId, line, col, (__e)=>eval(__e));` before executable statements
/// - `__dd_dbg_d(scriptId, line, col, (__e)=>eval(__e));` replacing `debugger;`
/// - `__dd_dbg_l(scriptId, line, col, (__e)=>eval(__e));` at loop body entry
///
/// Never inserts newlines so every statement keeps its original 0-based line number.
pub(crate) fn instrument_js(script_id: u64, source: &str) -> String {
    let lines = LineTable::new(source);
    let toks = tokenize(source, &lines);
    if toks.is_empty() {
        return source.to_string();
    }

    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut stack: Vec<Container> = vec![Container::TopLevel];
    let mut at_stmt_start = true;
    let mut after_control: Option<ControlHeader> = None;
    let mut prev_closed_block = false;
    let mut in_class_header = false;
    let mut in_switch_label = false;

    let mut idx = 0usize;
    while idx < toks.len() {
        let tok = toks[idx];
        let prev = if idx > 0 { Some(&toks[idx - 1]) } else { None };
        let next = toks.get(idx + 1);
        let top = *stack.last().unwrap_or(&Container::TopLevel);
        let in_stmt_container = matches!(
            top,
            Container::TopLevel
                | Container::Brace(BraceKind::CodeBlock)
                | Container::Brace(BraceKind::SwitchBody)
        );

        // Detect ASI statement boundaries across newlines.
        if in_stmt_container
            && !at_stmt_start
            && tok.newline_before
            && !in_switch_label
            && !matches!(tok.kind, TokKind::RBrace | TokKind::Semi)
            && prev.is_some_and(can_end_stmt)
            && !continues_expr(&tok)
        {
            at_stmt_start = true;
        }
        // Also: `return\nstmt` / `break\nstmt` / `continue\nstmt`
        if in_stmt_container
            && !at_stmt_start
            && tok.newline_before
            && prev.is_some_and(|p| {
                p.kind == TokKind::Ident && matches!(p.text, "return" | "break" | "continue")
            })
        {
            at_stmt_start = true;
        }

        // Handle template interpolations
        if tok.kind == TokKind::TemplateHead || tok.kind == TokKind::TemplateMiddle {
            if tok.kind == TokKind::TemplateMiddle
                && matches!(
                    stack.last(),
                    Some(Container::Brace(BraceKind::TemplateExpr))
                )
            {
                stack.pop();
            }
            stack.push(Container::Brace(BraceKind::TemplateExpr));
            at_stmt_start = false;
            prev_closed_block = false;
            idx += 1;
            continue;
        }
        if tok.kind == TokKind::TemplateTail {
            if matches!(
                stack.last(),
                Some(Container::Brace(BraceKind::TemplateExpr))
            ) {
                stack.pop();
            }
            at_stmt_start = false;
            prev_closed_block = false;
            idx += 1;
            continue;
        }

        // Check `debugger` statement anywhere at statement start or after a single-stmt control header.
        if in_stmt_container
            && (at_stmt_start || after_control.is_some())
            && tok.kind == TokKind::Ident
            && tok.text == "debugger"
        {
            let (end_byte, advance) = if next.is_some_and(|n| n.kind == TokKind::Semi) {
                (next.unwrap().end, 2)
            } else {
                (tok.end, 1)
            };
            edits.push((
                tok.start,
                end_byte,
                format!(
                    "__dd_dbg_d({script_id},{},{},(__e)=>eval(__e));",
                    tok.line, tok.col
                ),
            ));
            at_stmt_start = true;
            after_control = None;
            prev_closed_block = false;
            idx += advance;
            continue;
        }

        // Statement-start instrumentation (only inside statement containers, not after bare control headers).
        if in_stmt_container && at_stmt_start && after_control.is_none() {
            let skip_hook = match tok.kind {
                TokKind::Semi | TokKind::RBrace | TokKind::LBrace => true,
                TokKind::String if (tok.text == "\"use strict\"" || tok.text == "'use strict'") => {
                    true
                }
                TokKind::Ident => {
                    matches!(
                        tok.text,
                        "else"
                            | "catch"
                            | "finally"
                            | "case"
                            | "default"
                            | "function"
                            | "import"
                            | "export"
                    ) || (tok.text == "async"
                        && next.is_some_and(|n| n.kind == TokKind::Ident && n.text == "function"))
                        || next.is_some_and(|n| n.kind == TokKind::Colon)
                }
                _ => false,
            };
            if !skip_hook {
                edits.push((
                    tok.start,
                    tok.start,
                    format!(
                        "__dd_dbg_s({script_id},{},{},(__e)=>eval(__e));",
                        tok.line, tok.col
                    ),
                ));
            }
        }

        // Now advance parser state based on current token.
        let mut clear_after_control = false;
        match tok.kind {
            TokKind::LParen => {
                let pkind = match prev {
                    Some(p) if p.kind == TokKind::Ident => match p.text {
                        "if" => ParenKind::If,
                        "while" => ParenKind::While(p.line, p.col),
                        "for" => ParenKind::For(p.line, p.col),
                        "switch" => ParenKind::Switch,
                        "catch" => ParenKind::Catch,
                        "with" => ParenKind::With,
                        _ => ParenKind::Normal,
                    },
                    _ => ParenKind::Normal,
                };
                stack.push(Container::Paren(pkind));
                at_stmt_start = false;
                clear_after_control = true;
            }
            TokKind::RParen => {
                if let Some(Container::Paren(pkind)) = stack.pop() {
                    match pkind {
                        ParenKind::If | ParenKind::With => {
                            after_control = Some(ControlHeader::IfOrElse);
                            at_stmt_start = false;
                        }
                        ParenKind::While(l, c) | ParenKind::For(l, c) => {
                            after_control = Some(ControlHeader::Loop(l, c));
                            at_stmt_start = false;
                        }
                        ParenKind::Switch | ParenKind::Catch | ParenKind::Normal => {
                            at_stmt_start = false;
                        }
                    }
                }
            }
            TokKind::LBracket => {
                stack.push(Container::Bracket);
                at_stmt_start = false;
                clear_after_control = true;
            }
            TokKind::RBracket => {
                if matches!(stack.last(), Some(Container::Bracket)) {
                    stack.pop();
                }
                at_stmt_start = false;
            }
            TokKind::LBrace => {
                let bkind = if in_class_header {
                    in_class_header = false;
                    BraceKind::ClassBody
                } else if matches!(
                    after_control,
                    Some(ControlHeader::Loop(_, _) | ControlHeader::IfOrElse)
                ) || prev.is_some_and(|p| {
                    p.kind == TokKind::Arrow
                        || (p.kind == TokKind::Ident
                            && matches!(
                                p.text,
                                "try" | "finally" | "else" | "do" | "catch" | "static"
                            ))
                }) {
                    BraceKind::CodeBlock
                } else if prev.is_some_and(|p| p.kind == TokKind::RParen) {
                    // `function f() {`, `if (...) {`, `async fetch() {` inside object/class, `switch (...) {`
                    // Look back to check if the matching `(` belonged to `switch`.
                    let is_switch = is_prev_switch_paren(&toks, idx - 1);
                    if is_switch {
                        BraceKind::SwitchBody
                    } else {
                        BraceKind::CodeBlock
                    }
                } else if in_stmt_container && at_stmt_start {
                    BraceKind::CodeBlock
                } else {
                    BraceKind::ObjectOrPattern
                };

                if let Some(ControlHeader::Loop(ll, lc)) = after_control {
                    edits.push((
                        tok.end,
                        tok.end,
                        format!("__dd_dbg_l({script_id},{ll},{lc},(__e)=>eval(__e));"),
                    ));
                }
                after_control = None;
                stack.push(Container::Brace(bkind));
                at_stmt_start = matches!(bkind, BraceKind::CodeBlock | BraceKind::SwitchBody);
            }
            TokKind::RBrace => {
                let popped = stack.pop();
                let new_top = *stack.last().unwrap_or(&Container::TopLevel);
                let now_in_stmt = matches!(
                    new_top,
                    Container::TopLevel
                        | Container::Brace(BraceKind::CodeBlock)
                        | Container::Brace(BraceKind::SwitchBody)
                );
                let closed_code = matches!(
                    popped,
                    Some(Container::Brace(
                        BraceKind::CodeBlock | BraceKind::SwitchBody | BraceKind::ClassBody
                    ))
                );
                prev_closed_block = closed_code;
                at_stmt_start = now_in_stmt && closed_code;
                clear_after_control = true;
            }
            TokKind::Semi => {
                if in_stmt_container {
                    at_stmt_start = true;
                    in_switch_label = false;
                }
                clear_after_control = true;
            }
            TokKind::Colon => {
                if in_switch_label {
                    in_switch_label = false;
                    at_stmt_start = true;
                } else if in_stmt_container
                    && prev.is_some_and(|p| p.kind == TokKind::Ident)
                    && idx >= 2
                    && matches!(
                        toks[idx - 2].kind,
                        TokKind::Semi | TokKind::LBrace | TokKind::RBrace
                    )
                {
                    // Labeled statement: next token starts statement; skip extra hook after label
                    at_stmt_start = false;
                } else {
                    at_stmt_start = false;
                }
            }
            TokKind::Ident => {
                if tok.text == "class" {
                    in_class_header = true;
                    at_stmt_start = false;
                    clear_after_control = true;
                } else if tok.text == "else" && in_stmt_container {
                    after_control = Some(ControlHeader::IfOrElse);
                    at_stmt_start = false;
                } else if tok.text == "do" && in_stmt_container {
                    after_control = Some(ControlHeader::Loop(tok.line, tok.col));
                    at_stmt_start = false;
                } else if (tok.text == "case" || tok.text == "default")
                    && matches!(top, Container::Brace(BraceKind::SwitchBody))
                {
                    in_switch_label = true;
                    at_stmt_start = false;
                    clear_after_control = true;
                } else {
                    at_stmt_start = false;
                    clear_after_control = true;
                }
            }
            _ => {
                at_stmt_start = false;
                clear_after_control = true;
            }
        }

        if clear_after_control {
            after_control = None;
        }
        let _ = prev_closed_block;
        idx += 1;
    }

    if edits.is_empty() {
        return source.to_string();
    }
    edits.sort_by_key(|(s, e, _)| (*s, *e));
    let mut out = String::with_capacity(source.len() + edits.len() * 48);
    let mut cursor = 0usize;
    for (s, e, rep) in edits {
        if s >= cursor && e <= source.len() {
            out.push_str(&source[cursor..s]);
            out.push_str(&rep);
            cursor = e;
        }
    }
    out.push_str(&source[cursor..]);
    out
}

fn is_prev_switch_paren(toks: &[Token<'_>], rparen_idx: usize) -> bool {
    let mut depth = 0usize;
    let mut i = rparen_idx;
    loop {
        match toks[i].kind {
            TokKind::RParen => depth += 1,
            TokKind::LParen => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return i > 0
                        && toks[i - 1].kind == TokKind::Ident
                        && toks[i - 1].text == "switch";
                }
            }
            _ => {}
        }
        if i == 0 {
            return false;
        }
        i -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_line_numbers_and_instruments_debugger_and_loops() {
        let src = "let x = 20;\ndebugger;\nconsole.log('resumed', x);\nx * 2 + 2";
        let out = instrument_js(1, src);
        assert_eq!(out.lines().count(), src.lines().count());
        assert!(out.contains("__dd_dbg_s(1,0,0,(__e)=>eval(__e));let x = 20;"));
        assert!(out.contains("__dd_dbg_d(1,1,0,(__e)=>eval(__e));"));
        assert!(out.contains("__dd_dbg_s(1,2,0,(__e)=>eval(__e));console.log"));
        assert!(out.contains("__dd_dbg_s(1,3,0,(__e)=>eval(__e));x * 2 + 2"));
    }

    #[test]
    fn instruments_object_method_bodies_without_corrupting_object_literal() {
        let src = "const __dd_default = {\n  async fetch() {\n    const answer = 40 + 2;\n    return new Response(String(answer));\n  },\n};\n";
        let out = instrument_js(7, src);
        assert!(out.contains("async fetch() {"));
        assert!(out.contains("__dd_dbg_s(7,2,4,(__e)=>eval(__e));const answer = 40 + 2;"));
        assert!(out.contains("__dd_dbg_s(7,3,4,(__e)=>eval(__e));return new Response"));
        // Must NOT instrument `async fetch()` key as a statement inside the object literal!
        assert!(!out.contains("__dd_dbg_s(7,1,"));
    }

    #[test]
    fn instruments_while_loop_backedge() {
        let src = "let n = 0;\nwhile (true) { n++; }";
        let out = instrument_js(3, src);
        assert!(out.contains("while (true) {__dd_dbg_l(3,1,0,(__e)=>eval(__e));"));
    }
}
