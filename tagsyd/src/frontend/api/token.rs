//! Search-query lexer (stage 1 of two — see [`ApiService::parse_query`]).
//!
//! This module is deliberately **pure**: it turns a raw query string into a
//! vector of [`Token`]s without ever touching the database. Resolving a token's
//! text into concrete [`TagId`](tagsy_core::TagId)s or applying it against
//! the stored files happens in the resolver stage.
//!
//! # Grammar
//!
//! A query is a whitespace-separated sequence of *tokens*. A token is:
//!
//! 1. An optional `!` (negation) — must be a standalone whitespace-delimited
//!    token; `!foo` is **not** a negation, it's a literal token whose text
//!    starts with `!`.
//! 2. An optional *kind prefix* — one of `/t`, `/l`, `/p`, again standalone:
//!    - `/t` — tag token: match tags whose name/id resolves from the payload.
//!    - `/l` — logical-path token: substring match on the file's logical path.
//!    - `/p` — physical-path token (reserved; not wired up yet).
//!
//!    Unknown `/x` tokens are **not** prefixes: they become literal tokens
//!    whose payload starts with `/`. This keeps `/home/lucas` searchable.
//! 3. A *payload*, one of:
//!    - A double-quoted string `"..."` — a literal substring, capturing
//!      whitespace verbatim. Supports backslash escapes `\"` and `\\`; any
//!      other `\c` is left as-is (`\c`).
//!    - A `%`-delimited string `%...%` — a **regular expression**, also
//!      capturing whitespace verbatim, passed to the regex engine with no
//!      escape processing of its own (see [`read_regex`]).
//!    - Or a bare run of non-whitespace characters — a literal substring.
//!
//! A token without a kind prefix is [`TokenKind::Any`] — the resolver will
//! match its payload against *both* names and tags (union).
//!
//! # Why the delimiter selects the matcher
//!
//! The payload's *delimiter* chooses how to match (literal vs regex) while the
//! *prefix* chooses what to match against. Keeping them on separate axes means
//! they compose without the grammar enumerating combinations: `/l %^photos/%`,
//! `/t %^wip-%` and `! %\.tmp$%` all work by construction.
//!
//! `%` rather than the conventional `/.../`: this is a *path* search language,
//! so `/` is both extremely common inside the patterns being written (making
//! `/^photos\/.*/` the normal case) and already spoken for as the prefix
//! sigil, which would make `/tmp/foo` ambiguous between a literal path search
//! and a regex. `%` is rare inside logical paths, has no shell meaning, and
//! its SQL `LIKE` association points the right way.
//!
//! Quoting therefore doubles as the escape hatch for a payload that starts
//! with `%`: `"%20"` searches for the literal text `%20`, where a bare `%20`
//! would begin an unterminated regex and be dropped.
//!
//! # Error recovery
//!
//! Parsing is **infallible**: `lex_query` always returns a `Vec<Token>`, never
//! an error. Malformed input is skipped rather than rejected, so a search box
//! stays usable mid-typing. Specifically, when the lexer hits any of the
//! following it *discards the current token in progress* and resumes at the
//! next whitespace boundary:
//!
//! - a `!` or kind prefix followed by nothing (`!`, `/t`, `! /t` at EOF);
//! - conflicting kind prefixes (`/t /l foo` drops the `/t /l` token and
//!   continues from `foo`);
//! - a duplicate `!` (`! ! foo` drops that token);
//! - an unterminated quoted string (`"foo` at EOF is dropped entirely);
//! - an unterminated regex (`%foo` at EOF, same rule).
//!
//! Diagnostics are intentionally not surfaced: the caller sees only the tokens
//! that parsed cleanly.

/// What kind of filter a token expresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    /// Match names *and* tags (union).
    Any,
    /// The payload names a tag.
    Tag,
    /// The payload is a logical-path substring or tag name.
    Name,
    /// The payload is a logical-path substring.
    Logical,
    /// The payload is a physical-path substring.
    Physical,
}

/// One parsed token of the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub text: String,
    pub negated: bool,
    /// True when the payload was written as `%...%`, meaning `text` is a
    /// regular expression rather than a literal substring.
    ///
    /// Deliberately independent of [`TokenKind`]: the prefix chooses *what
    /// field* to match against, the delimiter chooses *how* to match. The
    /// two compose freely, so `/l %^photos/%` and `! %\.tmp$%` are both
    /// meaningful without the grammar having to enumerate the
    /// combinations.
    pub regex: bool,
}

/// Lex a query string into [`Token`]s. See the module docs for the grammar
/// and the error-recovery contract (this function is infallible; malformed
/// input is silently dropped).
pub fn lex_query(query: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut cursor = query;

    while !{
        cursor = cursor.trim_start();
        cursor.is_empty()
    } {
        let (maybe_chunk, rest) = lex_one_token(cursor);
        if let Some(token) = maybe_chunk {
            tokens.push(token);
        }
        cursor = rest;
    }
    tokens
}

/// Try to lex one token starting at `cursor` (which must be non-empty and
/// not start with whitespace). Returns the parsed token (if any) and the
/// remainder of the string to keep lexing.
///
/// On any grammar error we return `(None, rest_after_next_whitespace)` —
/// the whole in-progress token is discarded and lexing resumes at the next
/// token boundary. An unterminated quote is treated as consuming the whole
/// rest of the string (there is no whitespace boundary that could rescue
/// half of a broken quote).
fn lex_one_token(cursor: &str) -> (Option<Token>, &str) {
    let mut rest = cursor;
    let mut negated = false;
    let mut kind: Option<TokenKind> = None;

    // Consume prefix words until we hit something that isn't a prefix —
    // that becomes the payload. On any grammar error we drop the current
    // token and resume at the *next* whitespace boundary (the word that
    // caused the error is itself skipped).
    loop {
        let word_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let word = &rest[..word_end];

        match word {
            "!" => {
                if negated {
                    return (None, &rest[word_end..]);
                }
                negated = true;
            }
            "/t" | "/n" | "/l" | "/p" => {
                if kind.is_some() {
                    return (None, &rest[word_end..]);
                }
                kind = Some(match word {
                    "/t" => TokenKind::Tag,
                    "/n" => TokenKind::Name,
                    "/l" => TokenKind::Logical,
                    "/p" => TokenKind::Physical,
                    _ => unreachable!(),
                });
            }
            _ => break, // not a prefix — treat as payload
        }

        // Advance past the prefix and its trailing whitespace.
        rest = rest[word_end..].trim_start();
        if rest.is_empty() {
            // Prefix with no following token: drop it.
            return (None, rest);
        }
    }

    // Read the payload: regex, quoted string, or bare token.
    let (text, regex, rest) = if let Some(after_quote) = rest.strip_prefix('"') {
        match read_quoted(after_quote) {
            Some((text, rest)) => (text, false, rest),
            // Unterminated quote: discard the rest of the input entirely.
            None => return (None, ""),
        }
    } else if let Some(after_delimiter) = rest.strip_prefix('%') {
        match read_regex(after_delimiter) {
            Some((text, rest)) => (text, true, rest),
            // Unterminated, exactly like an unterminated quote.
            None => return (None, ""),
        }
    } else {
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        (rest[..end].to_owned(), false, &rest[end..])
    };

    (
        Some(Token {
            kind: kind.unwrap_or(TokenKind::Any),
            text,
            negated,
            regex,
        }),
        rest,
    )
}

/// Read a `%...%`-delimited regex payload starting *after* the opening
/// delimiter. Returns `Some((pattern, remainder_after_closing_delimiter))`,
/// or `None` if the closing `%` is missing.
///
/// Unlike [`read_quoted`] this performs **no escape processing at all**:
/// the payload is handed to the regex engine exactly as written, and
/// terminates at the first `%`. A regex is already a language with its own
/// escaping rules, and layering a second one on top would mean `\.` and
/// `\\.` differing for reasons that have nothing to do with the pattern.
///
/// The cost is that a literal `%` cannot appear directly in a pattern.
/// That is not a real limitation — regex spells it `\x25` — and it buys a
/// payload the user can copy verbatim out of any other regex tool.
fn read_regex(input: &str) -> Option<(String, &str)> {
    let end = input.find('%')?;
    Some((input[..end].to_owned(), &input[end + '%'.len_utf8()..]))
}

/// Read a `"..."`-quoted payload starting *after* the opening quote.
/// Returns `Some((unescaped_text, remainder_after_closing_quote))`, or
/// `None` if the closing quote is missing (unterminated string).
fn read_quoted(input: &str) -> Option<(String, &str)> {
    let mut out = String::new();
    let mut chars = input.char_indices();
    while let Some((idx, ch)) = chars.next() {
        match ch {
            '"' => {
                let rest = &input[idx + ch.len_utf8()..];
                return Some((out, rest));
            }
            '\\' => match chars.next() {
                Some((_, esc @ ('"' | '\\'))) => out.push(esc),
                Some((_, other)) => {
                    // Unknown escape: keep the backslash + char verbatim.
                    out.push('\\');
                    out.push(other);
                }
                // Trailing backslash inside a quote: treat as unterminated.
                None => return None,
            },
            _ => out.push(ch),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn any(text: &str) -> Token {
        Token {
            kind: TokenKind::Any,
            text: text.to_owned(),
            negated: false,
            regex: false,
        }
    }

    fn tag(text: &str) -> Token {
        Token {
            kind: TokenKind::Tag,
            text: text.to_owned(),
            negated: false,
            regex: false,
        }
    }

    fn logical(text: &str) -> Token {
        Token {
            kind: TokenKind::Logical,
            text: text.to_owned(),
            negated: false,
            regex: false,
        }
    }

    fn negate(mut token: Token) -> Token {
        token.negated = true;
        token
    }

    /// Mark a token as a regex payload, so expectations read as
    /// `regex(logical("^photos/"))`.
    fn regex(mut token: Token) -> Token {
        token.regex = true;
        token
    }

    #[test]
    fn empty_and_whitespace_only_yield_no_chunks() {
        assert_eq!(lex_query(""), Vec::<Token>::new());
        assert_eq!(lex_query("   \t  "), Vec::<Token>::new());
    }

    #[test]
    fn bare_words_become_any_chunks() {
        assert_eq!(lex_query("foo bar"), vec![any("foo"), any("bar")]);
    }

    #[test]
    fn quoted_strings_capture_whitespace() {
        assert_eq!(lex_query(r#""foo bar" baz"#), vec![
            any("foo bar"),
            any("baz")
        ],);
    }

    #[test]
    fn quoted_string_supports_escapes() {
        assert_eq!(lex_query(r#""a\"b\\c""#), vec![any(r#"a"b\c"#)]);
    }

    #[test]
    fn unknown_backslash_escape_is_kept_literally() {
        // `\n` is not a recognized escape; we keep it verbatim rather than
        // silently interpreting it, to match the "quotes are only for
        // whitespace capture" contract.
        assert_eq!(lex_query(r#""a\nb""#), vec![any(r"a\nb")]);
    }

    #[test]
    fn percent_delimiters_produce_a_regex_payload() {
        assert_eq!(lex_query(r"%\.md$%"), vec![regex(any(r"\.md$"))]);
    }

    #[test]
    fn regex_payload_captures_whitespace() {
        assert_eq!(lex_query("%foo bar% baz"), vec![
            regex(any("foo bar")),
            any("baz")
        ]);
    }

    /// The delimiter (how to match) and the prefix (what to match against)
    /// are independent axes, so every combination lexes without the
    /// grammar special-casing them.
    #[test]
    fn regex_composes_with_kind_prefixes_and_negation() {
        assert_eq!(lex_query("/l %^photos/%"), vec![regex(logical("^photos/"))]);
        assert_eq!(lex_query("/t %^wip-%"), vec![regex(tag("^wip-"))]);
        assert_eq!(lex_query(r"! %\.tmp$%"), vec![negate(regex(any(
            r"\.tmp$"
        )))]);
        assert_eq!(lex_query("! /l %^tmp/%"), vec![negate(regex(logical(
            "^tmp/"
        )))]);
    }

    /// Slashes need no escaping, which is the entire reason the delimiter
    /// is `%` and not `/`.
    #[test]
    fn regex_payload_may_contain_slashes_verbatim() {
        assert_eq!(lex_query("%^photos/.*/raw$%"), vec![regex(any(
            "^photos/.*/raw$"
        ))]);
    }

    /// No escape processing inside `%...%`: the payload reaches the regex
    /// engine exactly as typed, so backslashes are not consumed the way
    /// `read_quoted` consumes them.
    #[test]
    fn regex_payload_does_not_process_backslash_escapes() {
        assert_eq!(lex_query(r"%a\\b%"), vec![regex(any(r"a\\b"))]);
        assert_eq!(lex_query(r#"%a\"b%"#), vec![regex(any(r#"a\"b"#))]);
    }

    /// A regex terminates at the first `%`; there is no way to escape one,
    /// by design (regex spells a literal percent `\x25`).
    #[test]
    fn regex_terminates_at_the_first_delimiter() {
        assert_eq!(lex_query("%a%b%"), vec![regex(any("a")), any("b%")]);
    }

    /// Quoting is the escape hatch for a literal payload that starts with
    /// `%`, which a bare token can no longer express.
    #[test]
    fn quoting_keeps_a_leading_percent_literal() {
        assert_eq!(lex_query(r#""%20""#), vec![any("%20")]);
    }

    /// A `%` that is not in payload-leading position is an ordinary
    /// character, so `50%` still searches literally.
    #[test]
    fn percent_inside_a_bare_token_is_literal() {
        assert_eq!(lex_query("50% off"), vec![any("50%"), any("off")]);
    }

    #[test]
    fn empty_regex_is_allowed() {
        // Matches everything, exactly as the empty substring `""` does.
        assert_eq!(lex_query("%%"), vec![regex(any(""))]);
    }

    #[test]
    fn kind_prefixes_apply_to_the_next_chunk() {
        assert_eq!(lex_query("/t foo"), vec![tag("foo")]);
        assert_eq!(lex_query("/l foo"), vec![logical("foo")]);
    }

    #[test]
    fn kind_prefix_only_matches_as_standalone_token() {
        // `/tfoo` is not a `/t` prefix — it's a literal token starting with `/`.
        assert_eq!(lex_query("/tfoo"), vec![any("/tfoo")]);
    }

    #[test]
    fn negation_alone_and_with_kind_prefix() {
        assert_eq!(lex_query("! foo"), vec![negate(any("foo"))]);
        assert_eq!(lex_query("! /t foo"), vec![negate(tag("foo"))]);
        // Order of `!` and `/t` doesn't matter.
        assert_eq!(lex_query("/t ! foo"), vec![negate(tag("foo"))]);
    }

    #[test]
    fn negation_applies_to_quoted_payload() {
        assert_eq!(lex_query(r#"! /t "foo bar""#), vec![negate(tag("foo bar"))],);
    }

    #[test]
    fn bang_without_space_is_literal_not_negation() {
        // `!foo` is a literal token whose text is `!foo`, matching the
        // "prefixes are standalone tokens" rule from the grammar.
        assert_eq!(lex_query("!foo"), vec![any("!foo")]);
    }

    #[test]
    fn unknown_slash_prefix_is_literal() {
        // `/x` isn't a known kind prefix, so it's just a token payload.
        // This keeps paths like `/home/lucas` searchable.
        assert_eq!(lex_query("/x foo"), vec![any("/x"), any("foo")]);
        assert_eq!(lex_query("/home/lucas"), vec![any("/home/lucas")]);
    }

    #[test]
    fn mixed_query_parses_end_to_end() {
        // A realistic mix: bare word, tag, quoted logical path, negated tag.
        let got = lex_query(r#"foo /t bar /l "my file.txt" ! /t old"#);
        assert_eq!(got, vec![
            any("foo"),
            tag("bar"),
            logical("my file.txt"),
            negate(tag("old")),
        ],);
    }

    // The lexer is infallible: it drops the current token-in-progress on
    // any grammar error and resumes at the next whitespace boundary. The
    // tests below pin down exactly what "resume" means for each error
    // shape.

    #[test]
    fn unterminated_quote_drops_rest_of_input() {
        // Prior tokens are kept; the broken quote and everything after it
        // are discarded (there is no whitespace *inside* the broken quote
        // that could rescue the remainder).
        assert_eq!(lex_query(r#"foo "bar baz"#), vec![any("foo")]);
        assert_eq!(lex_query(r#""foo"#), Vec::<Token>::new());
    }

    #[test]
    fn unterminated_regex_drops_rest_of_input() {
        // Same recovery as an unterminated quote: a missing closing `%`
        // gives the lexer no boundary it could trust to resume at.
        assert_eq!(lex_query("foo %bar baz"), vec![any("foo")]);
        assert_eq!(lex_query("%foo"), Vec::<Token>::new());
    }

    #[test]
    fn trailing_prefix_is_silently_dropped() {
        // A prefix with no payload (`!`, `/t`, `! /t` at EOF) yields no
        // token but doesn't affect tokens already parsed.
        assert_eq!(lex_query("foo !"), vec![any("foo")]);
        assert_eq!(lex_query("foo /t"), vec![any("foo")]);
        assert_eq!(lex_query("foo ! /t"), vec![any("foo")]);
        // Just the bad prefix on its own is an empty result, not an error.
        assert_eq!(lex_query("!"), Vec::<Token>::new());
        assert_eq!(lex_query("/t"), Vec::<Token>::new());
    }

    #[test]
    fn conflicting_kind_prefixes_drop_that_chunk_only() {
        // `/t /l` conflicts — that token-in-progress is discarded at the
        // conflict point, so lexing resumes with `foo bar` intact.
        assert_eq!(lex_query("/t /l foo bar"), vec![any("foo"), any("bar")],);
    }

    #[test]
    fn duplicate_negation_drops_that_chunk_only() {
        // `! !` is a duplicate — drop it, keep everything else.
        assert_eq!(lex_query("first ! ! second third"), vec![
            any("first"),
            any("second"),
            any("third")
        ],);
    }

    #[test]
    fn errors_between_valid_chunks_do_not_bleed() {
        // Interleave several error shapes with valid tokens to prove each
        // recovery is local.
        let got = lex_query(r#"a /t /l b ! ! c "unterminated"#);
        assert_eq!(got, vec![any("a"), any("b"), any("c")]);
    }
}
