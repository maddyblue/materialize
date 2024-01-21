use std::str::CharIndices;

use rowan::{Checkpoint, GreenNode, GreenNodeBuilder};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(non_camel_case_types)]
#[repr(u16)]
enum SyntaxKind {
    // Lexed tokens.
    COMMENT = 0,
    SPACE,
    LINE,
    DIVIDER,
    TEXT,

    // Composite nodes.
    RECORD,
    DIRECTIVE,
    ARGUMENTS,
    INPUT,
    OUTPUT,
    STATEMENT,
    RESET_SERVER,
    OK_ERR,
    OK,
    TYPES,
    OPTIONS,
    LABEL,
    QUERY,

    ERR,
    ERR_STRING,

    // Special nodes.
    ERROR,
    ROOT, // Top-level node, must be last.
}
use SyntaxKind::*;

impl From<SyntaxKind> for rowan::SyntaxKind {
    fn from(kind: SyntaxKind) -> Self {
        Self(kind as u16)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Lang {}
impl rowan::Language for Lang {
    type Kind = SyntaxKind;
    fn kind_from_raw(raw: rowan::SyntaxKind) -> Self::Kind {
        assert!(raw.0 <= ROOT as u16);
        unsafe { std::mem::transmute::<u16, SyntaxKind>(raw.0) }
    }
    fn kind_to_raw(kind: Self::Kind) -> rowan::SyntaxKind {
        kind.into()
    }
}

struct Parse {
    green_node: GreenNode,
    #[allow(unused)]
    errors: Vec<String>,
}

fn parse(text: &str) -> Parse {
    struct Parser<'a> {
        /// input tokens, including whitespace,
        /// in *reverse* order.
        tokens: Vec<(SyntaxKind, &'a str)>,
        /// the in-progress tree.
        builder: GreenNodeBuilder<'static>,
        /// the list of syntax errors we've accumulated
        /// so far.
        errors: Vec<String>,
    }

    /// The outcome of parsing a single record.
    enum RecordRes {
        /// A record was successfully parsed.
        Ok,
        /// Nothing was parsed, as no significant tokens remained.
        Eof,
        /// A parsing error occurred.
        Error(String),
    }

    impl<'a> Parser<'a> {
        fn parse(mut self) -> Parse {
            self.builder.start_node(ROOT.into());
            loop {
                match self.record() {
                    RecordRes::Eof => break,
                    RecordRes::Error(err) => {
                        self.start_node(ERROR);
                        self.errors.push(err);
                        let mut last_line = false;
                        // Eat tokens until two lines. This should be correct for everything except
                        // possibly multiline input.
                        loop {
                            match self.current() {
                                Some(LINE) => {
                                    if last_line {
                                        self.bump();
                                        break;
                                    }
                                    last_line = true;
                                }
                                None => break,
                                Some(_) => last_line = false,
                            }
                            self.bump();
                        }
                        self.builder.finish_node();
                    }
                    RecordRes::Ok => (),
                }
            }
            self.skip_ws();
            self.builder.finish_node();
            Parse {
                green_node: self.builder.finish(),
                errors: self.errors,
            }
        }
        fn output(&mut self) {
            if !matches!(self.current(), Some(SPACE | TEXT | DIVIDER)) {
                return;
            }
            self.builder.start_node(OUTPUT.into());
            let mut last_line = false;
            loop {
                match self.current() {
                    Some(SPACE | TEXT | DIVIDER) => {}
                    Some(LINE) => {
                        // Two lines in a row.
                        if last_line {
                            break;
                        }
                    }
                    _ => break,
                }
                last_line = self.current() == Some(LINE);
                self.bump();
            }
            self.builder.finish_node();
        }
        fn input(&mut self) -> Result<(), String> {
            self.as_kind(INPUT, |parser| {
                let mut last_line = false;
                loop {
                    match parser.current() {
                        Some(SPACE | TEXT) => {}
                        Some(LINE) => {
                            // Two lines in a row.
                            if last_line {
                                break;
                            }
                        }
                        _ => break,
                    }
                    last_line = parser.current() == Some(LINE);
                    parser.bump();
                }
                Ok(())
            })
        }
        fn arguments(&mut self) {
            self.start_node(ARGUMENTS);
            loop {
                match self.current() {
                    Some(SPACE | TEXT) => {
                        self.bump();
                    }
                    Some(LINE) => {
                        self.bump();
                        break;
                    }
                    _ => break,
                }
            }
            self.builder.finish_node();
        }
        fn start_node(&mut self, kind: impl Into<rowan::SyntaxKind>) {
            self.builder.start_node(kind.into());
        }
        fn start_node_at(&mut self, checkpoint: Checkpoint, kind: impl Into<rowan::SyntaxKind>) {
            self.builder.start_node_at(checkpoint, kind.into());
        }
        fn as_kind<F, T>(
            &mut self,
            kind: impl Into<rowan::SyntaxKind>,
            mut f: F,
        ) -> Result<T, String>
        where
            F: FnMut(&mut Self) -> Result<T, String>,
        {
            self.start_node(kind);
            let res = f(self);
            self.builder.finish_node();
            res
        }
        fn as_kind_at<F, T>(
            &mut self,
            checkpoint: Checkpoint,
            kind: impl Into<rowan::SyntaxKind>,
            mut f: F,
        ) -> Result<T, String>
        where
            F: FnMut(&mut Self) -> Result<T, String>,
        {
            self.start_node_at(checkpoint, kind);
            let res = f(self);
            self.builder.finish_node();
            res
        }
        fn expect(&mut self, kind: SyntaxKind) -> Result<(), String> {
            let Some(cur) = self.current() else {
                return Err(format!("expected {kind:?}, got EOF"));
            };
            if cur != kind {
                return Err(format!("expected {kind:?}, got {cur:?}"));
            }
            self.bump();
            Ok(())
        }
        /// If the next token is TEXT with value `text`, consume it and return true.
        fn parse_text(&mut self, text: &str) -> bool {
            let Some((TEXT, s)) = self.peek() else {
                return false;
            };
            if s == text {
                self.bump();
                true
            } else {
                false
            }
        }
        fn peek_text(&mut self) -> Option<&str> {
            self.peek().map(|(_, s)| s)
        }
        fn parse_statement(&mut self) -> Result<RecordRes, String> {
            self.as_kind(STATEMENT, |parser| {
                parser.directive();
                parser.as_kind(OK_ERR, |parser| {
                    let tok = match parser.peek() {
                        Some((TEXT, "ok")) => OK,
                        Some((TEXT, "error")) => ERR,
                        Some((_, other)) => return Err(format!("expect ok or error, got {other}")),
                        None => return Err(format!("expect ok or error, got EOF")),
                    };
                    parser.start_node(tok);
                    parser.bump();
                    parser.builder.finish_node();
                    if tok == ERR {
                        parser.as_kind(ERR_STRING, |parser| {
                            while let Some(TEXT | SPACE) = parser.current() {
                                parser.bump();
                            }
                            Ok(())
                        })?;
                    }
                    Ok(())
                })?;
                parser.expect(LINE)?;
                parser.input()?;
                Ok(RecordRes::Ok)
            })
        }
        fn parse_query(&mut self) -> Result<RecordRes, String> {
            self.as_kind(QUERY, |parser| {
                parser.directive();
                parser.as_kind(OK_ERR, |parser| {
                    let tok = match parser.peek() {
                        Some((TEXT, "ok")) => OK,
                        Some((TEXT, "error")) => ERR,
                        Some((_, other)) => {
                            return Err(format!("expect types or error, got {other}"))
                        }
                        None => return Err(format!("expect ok or error, got EOF")),
                    };
                    parser.start_node(tok);
                    parser.bump();
                    parser.builder.finish_node();
                    if tok == ERR {
                        parser.as_kind(ERR_STRING, |parser| {
                            while let Some(TEXT | SPACE) = parser.current() {
                                parser.bump();
                            }
                            Ok(())
                        })?;
                    }
                    Ok(())
                })?;
                parser.expect(LINE)?;
                parser.input()?;
                //parser.output()?;
                Ok(RecordRes::Ok)
            })
        }
        fn parse_reset_server(&mut self) -> Result<RecordRes, String> {
            self.as_kind(RESET_SERVER, |parser| {
                parser.directive();
                Ok(RecordRes::Ok)
            })
        }
        fn directive(&mut self) {
            self.start_node(DIRECTIVE);
            self.bump();
            self.builder.finish_node();
        }
        fn record(&mut self) -> RecordRes {
            self.skip_ws();
            let Some(t) = self.current() else {
                return RecordRes::Eof;
            };
            if t != TEXT {
                return RecordRes::Error("unexpected token".into());
            }
            self.start_node(RECORD);
            let res = match self.peek().expect("must exist").1 {
                "statement" => self.parse_statement(),
                "query" => self.parse_query(),
                /*


                "simple" => self.parse_simple(words),

                "hash-threshold" => {
                    let threshold = words
                        .next()
                        .ok_or_else(|| anyhow!("missing threshold in: {}", first_line))?
                        .parse::<u64>()
                        .map_err(|err| anyhow!("invalid threshold ({}) in: {}", err, first_line))?;
                    Ok(Record::HashThreshold { threshold })
                }

                // we'll follow the postgresql version of all these tests
                "skipif" => {
                    match words.next().unwrap() {
                        "postgresql" => {
                            // discard next record
                            self.parse_record()?;
                            self.parse_record()
                        }
                        _ => self.parse_record(),
                    }
                }
                "onlyif" => {
                    match words.next().unwrap() {
                        "postgresql" => self.parse_record(),
                        _ => {
                            // discard next record
                            self.parse_record()?;
                            self.parse_record()
                        }
                    }
                }

                "halt" => Ok(Record::Halt),

                // this is some cockroach-specific thing, we don't care
                "subtest" | "user" | "kv-batch-size" => self.parse_record(),

                "mode" => {
                    self.mode = match words.next() {
                        Some("cockroach") => Mode::Cockroach,
                        Some("standard") | Some("sqlite") => Mode::Standard,
                        other => bail!("unknown parse mode: {:?}", other),
                    };
                    self.parse_record()
                }

                "copy" => Ok(Record::Copy {
                    table_name: words
                        .next()
                        .ok_or_else(|| anyhow!("load directive missing table name"))?,
                    tsv_path: words
                        .next()
                        .ok_or_else(|| anyhow!("load directive missing TSV path"))?,
                }),
                */
                "reset-server" => self.parse_reset_server(),

                other => Err(format!("unexpected directive: {other}")),
            };
            self.builder.finish_node();
            res.unwrap_or_else(RecordRes::Error)
            /*
            self.bump();
            self.builder.finish_node();
            self.arguments();
            self.input();
            self.output();
            self.builder.finish_node();
            */
        }
        /// Advance one token and following non-line whitespace, adding them to the current branch
        /// of the tree builder.
        fn bump(&mut self) {
            loop {
                let (kind, text) = self.tokens.pop().unwrap();
                self.builder.token(kind.into(), text);
                if !matches!(self.current(), Some(SPACE)) {
                    break;
                }
            }
        }
        /// Peek at the first unprocessed token
        fn current(&self) -> Option<SyntaxKind> {
            self.tokens.last().map(|(kind, _)| *kind)
        }
        fn peek(&self) -> Option<(SyntaxKind, &str)> {
            self.tokens.last().map(|(k, s)| (*k, *s))
        }
        fn skip_ws(&mut self) {
            while matches!(self.current(), Some(SPACE | COMMENT | LINE)) {
                self.bump();
            }
        }
    }

    let mut tokens = lex(text);
    tokens.reverse();
    Parser {
        tokens,
        builder: GreenNodeBuilder::new(),
        errors: Vec::new(),
    }
    .parse()
}

type SyntaxNode = rowan::SyntaxNode<Lang>;
#[allow(unused)]
type SyntaxToken = rowan::SyntaxToken<Lang>;
#[allow(unused)]
type SyntaxElement = rowan::NodeOrToken<SyntaxNode, SyntaxToken>;

impl Parse {
    fn syntax(&self) -> SyntaxNode {
        SyntaxNode::new_root(self.green_node.clone())
    }

    fn root(&self) -> Root {
        Root::cast(self.syntax()).unwrap()
    }
}

macro_rules! ast_node {
    ($ast:ident, $kind:ident) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        #[repr(transparent)]
        struct $ast(SyntaxNode);
        impl $ast {
            #[allow(unused)]
            fn cast(node: SyntaxNode) -> Option<Self> {
                if node.kind() == $kind {
                    Some(Self(node))
                } else {
                    None
                }
            }
        }
    };
}

ast_node!(Root, ROOT);
ast_node!(Record, RECORD);
ast_node!(Directive, DIRECTIVE);
ast_node!(Arguments, ARGUMENTS);
ast_node!(Input, INPUT);
ast_node!(Output, OUTPUT);

impl Root {
    fn records(&self) -> impl Iterator<Item = Record> + '_ {
        self.0.children().filter_map(Record::cast)
    }
}

impl Record {
    fn directive(&self) -> Directive {
        self.0
            .children()
            .find_map(Directive::cast)
            .expect("must exist")
    }

    fn arguments(&self) -> Option<Arguments> {
        self.0.children().find_map(Arguments::cast)
    }

    fn input(&self) -> Option<Input> {
        self.0.children().find_map(Input::cast)
    }

    fn output(&self) -> Option<Output> {
        self.0.children().find_map(Output::cast)
    }
}

impl Directive {
    fn text(&self) -> SyntaxToken {
        self.0.first_token().unwrap()
    }
}

impl Arguments {
    fn args(&self) -> impl Iterator<Item = SyntaxToken> {
        self.0
            .children_with_tokens()
            .filter_map(|child| match child {
                rowan::NodeOrToken::Node(_) => None,
                rowan::NodeOrToken::Token(t) => {
                    if t.kind() == TEXT {
                        Some(t)
                    } else {
                        None
                    }
                }
            })
    }
}

fn lex(text: &str) -> Vec<(SyntaxKind, &str)> {
    let mut toks = Vec::new();
    for line in text.split_inclusive('\n') {
        if line.starts_with('#') {
            toks.push((COMMENT, line));
        } else if line.trim().is_empty() {
            toks.push((LINE, line));
        } else if line.trim_end() == "----" {
            toks.push((DIVIDER, line));
        } else {
            let mut chars = line.char_indices();
            let mut start = 0;
            let mut cur: Option<SyntaxKind> = None;
            while let Some((idx, ch)) = chars.next() {
                let next = if ch == '\n' {
                    LINE
                } else if ch.is_whitespace() {
                    SPACE
                } else {
                    TEXT
                };
                if cur.is_none() {
                    start = idx;
                    cur = Some(next);
                } else if let Some(current) = cur {
                    if current != next {
                        toks.push((current, &line[start..idx]));
                        start = idx;
                        cur = Some(next);
                    }
                }
            }
            if let Some(current) = cur {
                toks.push((current, &line[start..]));
            }
        }
    }
    toks
}

#[cfg(test)]
mod tests {
    use std::fs::read_to_string;

    use crate::{lex, parse};

    #[test]
    #[ignore]
    fn test_lex() {
        let file = read_to_string("test.slt").unwrap();
        dbg!(lex(&file));
    }

    #[test]
    fn test_parse() {
        let file = read_to_string("test.slt").unwrap();
        dbg!(parse(&file).syntax());
    }

    #[test]
    #[ignore]
    fn test_ast() {
        let file = read_to_string("test.slt").unwrap();
        let parsed = parse(&file);
        let root = parsed.root();
        let res = root.records().collect::<Vec<_>>();
        for rec in res {
            dbg!(&rec);
            dbg!(rec.directive().text());
            dbg!(rec.arguments());
            if let Some(args) = rec.arguments() {
                for a in args.args() {
                    dbg!(a);
                }
            }
            dbg!(rec.input());
            dbg!(rec.output());
        }
    }
}
