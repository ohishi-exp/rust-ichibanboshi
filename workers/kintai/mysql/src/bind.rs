//! SQL の名前付き引数 (`:from`・`:driver` 等) をリテラルに展開する (COM_QUERY はテキストプロトコルで、
//! prepared statement を持たないため)。
//!
//! - 受ける値は [`Value`] の 4 種だけ: 整数 (`u64`)・日時 (`'YYYY-MM-DD HH:MM:SS'`)・NULL・数字だけの文字列
//!   ([`Digits`]。`'…'` で囲む。23 桁の運行NO のように `u64` に入らない数字の列のため)。
//!   **任意の文字列を受ける口は作らない** (自由入力の文字列を SQL に入れる経路を持たない)
//! - 字句: `'…'`・`"…"`・バッククォートの中は飛ばす (`\` のエスケープと、引用符の 2 連続のエスケープも考慮)。
//!   `:` の直後が識別子の頭 (英字か `_`) でなければそのまま
//! - 未知の名前・渡したのに使われない名前・同じ名前を 2 回渡す・閉じない引用符・コメント (`#`・`/*`・`-- `) はエラー
//! - 同じ名前が何回出ても、全部に同じ値が入る

use chrono::NaiveDateTime;

/// 埋め込める値。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Value {
    UInt(u64),
    /// `'YYYY-MM-DD HH:MM:SS'` で書く
    DateTime(NaiveDateTime),
    Null,
    /// `'0123…'` で書く (数字だけなので引用符の中に escape の要る文字は入らない)
    Digits(Digits),
}

/// 数字だけの文字列 (1〜32 文字の `0-9`)。[`Digits::new`] を通らなければ作れない。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Digits {
    buf: [u8; Digits::MAX],
    len: u8,
}

impl Digits {
    /// 最大の長さ。
    pub const MAX: usize = 32;

    /// `^[0-9]{1,32}$` を満たさなければ `None`。
    pub fn new(s: &str) -> Option<Self> {
        let b = s.as_bytes();
        if b.is_empty() || b.len() > Self::MAX || !b.iter().all(u8::is_ascii_digit) {
            return None;
        }
        let mut buf = [0u8; Self::MAX];
        buf[..b.len()].copy_from_slice(b);
        Some(Self {
            buf,
            len: b.len() as u8,
        })
    }

    pub fn as_str(&self) -> &str {
        // new が ASCII の数字だけを入れている
        std::str::from_utf8(&self.buf[..self.len as usize]).unwrap_or_default()
    }
}

impl Value {
    fn literal(&self) -> String {
        match self {
            Value::UInt(n) => n.to_string(),
            Value::DateTime(dt) => format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S")),
            Value::Null => "NULL".to_string(),
            Value::Digits(d) => format!("'{}'", d.as_str()),
        }
    }
}

/// 展開の失敗。名前は SQL 定数の側のもの (利用者の入力ではない)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindError {
    /// SQL に出てくるが渡されていない名前
    UnknownName(String),
    /// 渡したが SQL に出てこない名前
    UnusedName(String),
    /// 同じ名前を 2 回渡した
    DuplicateName(String),
    /// 引用符・バッククォートが閉じていない
    Unterminated,
    /// コメントがある (`#`・`/*`・`-- `)
    Comment,
}

impl BindError {
    /// 応答とログに出す種別の名前。
    pub fn kind(&self) -> &'static str {
        match self {
            BindError::UnknownName(_) => "bind_unknown_name",
            BindError::UnusedName(_) => "bind_unused_name",
            BindError::DuplicateName(_) => "bind_duplicate_name",
            BindError::Unterminated => "bind_unterminated",
            BindError::Comment => "bind_comment",
        }
    }
}

/// 字句の 1 片。
enum Piece<'a> {
    /// そのまま写す部分
    Text(&'a str),
    /// `:name` の `name`
    Name(&'a str),
}

/// 引用符 `q` で始まる区間の終わり (閉じ引用符の次の位置) を返す。`start` は開き引用符の位置。
fn skip_quoted(b: &[u8], start: usize, q: u8) -> Result<usize, BindError> {
    let mut i = start + 1;
    while i < b.len() {
        // `\` のエスケープ (引用符の中だけ) と、引用符の 2 連続はどちらも 2 byte 飛ばす
        let backslash = b[i] == b'\\' && q != b'`';
        let doubled = b[i] == q && b.get(i + 1) == Some(&q);
        if backslash || doubled {
            i += 2;
        } else if b[i] == q {
            return Ok(i + 1);
        } else {
            i += 1;
        }
    }
    Err(BindError::Unterminated)
}

fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_'
}

fn is_ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

/// コメントの始まりか (`#`・`/*`・`--` の後が空白か終わり)。
fn is_comment(b: &[u8], i: usize) -> bool {
    match b[i] {
        b'#' => true,
        b'/' => b.get(i + 1) == Some(&b'*'),
        b'-' => b.get(i + 1) == Some(&b'-') && b.get(i + 2).is_none_or(u8::is_ascii_whitespace),
        _ => false,
    }
}

/// SQL を「写す部分」と「名前」に分ける。区切りはすべて ASCII なので UTF-8 の途中では切れない。
fn tokenize(sql: &str) -> Result<Vec<Piece<'_>>, BindError> {
    let b = sql.as_bytes();
    let mut pieces = Vec::new();
    let (mut i, mut text_start) = (0, 0);
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' | b'`' => i = skip_quoted(b, i, b[i])?,
            _ if is_comment(b, i) => return Err(BindError::Comment),
            b':' if b.get(i + 1).copied().is_some_and(is_ident_start) => {
                pieces.push(Piece::Text(&sql[text_start..i]));
                let end = (i + 1..b.len())
                    .find(|&j| !is_ident(b[j]))
                    .unwrap_or(b.len());
                pieces.push(Piece::Name(&sql[i + 1..end]));
                (i, text_start) = (end, end);
            }
            _ => i += 1,
        }
    }
    pieces.push(Piece::Text(&sql[text_start..]));
    Ok(pieces)
}

/// SQL に出てくる名前の集合 (出現順・重複なし)。
pub fn names(sql: &str) -> Result<Vec<String>, BindError> {
    let mut out: Vec<String> = Vec::new();
    for piece in tokenize(sql)? {
        if let Piece::Name(n) = piece {
            if !out.iter().any(|o| o == n) {
                out.push(n.to_string());
            }
        }
    }
    Ok(out)
}

/// 名前付き引数を値のリテラルに置き換えた SQL を返す。
pub fn expand(sql: &str, params: &[(&str, Value)]) -> Result<String, BindError> {
    for (i, (name, _)) in params.iter().enumerate() {
        if params[..i].iter().any(|(n, _)| n == name) {
            return Err(BindError::DuplicateName(name.to_string()));
        }
    }
    let mut used = vec![false; params.len()];
    let mut out = String::with_capacity(sql.len());
    for piece in tokenize(sql)? {
        match piece {
            Piece::Text(t) => out.push_str(t),
            Piece::Name(n) => {
                let Some(i) = params.iter().position(|(p, _)| *p == n) else {
                    return Err(BindError::UnknownName(n.to_string()));
                };
                used[i] = true;
                out.push_str(&params[i].1.literal());
            }
        }
    }
    if let Some(i) = used.iter().position(|u| !u) {
        return Err(BindError::UnusedName(params[i].0.to_string()));
    }
    Ok(out)
}
