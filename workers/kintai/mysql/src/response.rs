//! コマンドと応答: OK / ERR / EOF の判定、COM_QUERY・COM_QUIT、テキストプロトコルの結果セット
//! (column count → column definitions → EOF → rows → EOF)。CLIENT_DEPRECATE_EOF は立てないので EOF が必ず来る。

use crate::packet::{utf8, Reader};
use crate::Error;

const COM_QUIT: u8 = 0x01;
const COM_QUERY: u8 = 0x03;

/// ERR パケット。`message` はサーバーの文言 (ユーザー名・ホストを含みうる) なので応答にもログにも出さないこと。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrPacket {
    pub code: u16,
    /// `#` の後の 5 文字 (CLIENT_PROTOCOL_41)。handshake 前の ERR には無い
    pub sql_state: Option<String>,
    pub message: String,
}

impl ErrPacket {
    pub fn parse(payload: &[u8]) -> Result<Self, Error> {
        let mut r = Reader::new(payload);
        let header = r.u8()?;
        if header != 0xFF {
            return Err(Error::Unexpected(header));
        }
        let code = r.u16()?;
        let sql_state = if r.peek() == Some(b'#') {
            r.u8()?;
            Some(utf8(r.bytes(5)?)?)
        } else {
            None
        };
        let message = String::from_utf8_lossy(r.rest()).into_owned();
        Ok(Self {
            code,
            sql_state,
            message,
        })
    }
}

/// OK パケット (info 文字列は読まない)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OkPacket {
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status: u16,
    pub warnings: u16,
}

impl OkPacket {
    pub fn parse(payload: &[u8]) -> Result<Self, Error> {
        let mut r = Reader::new(payload);
        let header = r.u8()?;
        if header != 0x00 {
            return Err(Error::Unexpected(header));
        }
        Ok(Self {
            affected_rows: r.lenenc_int()?,
            last_insert_id: r.lenenc_int()?,
            status: r.u16()?,
            warnings: r.u16()?,
        })
    }
}

/// EOF パケットか (先頭 0xFE で 9 byte 未満。0xFE で始まる 8 byte 長の lenenc の行と区別する)。
pub fn is_eof(payload: &[u8]) -> bool {
    payload.first() == Some(&0xFE) && payload.len() < 9
}

/// COM_QUERY の payload。
pub fn com_query(sql: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + sql.len());
    out.push(COM_QUERY);
    out.extend_from_slice(sql.as_bytes());
    out
}

/// COM_QUIT の payload。
pub fn com_quit() -> Vec<u8> {
    vec![COM_QUIT]
}

/// COM_QUERY への最初の返事。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryResponse {
    /// 結果セットを返さない文 (SET 等)
    Ok(OkPacket),
    /// 結果セットが続く。値は列の数
    ResultSet(u64),
}

/// COM_QUERY への最初の返事を読む。ERR は `Error::Server`、LOCAL INFILE の要求 (0xFB) は応じないので `Error::Unexpected`。
pub fn parse_query_response(payload: &[u8]) -> Result<QueryResponse, Error> {
    match payload.first() {
        Some(0x00) => Ok(QueryResponse::Ok(OkPacket::parse(payload)?)),
        Some(0xFF) => Err(Error::Server(ErrPacket::parse(payload)?)),
        Some(0xFB) => Err(Error::Unexpected(0xFB)),
        Some(_) => Ok(QueryResponse::ResultSet(Reader::new(payload).lenenc_int()?)),
        None => Err(Error::Truncated),
    }
}

/// 列の定義 (Protocol::ColumnDefinition41) のうち使うものだけ。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub column_type: u8,
    pub charset: u16,
}

/// Protocol::ColumnDefinition41 を読む。
pub fn parse_column_definition(payload: &[u8]) -> Result<Column, Error> {
    let mut r = Reader::new(payload);
    r.lenenc_str()?; // catalog ("def")
    r.lenenc_str()?; // schema
    r.lenenc_str()?; // table
    r.lenenc_str()?; // org_table
    let name = utf8(r.lenenc_str()?)?;
    r.lenenc_str()?; // org_name
    r.lenenc_int()?; // 固定長部分の長さ (0x0c)
    let charset = r.u16()?;
    r.u32()?; // column length
    let column_type = r.u8()?;
    Ok(Column {
        name,
        column_type,
        charset,
    })
}

/// テキストプロトコルの 1 行。各列は文字列のバイト列か NULL。
pub type Row = Vec<Option<Vec<u8>>>;

/// テキストプロトコルの行を読む (各列は lenenc string か 0xFB = NULL)。列数が合わなければ `Malformed`。
pub fn parse_row(payload: &[u8], columns: usize) -> Result<Row, Error> {
    let mut r = Reader::new(payload);
    let mut row = Vec::with_capacity(columns);
    for _ in 0..columns {
        if r.peek() == Some(0xFB) {
            r.u8()?;
            row.push(None);
        } else {
            row.push(Some(r.lenenc_str()?.to_vec()));
        }
    }
    if r.remaining() != 0 {
        return Err(Error::Malformed);
    }
    Ok(row)
}

/// 読み終えた結果セット。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

impl ResultSet {
    /// `row` 行目 `col` 列目を UTF-8 の文字列として読む。範囲外・NULL・UTF-8 でなければ `None`。
    pub fn text(&self, row: usize, col: usize) -> Option<&str> {
        let cell = self.rows.get(row)?.get(col)?.as_deref()?;
        std::str::from_utf8(cell).ok()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Columns,
    ColumnsEof,
    Rows,
}

/// 結果セットのパケットを 1 つずつ読ませる。column count のパケットの後から、最後の EOF までを渡す。
#[derive(Debug)]
pub struct ResultSetReader {
    count: usize,
    state: State,
    set: ResultSet,
}

impl ResultSetReader {
    pub fn new(column_count: u64) -> Result<Self, Error> {
        let count = usize::try_from(column_count).map_err(|_| Error::Malformed)?;
        let state = if count == 0 {
            State::ColumnsEof
        } else {
            State::Columns
        };
        Ok(Self {
            count,
            state,
            set: ResultSet::default(),
        })
    }

    /// 次のパケットを読ませる。最後の EOF を読んだら `Some(結果セット)`。途中の ERR は `Error::Server`。
    /// `Some` を返した後には呼ばないこと。
    pub fn push(&mut self, payload: &[u8]) -> Result<Option<ResultSet>, Error> {
        if payload.first() == Some(&0xFF) {
            return Err(Error::Server(ErrPacket::parse(payload)?));
        }
        match self.state {
            State::Columns => {
                self.set.columns.push(parse_column_definition(payload)?);
                if self.set.columns.len() == self.count {
                    self.state = State::ColumnsEof;
                }
                Ok(None)
            }
            State::ColumnsEof => {
                if !is_eof(payload) {
                    return Err(Error::Unexpected(
                        payload.first().copied().unwrap_or_default(),
                    ));
                }
                self.state = State::Rows;
                Ok(None)
            }
            State::Rows if is_eof(payload) => Ok(Some(std::mem::take(&mut self.set))),
            State::Rows => {
                self.set.rows.push(parse_row(payload, self.count)?);
                Ok(None)
            }
        }
    }
}
