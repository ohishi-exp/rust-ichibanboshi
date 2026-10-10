//! パケットの枠 (3 byte の長さ little-endian + 1 byte の sequence id) と length-encoded の値。

use crate::Error;

/// 1 パケットに載る payload の上限。ちょうどこの長さのパケットは「続きがある」印。
pub const MAX_PAYLOAD: usize = 0xFF_FFFF;

/// 枠を外した 1 つの論理パケット。複数の物理パケットに分かれていたものは連結済み。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    /// 最後の物理パケットの sequence id (返事はこれ + 1 で送る)
    pub seq: u8,
    pub payload: Vec<u8>,
}

/// socket から読んだバイト列を溜め、揃ったパケットから順に取り出す。
#[derive(Debug, Default)]
pub struct PacketBuf {
    buf: Vec<u8>,
}

impl PacketBuf {
    pub fn new() -> Self {
        Self::default()
    }

    /// 読んだバイト列を足す。
    pub fn extend(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// 論理パケットが 1 つ揃っていれば取り出す。足りなければ `None` (もっと読む)。
    pub fn take(&mut self) -> Option<Packet> {
        let mut pos = 0;
        let mut payload = Vec::new();
        loop {
            let header = self.buf.get(pos..pos + 4)?;
            let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
            let seq = header[3];
            let body = self.buf.get(pos + 4..pos + 4 + len)?;
            payload.extend_from_slice(body);
            pos += 4 + len;
            if len < MAX_PAYLOAD {
                self.buf.drain(..pos);
                return Some(Packet { seq, payload });
            }
        }
    }
}

/// payload に枠を付ける。16MB 以上は分割が要るが、このクライアントが送るものは小さいので `TooLarge` にする。
pub fn frame(seq: u8, payload: &[u8]) -> Result<Vec<u8>, Error> {
    if payload.len() >= MAX_PAYLOAD {
        return Err(Error::TooLarge);
    }
    let len = payload.len() as u32;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len.to_le_bytes()[..3]);
    out.push(seq);
    out.extend_from_slice(payload);
    Ok(out)
}

/// length-encoded integer を書く。
pub fn put_lenenc_int(out: &mut Vec<u8>, v: u64) {
    if v < 0xFB {
        out.push(v as u8);
    } else if v <= 0xFFFF {
        out.push(0xFC);
        out.extend_from_slice(&(v as u16).to_le_bytes());
    } else if v <= 0xFF_FFFF {
        out.push(0xFD);
        out.extend_from_slice(&(v as u32).to_le_bytes()[..3]);
    } else {
        out.push(0xFE);
        out.extend_from_slice(&v.to_le_bytes());
    }
}

/// length-encoded string を書く。
pub fn put_lenenc_str(out: &mut Vec<u8>, s: &[u8]) {
    put_lenenc_int(out, s.len() as u64);
    out.extend_from_slice(s);
}

/// payload を先頭から読む。読み過ぎは `Error::Truncated`。
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// 残りのバイト数。
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// 次の 1 byte を消費せずに見る。
    pub fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self.pos.checked_add(n).ok_or(Error::Truncated)?;
        let out = self.buf.get(self.pos..end).ok_or(Error::Truncated)?;
        self.pos = end;
        Ok(out)
    }

    /// 残り全部。
    pub fn rest(&mut self) -> &'a [u8] {
        let out = &self.buf[self.pos..];
        self.pos = self.buf.len();
        out
    }

    pub fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.bytes(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16, Error> {
        let b = self.bytes(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    pub fn u24(&mut self) -> Result<u32, Error> {
        let b = self.bytes(3)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], 0]))
    }

    pub fn u32(&mut self) -> Result<u32, Error> {
        let b = self.bytes(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn u64(&mut self) -> Result<u64, Error> {
        let b = self.bytes(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    /// length-encoded integer。0xFB (NULL) と 0xFF (ERR の印) は整数ではないので `Malformed`。
    pub fn lenenc_int(&mut self) -> Result<u64, Error> {
        match self.u8()? {
            v @ 0..=0xFA => Ok(u64::from(v)),
            0xFC => Ok(u64::from(self.u16()?)),
            0xFD => Ok(u64::from(self.u24()?)),
            0xFE => self.u64(),
            _ => Err(Error::Malformed),
        }
    }

    /// length-encoded string。
    pub fn lenenc_str(&mut self) -> Result<&'a [u8], Error> {
        let len = self.lenenc_int()?;
        let len = usize::try_from(len).map_err(|_| Error::Truncated)?;
        self.bytes(len)
    }

    /// NUL で終わる文字列 (NUL は消費して返り値に含めない)。NUL が無ければ `Malformed`。
    pub fn null_str(&mut self) -> Result<&'a [u8], Error> {
        let rest = &self.buf[self.pos..];
        let end = rest.iter().position(|&b| b == 0).ok_or(Error::Malformed)?;
        self.pos += end + 1;
        Ok(&rest[..end])
    }

    /// NUL か末尾までの文字列 (NUL があれば消費する)。末尾の NUL を省くサーバーがあるため。
    pub fn null_or_eof_str(&mut self) -> &'a [u8] {
        match self.null_str() {
            Ok(s) => s,
            Err(_) => self.rest(),
        }
    }
}

/// UTF-8 として読む。
pub fn utf8(b: &[u8]) -> Result<String, Error> {
    String::from_utf8(b.to_vec()).map_err(|_| Error::Malformed)
}
