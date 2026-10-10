//! kintai-mysql のコーデックの単体テスト。I/O は無く、バイト列を組み立てて読ませる。

use kintai_mysql::handshake::{
    auth_switch_response, handshake_response41, native_password_scramble, parse_auth_reply,
    parse_initial_handshake, AuthReply, CHARSET_UTF8MB4, CLIENT_CONNECT_WITH_DB,
    CLIENT_DEPRECATE_EOF, CLIENT_FLAGS, CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41,
    CLIENT_SECURE_CONNECTION, CLIENT_SSL, MAX_PACKET_SIZE, NATIVE_PASSWORD,
};
use kintai_mysql::packet::{
    frame, put_lenenc_int, put_lenenc_str, utf8, Packet, PacketBuf, Reader, MAX_PAYLOAD,
};
use kintai_mysql::response::{
    com_query, com_quit, is_eof, parse_column_definition, parse_query_response, parse_row, Column,
    ErrPacket, OkPacket, QueryResponse, ResultSet, ResultSetReader,
};
use kintai_mysql::Error;

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

// ── packet ──

#[test]
fn frame_adds_length_and_seq() {
    assert_eq!(
        frame(3, b"abc").unwrap(),
        vec![3, 0, 0, 3, b'a', b'b', b'c']
    );
    assert_eq!(frame(0, &[]).unwrap(), vec![0, 0, 0, 0]);
    assert_eq!(frame(0, &vec![0; MAX_PAYLOAD]), Err(Error::TooLarge));
}

#[test]
fn packet_buf_waits_for_whole_packet() {
    let mut buf = PacketBuf::new();
    assert_eq!(buf.take(), None);
    buf.extend(&[2, 0]);
    assert_eq!(buf.take(), None, "header の途中");
    buf.extend(&[0, 7, b'x']);
    assert_eq!(buf.take(), None, "body の途中");
    // 2 つ目のパケットまで一度に届く
    buf.extend(&[b'y', 1, 0, 0, 8, b'z']);
    assert_eq!(
        buf.take(),
        Some(Packet {
            seq: 7,
            payload: b"xy".to_vec()
        })
    );
    assert_eq!(
        buf.take(),
        Some(Packet {
            seq: 8,
            payload: b"z".to_vec()
        })
    );
    assert_eq!(buf.take(), None);
}

#[test]
fn packet_buf_joins_max_size_packets() {
    let mut buf = PacketBuf::new();
    let mut first = vec![0xFF, 0xFF, 0xFF, 1];
    first.extend(vec![b'a'; MAX_PAYLOAD]);
    buf.extend(&first);
    assert_eq!(buf.take(), None, "ちょうど上限の長さは続きがある印");
    buf.extend(&[2, 0, 0, 2, b'b', b'c']);
    let p = buf.take().unwrap();
    assert_eq!(p.seq, 2);
    assert_eq!(p.payload.len(), MAX_PAYLOAD + 2);
    assert_eq!(&p.payload[MAX_PAYLOAD..], b"bc");
    assert_eq!(buf.take(), None);
}

#[test]
fn lenenc_int_round_trips_every_width() {
    for (v, len) in [
        (0u64, 1usize),
        (0xFA, 1),
        (0xFB, 3),
        (0xFFFF, 3),
        (0x1_0000, 4),
        (0xFF_FFFF, 4),
        (0x100_0000, 9),
        (u64::MAX, 9),
    ] {
        let mut out = Vec::new();
        put_lenenc_int(&mut out, v);
        assert_eq!(out.len(), len, "{v:#x}");
        let mut r = Reader::new(&out);
        assert_eq!(r.lenenc_int().unwrap(), v);
        assert_eq!(r.remaining(), 0);
    }
}

#[test]
fn lenenc_int_rejects_null_and_err_markers() {
    assert_eq!(Reader::new(&[0xFB]).lenenc_int(), Err(Error::Malformed));
    assert_eq!(Reader::new(&[0xFF]).lenenc_int(), Err(Error::Malformed));
    assert_eq!(Reader::new(&[0xFC, 1]).lenenc_int(), Err(Error::Truncated));
    assert_eq!(Reader::new(&[]).lenenc_int(), Err(Error::Truncated));
}

#[test]
fn lenenc_str_round_trips() {
    let mut out = Vec::new();
    put_lenenc_str(&mut out, b"utf8mb4");
    assert_eq!(out[0], 7);
    let mut r = Reader::new(&out);
    assert_eq!(r.lenenc_str().unwrap(), b"utf8mb4");
    // 長さより中身が短い
    assert_eq!(Reader::new(&[5, b'a']).lenenc_str(), Err(Error::Truncated));
    // 8 byte 長が usize を超えても panic しない
    let mut huge = vec![0xFE];
    huge.extend_from_slice(&u64::MAX.to_le_bytes());
    assert_eq!(Reader::new(&huge).lenenc_str(), Err(Error::Truncated));
}

#[test]
fn reader_fixed_width_ints_and_strings() {
    let data = [
        0x01, 0x02, 0x01, 0x03, 0x02, 0x01, 0x04, 0x03, 0x02, 0x01, 8, 7, 6, 5, 4, 3, 2, 1, b'h',
        b'i', 0, b'x', b'y',
    ];
    let mut r = Reader::new(&data);
    assert_eq!(r.peek(), Some(1));
    assert_eq!(r.u8().unwrap(), 1);
    assert_eq!(r.u16().unwrap(), 0x0102);
    assert_eq!(r.u24().unwrap(), 0x01_0203);
    assert_eq!(r.u32().unwrap(), 0x0102_0304);
    assert_eq!(r.u64().unwrap(), 0x0102_0304_0506_0708);
    assert_eq!(r.null_str().unwrap(), b"hi");
    assert_eq!(r.null_str(), Err(Error::Malformed), "NUL が無い");
    assert_eq!(r.null_or_eof_str(), b"xy", "NUL が無ければ末尾まで");
    assert_eq!(r.remaining(), 0);
    assert_eq!(r.peek(), None);
    assert_eq!(r.rest(), b"");
    assert_eq!(r.bytes(1), Err(Error::Truncated));

    let mut r = Reader::new(b"ab\0cd");
    assert_eq!(r.null_or_eof_str(), b"ab");
    assert_eq!(r.rest(), b"cd");
}

#[test]
fn utf8_rejects_invalid_bytes() {
    assert_eq!(utf8(b"ok").unwrap(), "ok");
    assert_eq!(utf8(&[0xC3]), Err(Error::Malformed));
}

// ── handshake ──

const MARIADB_VERSION: &str = "5.5.5-10.6.18-MariaDB";
/// テスト用の nonce (サーバーが送る 20 byte の乱数の代わり。NUL を含まない)
const NONCE: &[u8; 20] = &[
    0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x08, 0x19, 0x2A, 0x3B, 0x4C, 0x5D, 0x6E, 0x7F, 0x01,
    0x12, 0x23, 0x34, 0x45,
];

/// MariaDB 10.6 が送る形の Initial Handshake を組み立てる。
fn initial_handshake(protocol: u8, caps: u32, part2: &[u8], plugin: &[u8]) -> Vec<u8> {
    let mut p = vec![protocol];
    p.extend_from_slice(MARIADB_VERSION.as_bytes());
    p.push(0);
    p.extend_from_slice(&42u32.to_le_bytes());
    p.extend_from_slice(&NONCE[..8]);
    p.push(0); // filler
    p.extend_from_slice(&(caps as u16).to_le_bytes());
    p.push(45); // charset
    p.extend_from_slice(&2u16.to_le_bytes()); // status
    p.extend_from_slice(&((caps >> 16) as u16).to_le_bytes());
    p.push(21); // auth_plugin_data_len (8 + 12 + NUL)
    p.extend_from_slice(&[0; 6]);
    p.extend_from_slice(&[0x1D, 0, 0, 0]); // MariaDB の拡張 capability (読まない)
    p.extend_from_slice(part2);
    p.extend_from_slice(plugin);
    p
}

const SERVER_CAPS: u32 = 0xF7FE | (0x81BF << 16);

#[test]
fn parses_mariadb_initial_handshake() {
    let mut part2 = NONCE[8..].to_vec();
    part2.push(0); // part2 は max(13, 21 - 8) = 13 byte で末尾が NUL
    let p = initial_handshake(10, SERVER_CAPS, &part2, b"mysql_native_password\0");
    let hs = parse_initial_handshake(&p).unwrap();
    assert_eq!(hs.server_version, MARIADB_VERSION);
    assert_eq!(hs.connection_id, 42);
    assert_eq!(hs.capabilities, SERVER_CAPS);
    assert_eq!(hs.charset, 45);
    assert_eq!(hs.status, 2);
    assert_eq!(
        hs.auth_plugin_data,
        NONCE.to_vec(),
        "part1 + part2 の NUL を除いた 20 byte"
    );
    assert_eq!(hs.auth_plugin_name, NATIVE_PASSWORD);
}

#[test]
fn initial_handshake_plugin_name_without_trailing_nul() {
    let mut part2 = NONCE[8..].to_vec();
    part2.push(0);
    let p = initial_handshake(10, SERVER_CAPS, &part2, b"mysql_native_password");
    assert_eq!(
        parse_initial_handshake(&p).unwrap().auth_plugin_name,
        NATIVE_PASSWORD
    );
}

#[test]
fn initial_handshake_failures() {
    let mut part2 = NONCE[8..].to_vec();
    part2.push(0);
    // サーバーが接続を断ると handshake の代わりに ERR が来る (sql_state 無し)
    let mut err = vec![0xFF];
    err.extend_from_slice(&1130u16.to_le_bytes());
    err.extend_from_slice(b"not allowed");
    match parse_initial_handshake(&err) {
        Err(Error::Server(e)) => {
            assert_eq!(e.code, 1130);
            assert_eq!(e.sql_state, None);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        parse_initial_handshake(&initial_handshake(9, SERVER_CAPS, &part2, b"")),
        Err(Error::ProtocolVersion(9))
    );
    let no_plugin_auth = SERVER_CAPS & !CLIENT_PLUGIN_AUTH;
    assert_eq!(
        parse_initial_handshake(&initial_handshake(10, no_plugin_auth, &part2, b"")),
        Err(Error::Capability(CLIENT_PLUGIN_AUTH))
    );
    let p = initial_handshake(10, SERVER_CAPS, &part2[..5], b"");
    assert_eq!(parse_initial_handshake(&p), Err(Error::Truncated));
    assert_eq!(parse_initial_handshake(&[]), Err(Error::Truncated));
}

#[test]
fn client_flags_are_the_four_and_not_ssl() {
    assert_eq!(
        CLIENT_FLAGS,
        CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_WITH_DB
    );
    assert_eq!(CLIENT_FLAGS & CLIENT_SSL, 0);
    assert_eq!(CLIENT_FLAGS & CLIENT_DEPRECATE_EOF, 0);
}

#[test]
fn native_password_known_vectors() {
    // 期待値は Python の hashlib で SHA1(pw) XOR SHA1(nonce + SHA1(SHA1(pw))) を計算したもの
    let nonce: Vec<u8> = (1..=20).collect();
    assert_eq!(
        native_password_scramble(b"secret", &nonce),
        hex("b32bb3a583e1340c0a1108d58b1be49781ad8c2f")
    );
    assert_eq!(
        native_password_scramble(b"root", b"abcdefghijklmnopqrst"),
        hex("5d14f4172d69b6d30da98a8c52f0911e8a019132")
    );
    // 20 byte を超える nonce (末尾 NUL 付き等) は先頭 20 byte だけを使う
    assert_eq!(
        native_password_scramble(b"root", b"abcdefghijklmnopqrst\0"),
        hex("5d14f4172d69b6d30da98a8c52f0911e8a019132")
    );
    assert!(native_password_scramble(b"", &nonce).is_empty());
}

#[test]
fn handshake_response41_layout() {
    let auth = native_password_scramble(b"pw", NONCE);
    let p = handshake_response41("reader", &auth, "kintai").unwrap();
    let mut r = Reader::new(&p);
    assert_eq!(r.u32().unwrap(), CLIENT_FLAGS);
    assert_eq!(r.u32().unwrap(), MAX_PACKET_SIZE);
    assert_eq!(r.u8().unwrap(), CHARSET_UTF8MB4);
    assert_eq!(r.bytes(23).unwrap(), &[0; 23]);
    assert_eq!(r.null_str().unwrap(), b"reader");
    let len = usize::from(r.u8().unwrap());
    assert_eq!(r.bytes(len).unwrap(), auth.as_slice());
    assert_eq!(r.null_str().unwrap(), b"kintai");
    assert_eq!(r.null_str().unwrap(), NATIVE_PASSWORD.as_bytes());
    assert_eq!(r.remaining(), 0);
}

#[test]
fn handshake_response41_rejects_bad_input() {
    assert_eq!(
        handshake_response41("a\0b", &[], "db"),
        Err(Error::Malformed)
    );
    assert_eq!(
        handshake_response41("a", &[], "d\0b"),
        Err(Error::Malformed)
    );
    assert_eq!(
        handshake_response41("a", &[0; 256], "db"),
        Err(Error::Malformed)
    );
}

fn ok_packet() -> Vec<u8> {
    vec![0x00, 0, 0, 2, 0, 0, 0]
}

#[test]
fn auth_reply_kinds() {
    assert_eq!(
        parse_auth_reply(&ok_packet()).unwrap(),
        AuthReply::Ok(OkPacket {
            affected_rows: 0,
            last_insert_id: 0,
            status: 2,
            warnings: 0
        })
    );
    let mut err = vec![0xFF];
    err.extend_from_slice(&1045u16.to_le_bytes());
    err.extend_from_slice(b"#28000Access denied");
    match parse_auth_reply(&err) {
        Err(Error::Server(e)) => {
            assert_eq!(e.code, 1045);
            assert_eq!(e.sql_state.as_deref(), Some("28000"));
        }
        other => panic!("{other:?}"),
    }
    let mut switch = vec![0xFE];
    switch.extend_from_slice(b"mysql_native_password\0");
    switch.extend_from_slice(NONCE);
    switch.push(0);
    assert_eq!(
        parse_auth_reply(&switch).unwrap(),
        AuthReply::Switch {
            plugin: NATIVE_PASSWORD.to_string(),
            data: NONCE.to_vec()
        }
    );
    // 末尾 NUL の無いデータ
    let mut switch = vec![0xFE];
    switch.extend_from_slice(b"client_ed25519\0xyz");
    assert_eq!(
        parse_auth_reply(&switch).unwrap(),
        AuthReply::Switch {
            plugin: "client_ed25519".to_string(),
            data: b"xyz".to_vec()
        }
    );
    assert_eq!(
        parse_auth_reply(&[0xFE]),
        Err(Error::Unexpected(0xFE)),
        "旧形式への切り替え"
    );
    assert_eq!(
        parse_auth_reply(&[0x01, 4]),
        Err(Error::Unexpected(0x01)),
        "追加データ"
    );
    assert_eq!(parse_auth_reply(&[]), Err(Error::Truncated));
    assert_eq!(
        parse_auth_reply(&[0xFE, b'x']),
        Err(Error::Malformed),
        "プラグイン名に NUL が無い"
    );
}

#[test]
fn auth_switch_only_native_password() {
    assert_eq!(
        auth_switch_response(NATIVE_PASSWORD, b"abcdefghijklmnopqrst", b"root").unwrap(),
        hex("5d14f4172d69b6d30da98a8c52f0911e8a019132")
    );
    assert_eq!(
        auth_switch_response("caching_sha2_password", NONCE, b"root"),
        Err(Error::AuthPlugin("caching_sha2_password".to_string()))
    );
}

// ── response ──

#[test]
fn err_packet_parse() {
    let mut p = vec![0xFF];
    p.extend_from_slice(&1146u16.to_le_bytes());
    p.extend_from_slice(b"#42S02Table doesn't exist");
    assert_eq!(
        ErrPacket::parse(&p).unwrap(),
        ErrPacket {
            code: 1146,
            sql_state: Some("42S02".to_string()),
            message: "Table doesn't exist".to_string()
        }
    );
    assert_eq!(ErrPacket::parse(&ok_packet()), Err(Error::Unexpected(0)));
    assert_eq!(ErrPacket::parse(&[0xFF, 1]), Err(Error::Truncated));
}

#[test]
fn ok_packet_parse() {
    let ok = OkPacket::parse(&[0x00, 0xFC, 0x00, 0x01, 5, 0x22, 0x00, 1, 0]).unwrap();
    assert_eq!(ok.affected_rows, 256);
    assert_eq!(ok.last_insert_id, 5);
    assert_eq!(ok.status, 0x22);
    assert_eq!(ok.warnings, 1);
    assert_eq!(
        OkPacket::parse(&[0xFE, 0, 0, 0, 0]),
        Err(Error::Unexpected(0xFE))
    );
    assert_eq!(OkPacket::parse(&[0x00, 0]), Err(Error::Truncated));
}

#[test]
fn eof_detection() {
    assert!(is_eof(&[0xFE, 0, 0, 2, 0]));
    assert!(
        !is_eof(&[0xFE, 0, 0, 0, 0, 0, 0, 0, 0]),
        "9 byte 以上は lenenc の行"
    );
    assert!(!is_eof(&ok_packet()));
    assert!(!is_eof(&[]));
}

#[test]
fn commands() {
    assert_eq!(com_query("SELECT 1"), b"\x03SELECT 1".to_vec());
    assert_eq!(com_quit(), vec![0x01]);
}

#[test]
fn query_response_kinds() {
    assert!(matches!(
        parse_query_response(&ok_packet()),
        Ok(QueryResponse::Ok(_))
    ));
    assert_eq!(
        parse_query_response(&[4]).unwrap(),
        QueryResponse::ResultSet(4)
    );
    let mut err = vec![0xFF];
    err.extend_from_slice(&1969u16.to_le_bytes());
    err.extend_from_slice(b"#70100Query execution was interrupted");
    match parse_query_response(&err) {
        Err(Error::Server(e)) => assert_eq!(e.code, 1969),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        parse_query_response(&[0xFB, b'f']),
        Err(Error::Unexpected(0xFB)),
        "LOCAL INFILE"
    );
    assert_eq!(parse_query_response(&[]), Err(Error::Truncated));
}

/// Protocol::ColumnDefinition41 を組み立てる。
fn column_def(name: &str, column_type: u8, charset: u16) -> Vec<u8> {
    let mut p = Vec::new();
    for s in ["def", "", "", "", name, ""] {
        put_lenenc_str(&mut p, s.as_bytes());
    }
    p.push(0x0C);
    p.extend_from_slice(&charset.to_le_bytes());
    p.extend_from_slice(&64u32.to_le_bytes());
    p.push(column_type);
    p.extend_from_slice(&0u16.to_le_bytes()); // flags
    p.push(0); // decimals
    p.extend_from_slice(&[0, 0]);
    p
}

fn text_row(cells: &[Option<&str>]) -> Vec<u8> {
    let mut p = Vec::new();
    for c in cells {
        match c {
            Some(s) => put_lenenc_str(&mut p, s.as_bytes()),
            None => p.push(0xFB),
        }
    }
    p
}

const EOF: [u8; 5] = [0xFE, 0, 0, 2, 0];

#[test]
fn column_definition_parse() {
    assert_eq!(
        parse_column_definition(&column_def("VERSION()", 0xFD, 45)).unwrap(),
        Column {
            name: "VERSION()".to_string(),
            column_type: 0xFD,
            charset: 45
        }
    );
    assert_eq!(parse_column_definition(&[3, b'd']), Err(Error::Truncated));
}

#[test]
fn row_parse() {
    assert_eq!(
        parse_row(&text_row(&[Some("1"), None, Some("")]), 3).unwrap(),
        vec![Some(b"1".to_vec()), None, Some(Vec::new())]
    );
    assert_eq!(
        parse_row(&text_row(&[Some("1"), Some("2")]), 1),
        Err(Error::Malformed)
    );
    assert_eq!(parse_row(&text_row(&[Some("1")]), 2), Err(Error::Truncated));
}

/// probe が流す `SELECT 1, VERSION(), @@character_set_connection, CURRENT_USER()` の結果セット。
#[test]
fn reads_probe_result_set() {
    let first = [4u8];
    let QueryResponse::ResultSet(n) = parse_query_response(&first).unwrap() else {
        panic!("result set のはず");
    };
    let mut rs = ResultSetReader::new(n).unwrap();
    for name in [
        "1",
        "VERSION()",
        "@@character_set_connection",
        "CURRENT_USER()",
    ] {
        assert_eq!(rs.push(&column_def(name, 0xFD, 45)).unwrap(), None);
    }
    assert_eq!(rs.push(&EOF).unwrap(), None);
    let row = text_row(&[
        Some("1"),
        Some("10.6.18-MariaDB"),
        Some("utf8mb4"),
        Some("reader@%"),
    ]);
    assert_eq!(rs.push(&row).unwrap(), None);
    let set = rs.push(&EOF).unwrap().unwrap();
    assert_eq!(set.columns.len(), 4);
    assert_eq!(set.columns[3].name, "CURRENT_USER()");
    assert_eq!(set.text(0, 0), Some("1"));
    assert_eq!(set.text(0, 1), Some("10.6.18-MariaDB"));
    assert_eq!(set.text(0, 2), Some("utf8mb4"));
    assert_eq!(set.text(0, 3), Some("reader@%"));
    assert_eq!(set.text(0, 4), None, "列の範囲外");
    assert_eq!(set.text(1, 0), None, "行の範囲外");
}

#[test]
fn result_set_text_null_and_invalid_utf8() {
    let set = ResultSet {
        columns: Vec::new(),
        rows: vec![vec![None, Some(vec![0xC3])]],
    };
    assert_eq!(set.text(0, 0), None);
    assert_eq!(set.text(0, 1), None);
}

#[test]
fn result_set_reader_errors() {
    // 列の定義の後に EOF でないものが来た
    let mut rs = ResultSetReader::new(1).unwrap();
    rs.push(&column_def("a", 3, 63)).unwrap();
    assert_eq!(rs.push(&text_row(&[Some("1")])), Err(Error::Unexpected(1)));
    assert_eq!(rs.push(&[]), Err(Error::Unexpected(0)));
    // 行の途中で ERR (max_statement_time の打ち切り等)
    let mut rs = ResultSetReader::new(1).unwrap();
    rs.push(&column_def("a", 3, 63)).unwrap();
    rs.push(&EOF).unwrap();
    let mut err = vec![0xFF];
    err.extend_from_slice(&1969u16.to_le_bytes());
    err.extend_from_slice(b"#70100interrupted");
    match rs.push(&err) {
        Err(Error::Server(e)) => assert_eq!(e.code, 1969),
        other => panic!("{other:?}"),
    }
    // 列の数と合わない行
    assert_eq!(
        rs.push(&text_row(&[Some("1"), Some("2")])),
        Err(Error::Malformed)
    );
}

#[test]
fn result_set_reader_zero_columns() {
    let mut rs = ResultSetReader::new(0).unwrap();
    assert_eq!(rs.push(&EOF).unwrap(), None);
    assert_eq!(rs.push(&EOF).unwrap(), Some(ResultSet::default()));
}

// ── Error ──

#[test]
fn error_kinds_carry_no_identifiers() {
    let server = Error::Server(ErrPacket {
        code: 1045,
        sql_state: Some("28000".to_string()),
        message: "Access denied for user 'someone'@'somewhere'".to_string(),
    });
    let cases = [
        (Error::Truncated, "truncated"),
        (Error::Malformed, "malformed"),
        (Error::ProtocolVersion(9), "protocol_version"),
        (Error::Capability(CLIENT_PLUGIN_AUTH), "capability"),
        (
            Error::AuthPlugin("caching_sha2_password".to_string()),
            "auth_plugin",
        ),
        (Error::Unexpected(1), "unexpected_packet"),
        (server, "server:1045"),
        (Error::TooLarge, "too_large"),
    ];
    for (e, want) in cases {
        assert_eq!(e.kind(), want);
    }
}
