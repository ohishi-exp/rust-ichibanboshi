//! `/defaults` が返す接続の既定値と、その応答に付ける CORS の判断 (Refs #296)。
//!
//! ## なぜ中継が持つのか
//!
//! ブラウザ側の画面 (`nuxt-dtako-admin` の `/remote-app`) は、宛先・ドメイン・
//! RemoteApp のエイリアスを利用者に打たせていた。**宛先は中継が `--allow` で既に
//! 知っている**ので、画面がもう一度持つと二重管理になり、ズレた瞬間に
//! 「許可されていない宛先」で黙って閉じられる。配置先ごとの値を site の env 1 か所
//! (`/etc/ichibanboshi/rdp-relay.env`) に集め、画面はそれを読んで初期値に入れる。
//!
//! ドメインと RemoteApp は RDP セッションの中身なので中継からは見えない。**中継が
//! 「配る」だけで、使うのはブラウザ**という役割分担になる。
//!
//! ## CORS
//!
//! 画面は別オリジン (`https://dtako.ippoan.org`) から cookie 付きで読む。
//! **credentials 付きの要求に `*` は使えない**ので、許可した origin をそのまま
//! echo する。前段の Cloudflare Access は origin 応答に CORS ヘッダを足さない
//! (実測: preflight は 403 で落ちる) ので、中継が自分で付ける。
//!
//! ここは純粋関数だけ。JWT の検証も I/O も `bin/rdp_relay.rs` 側が持つ。

use serde::Serialize;

/// 画面に配る既定値。**資格情報は入れない** (利用者ごとに違うため)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RdpDefaults {
    /// 繋ぎ先。`--allow` の先頭 (= 唯一の宛先) をそのまま渡す。
    pub destination: String,
    /// Windows のドメイン。未設定なら空文字 (画面は空欄のままにする)。
    pub domain: String,
    /// publish 済み RemoteApp のエイリアス (`||ALIAS`)。空ならフルデスクトップ。
    pub remote_app: String,
}

/// 既定値を組み立てる。
///
/// `allow` が空のまま起動することは無い (`serve` が拒否する) が、ここは総関数に
/// しておく — 空なら宛先も空で返し、画面側は「未設定」として扱える。
pub fn build_defaults(
    allow: &[String],
    domain: Option<&str>,
    remote_app: Option<&str>,
) -> RdpDefaults {
    RdpDefaults {
        destination: allow.first().cloned().unwrap_or_default(),
        domain: domain.unwrap_or_default().to_string(),
        remote_app: remote_app.unwrap_or_default().to_string(),
    }
}

/// 応答に付ける `Access-Control-Allow-Origin` を決める。
///
/// 許可一覧に無い origin には**何も付けない** — ブラウザ側で読めなくなるだけで、
/// 応答そのものは Access を通った利用者にしか届かない。
pub fn cors_allow_origin(origin: Option<&str>, allowed: &[String]) -> Option<String> {
    let origin = origin?;
    allowed.iter().find(|a| a.as_str() == origin).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn defaults_take_the_destination_from_the_allowlist() {
        let d = build_defaults(
            &v(&["10.0.0.1:3389", "10.0.0.2:3389"]),
            Some("OHISHI"),
            Some("||APP"),
        );
        assert_eq!(d.destination, "10.0.0.1:3389");
        assert_eq!(d.domain, "OHISHI");
        assert_eq!(d.remote_app, "||APP");
    }

    #[test]
    fn defaults_are_empty_when_nothing_is_configured() {
        let d = build_defaults(&[], None, None);
        assert_eq!(
            d,
            RdpDefaults {
                destination: String::new(),
                domain: String::new(),
                remote_app: String::new()
            }
        );
    }

    #[test]
    fn defaults_serialize_with_snake_case_keys() {
        let d = build_defaults(&v(&["10.0.0.1:3389"]), Some("OHISHI"), Some("||APP"));
        let json = serde_json::to_string(&d).unwrap();
        assert!(json.contains("\"remote_app\":\"||APP\""), "{json}");
    }

    #[test]
    fn defaults_can_be_cloned_and_shown() {
        // derive した Clone / Debug / PartialEq も 100% gate の対象になるので通しておく。
        let d = build_defaults(&v(&["10.0.0.1:3389"]), Some("OHISHI"), None);
        assert_eq!(d.clone(), d);
        assert!(format!("{d:?}").contains("OHISHI"), "{d:?}");
    }

    #[test]
    fn cors_echoes_an_allowed_origin() {
        let allowed = v(&["https://a.example", "https://b.example"]);
        assert_eq!(
            cors_allow_origin(Some("https://b.example"), &allowed),
            Some("https://b.example".to_string())
        );
    }

    #[test]
    fn cors_refuses_an_unknown_origin() {
        assert_eq!(
            cors_allow_origin(Some("https://evil.example"), &v(&["https://a.example"])),
            None
        );
    }

    #[test]
    fn cors_refuses_a_request_without_an_origin() {
        assert_eq!(cors_allow_origin(None, &v(&["https://a.example"])), None);
    }
}
