# 一番星 RemoteApp のブラウザ内描画を Rust 化する計画 (guacd → IronRDP/WASM)

`ohishi-srv` の RemoteApp「一番星 運送業システム」を**ブラウザの中で描画**する仕組みの、
中継部分を Rust に置き換える計画。`.rdp` を落として mstsc で開くのは要件を満たさない前提。

現行は Apache Guacamole (guacd = FreeRDP, C) で**すでに本番運用できる状態**にある。
構築物と運用手順は作業機 `mini-ryzen` の `~/guacamole/README.md`。

## 背景・要件

- Microsoft の RD Web Client は RemoteApp のウィンドウ管理に修正不能なバグがあり使えない
  (最初のウィンドウが `windowId: null` 扱いになり、2 枚目を閉じるとセッション全体が操作不能。
  最新 2.1.65.2 まで RemoteApp/RAIL の修正なし)
- AVD も `frame-ancestors 'self'` で埋め込み不可。移行しても解決しない
- Apache Guacamole なら動く。実機で確認済み

Rust 化で得られるもの:

- サーバー側の中継 (guacd) が消え、3 コンテナ → 静的配信 + トンネルだけになる
- `X-Frame-Options` に縛られず iframe 埋め込みが自由になる (RD Web Client が使えなかった理由の一つ)

**いま困っていることは無い。**Guacamole で要件は満たせている。つまりこれは「動いているものを
置き換える」話であり、**途中でやめても損をしない設計にする**のが前提。

## 外せない受け入れ条件

Guacamole 側で実測して確定したもの。Rust 版もこれを満たさなければ置き換える意味がない。

| # | 条件 | 備考 |
|---|---|---|
| 1 | RemoteApp が表示される | `\|\|CAPE_UNYU` |
| 2 | **子画面を開いて閉じてもセッションが操作不能にならない** | RD Web Client が壊れていた一点 |
| 3 | 3 枚目のウィンドウ (印刷プレビュー) も出る | 2 枚目までで満足しないこと |
| 4 | 日本語入力 (IME の切り替えを含む) | ローカル IME が `半角/全角` を食う問題は Rust 版でも再来する |
| 5 | 印刷 | 一番星は `Microsoft Print to PDF` を使う。手元に落ちる経路の実体を先に確認 |
| 6 | クリップボード | secure context (HTTPS) 必須なのは Rust 版でも同じ |
| 7 | ログイン 1 回 | |

## 地形 (2026-08-17 に clone した master = `1723385` で実地確認)

**前任者の調査は上流の動きより前のスナップショットで、3 点が既に古い。**
以下は clone した実物を読んで確認した内容。行番号は当時のもの。

| 項目 | 実態 |
|---|---|
| Rust の RDP 実装 | 実質 **IronRDP 一択** (rdp-rs は 5 年塩漬け)。ここは変わらず |
| `ironrdp-rail` | **ワイヤ型のみ**で正しい。`lib.rs` は 17 行 (`CHANNEL_NAME` + `pdu` 再エクスポート)、`pdu.rs` 2573 行。crate 自体が **v0.1.0 = 新設されたばかり** |
| **RAIL のケイパビリティ交換** | **既に通っている。**`ironrdp-connector` に `remote_application_mode` / `rail_support_level` があり (`src/lib.rs:239-244`)、`connection_activation.rs` が Rail/WindowList のケイパビリティを処理する。**`ironrdp-web` も `RailSupportLevel::SUPPORTED` を広告済み** (`src/session.rs:1497`) |
| **なぜ映らないのか** | 依存が無いからではなく、**`remote_application_mode: false` にハードコードされているから** (`ironrdp-web/src/session.rs:1496`) |
| チャネル経路 | **前任者の図 (`svc→rail→session→web`) は誤り。**`ironrdp-session` は `ironrdp-rail` に依存していない。実際の配線は `ironrdp-connector` と `ironrdp-web` に直接入っている |
| **RAIL クライアント実装** | **既にある。**`ironrdp-client/src/rail.rs` 659 行 — 状態機械 (`RailState`)、`RailInputEvent` / `RailControlEvent` / `RailEvent`、キュー、`SvcProcessor` 実装。`ironrdp-client` / `ironrdp-activex` / `ironrdp-daemon` が使用中 |
| クリップボード (cliprdr) | **実装済み。**`ironrdp-web` の feature に入っている |
| 印刷・ドライブ (rdpdr) | **実装済み。**`ironrdp-web` の feature に入っており、`Rdpdr::with_printer` / `with_printer_driver` で PostScript 仮想プリンタを生やせる (`src/lib.rs:118-124`)。README は PostScript→PDF 変換にも言及 |
| **本当の空白地帯** | **ブラウザ側のウィンドウ合成。**`RailEvent` を消費しているのは native 側 (`ironrdp-viewer/src/app.rs`、`ironrdp-rpc` の IPC) だけで、`ironrdp-web` 向けの投影コードは 1 行も無い |
| 上流の未対応 | **アイコン・マルチモニタ・DPI は最新の native 実装でも未対応** |

実装時に効く知見: **RAIL のウィンドウ ID はサーバー側 HWND そのもの** (実測: メイン 590320、子ダイアログ 590274)。

参照実装の優先順位は **FreeRDP の C コードより `ironrdp-client/src/rail.rs`** を上に置く
(同じ Rust・同じ svc/connector 構造なので移植性が高い)。

## 上流が動いている前提での選択肢

master 先端は 2026-08-16。`ironrdp-rail` は v0.1.0 で、RAIL 対応は**開発元自身が今まさに前進させている最中**。
「全部自前で作る」か「Guacamole を続ける」かの二択ではない。

| 選択肢 | 内容 | 評価 |
|---|---|---|
| A. 待つ | 上流が `ironrdp-web` で `remote_application_mode` を解禁するのを待つ | 緊急性ゼロなので有力 |
| B. 空白地帯だけ書いて上流に還元 | ブラウザ側のウィンドウ合成を実装して PR を出す | 通れば保守コストが消える |
| C. fork して自前で持つ | 全部抱える | 保守コストが最大。最後の手段 |

**まず A で様子を見つつ Phase 0 を進め、Phase 1 の入口で B/C を判断する。**

## フェーズ

各フェーズは独立した PR サイズ。**Phase 0 を先頭に置くことで、以降のどこで止めても
「ブラウザ内で使える」状態が残る。**

### Phase 0 — 足場とベースライン (RAIL なし) ← 上流の動きと無関係に価値がある

現状の `ironrdp-web` をビルドし、**フルデスクトップ**で RDS に繋いで一番星を起動する。

- 接続先は既存のトンネル (`ohishi-data` 経由) をそのまま使える。RDP の口は既にある
- WASM のビルド・配信・NLA 認証・画面描画・マウス/キーボードがひととおり動くことの確認
- 受け入れ: デスクトップが映り、一番星を起動して操作できる

意義: ここまでで**条件 1 以外はほぼ満たせる**。RAIL が難航しても撤退先になる
(Guacamole 側にもデスクトップ接続を用意して同じ運用が成立することを確認済み)。

### Phase 1 — RemoteApp モードを開けて Window Order をブラウザまで運ぶ (描画はまだしない)

前任者の計画では「RAIL チャネルを一から実装する」フェーズだったが、**その大半は上流に既にある**。
実際にやることは配線とフラグ:

- `ironrdp-web/src/session.rs` の `remote_application_mode` を設定から切り替えられるようにする
- `ironrdp-client/src/rail.rs` の `RailClient` を web 側から使えるようにする
  (crate 間の置き場所は要検討。`ironrdp-client` はネイティブ向けなので、
  共有部分を切り出すか web に移植するかの判断が要る)
- RAIL の `Exec` に一番星の起動情報を渡す
- 受け入れ: `ExecResult` が成功し、**Window Order が届いてウィンドウ一覧
  (HWND / 座標 / タイトル) がブラウザのコンソールに出る**。描画はしない

**ここが撤退・方針判断ポイント。**上流が同じことをやっていれば乗る (選択肢 A)。
そうでなければ Phase 2 を自前で書いて還元を狙う (選択肢 B)。

### Phase 2 — ブラウザ側のウィンドウ合成 (**本当の空白地帯。ここが本体**)

まず**仮想デスクトップ 1 枚に描く方式** (Guacamole / FreeRDP と同じ) をやる。
個別 DOM ウィンドウ化は後回し — リスクに対して見返りが薄い。

- Window Order の Show / Hide / Move / Z オーダーを反映
- **アイコン・最小化状態・フォーカスの扱いは上流の native 実装にも無い**ので自前で設計する
- マルチモニタ・DPI も同様に未対応。今回は単一モニタ・DPI 100% に限定して逃げる
- 受け入れ: **条件 2 と 3** (子画面の開閉で固まらない / 3 枚目も出る)

### Phase 3 — 入力ルーティング

- キーボードはスキャンコード。`ja-jp` レイアウト
- `半角/全角` はローカル IME に食われてブラウザに届かない (Guacamole でも同じ。実測済み)。
  Rust 版でも**ブラウザ側にボタンを置いて注入する**必要がある。
  Guacamole 拡張として作った `ime-toolbar` が移植元になる
- 受け入れ: **条件 4**、および Alt+Tab 相当

### Phase 4 — 周辺チャネル (実装ではなく統合)

cliprdr も rdpdr も**実装済み**。残る不確実性は
「IronRDP のバックエンドトレイトをブラウザの Clipboard API / File API にどう繋ぐか」に絞られる。

- クリップボード: `ironrdp-cliprdr` のバックエンドを web 側に実装
- 印刷: `Rdpdr::with_printer` で PostScript プリンタを生やし、PDF 化してブラウザへ渡す
  (Guacamole が ghostscript でやっていることと同じ)
- 受け入れ: **条件 5, 6**

### Phase 5 — 置き換え

- 認証と接続定義 (Guacamole の `user-mapping.xml` 相当) をどう持つか
- HTTPS 配信は `tailscale serve` のまま流用できる
- 並行運用 → 切り替え → guacd 撤去

## 先に潰す未確認事項

**Phase 0 に入る前に片付ける。**

1. **上流の進捗を watch する** — 特に `ironrdp-web` で `remote_application_mode` を解禁する
   変更が出ていないか。Phase 1 着手前に必ず再確認する。
   なお `ironrdp-agent` の RAIL 監査系は Devolutions Gateway 向けで**この計画とは無関係**。
   "rail" で追うと紛れるので区別すること
2. 「PDF が手元に落ちる」経路の実体 — サーバー完結か、mstsc のドライブリダイレクト依存か。
   後者なら Phase 4 に rdpdr/drive が必須で入る
3. RDS 側の RemoteApp 構成 — `||CAPE_UNYU` の起動引数・作業ディレクトリ (RAIL の `Exec` に必要)。
   **Windows Server 側を見るだけで分かる**ので、Rust の検証を待たずに並行で潰せる

## 見積もりの感覚

上流に実装があるぶん、前任者の見積もりより Phase 1 が軽く、Phase 2 の性格が変わった
(「全部ゼロから」→「移植可能な部分 + 純粋新規のブラウザ合成」)。

| Phase | 規模 | 備考 |
|---|---|---|
| 0 | 数日 | |
| 1 | 数日〜1 週間 | 一から実装するのではなく配線 |
| 2 | **数週間。ここが本体** | ブラウザ合成は上流にも前例が無い |
| 3〜5 | 各数日〜1 週間 | Phase 4 は統合作業に格下げ |

Phase 1 の受け入れ (Window Order がブラウザまで届く) に到達するまでが、やる/やめるの判断材料。
それ以前に工数を積まないこと。
