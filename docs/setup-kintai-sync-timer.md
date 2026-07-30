# 勤怠 sync の systemd timer 設置手順

Refs #205 実装計画 07「systemd timer で無人化」

## 目的

`ichibanboshi sync`（04〜06 で実装する「打刻の push → 差分があれば再計算」を束ねた
サブコマンド）を **1 日 1 回、当月と前月について無人で回す**。

前月も回すのは、月初に前月末の打刻が遅れて確定することがあるため。当月だけだと
月をまたいだ時点で前月末の勤務が古い値のまま残ってしまう。

## 前提

- 対象ホスト: `ohishi-data`（`/opt/ichibanboshi/` にインストール、`ubuntu` 所有）
- バイナリ `ichibanboshi` は CI（`ci.yml` の `deploy` job）または `./deploy.sh` が
  配る。**`scripts/deploy-remote.sh` はバイナリしか転送しない**（scp → `mv` →
  PathModified で restart）。unit ファイル・config・本手順のラッパーは
  **すべてホスト側に手で置く運用**で、既存の `ichibanboshi.service` も同じ扱い。
- したがって `deploy/` 配下の 3 ファイルを更新したら、**手で配り直さないと
  ホストには反映されない**。CI は関与しない。

## 置くファイル

| repo | ホスト側 | 備考 |
|---|---|---|
| `deploy/ichibanboshi-sync.sh` | `/opt/ichibanboshi/ichibanboshi-sync.sh` | 実行月を算出するラッパー。要 `chmod +x` |
| `deploy/ichibanboshi-sync.service` | `/etc/systemd/system/ichibanboshi-sync.service` | `Type=oneshot`、当月 → 前月の 2 回 |
| `deploy/ichibanboshi-sync.timer` | `/etc/systemd/system/ichibanboshi-sync.timer` | 毎日 04:15、`Persistent=true` |

`ichibanboshi.toml`（`/opt/ichibanboshi/ichibanboshi.toml`）は既に置かれている前提。
sync が読む設定項目は 04〜06 の実装次第なので、**設置前に必要な項目
（kintai / Supabase 接続系）がホスト側の toml に入っているかを確認すること**。
repo の `deploy/ichibanboshi.toml` は実値を含まない雛形。

## 手順

### 0. ホストのタイムゾーンを確認する（必須）

timer の `OnCalendar=` は**ホストの system timezone で解釈される**。unit の
`Environment=TZ=Asia/Tokyo` は service の中身にしか効かず、起動時刻には効かない。
ホストが UTC のままだと 04:15 UTC = 13:15 JST となり、業務時間のど真ん中で走る。

```bash
ssh ubuntu@ohishi-data.tailea945d.ts.net
timedatectl        # Time zone: Asia/Tokyo (JST, +0900) であること
```

`Asia/Tokyo` でなければ、`sudo timedatectl set-timezone Asia/Tokyo` にするか、
`OnCalendar=` の時刻を JST 04:15 相当（UTC なら `19:15`）に書き換える。
書き換える場合は timer 内のコメントも合わせて直すこと。

### 1. ファイルを配る

repo のあるマシンから:

```bash
cd rust-ichibanboshi
scp deploy/ichibanboshi-sync.sh \
    ubuntu@ohishi-data.tailea945d.ts.net:/opt/ichibanboshi/ichibanboshi-sync.sh
scp deploy/ichibanboshi-sync.service deploy/ichibanboshi-sync.timer \
    ubuntu@ohishi-data.tailea945d.ts.net:/tmp/
```

ホスト側で:

```bash
chmod +x /opt/ichibanboshi/ichibanboshi-sync.sh
sudo mv /tmp/ichibanboshi-sync.service /tmp/ichibanboshi-sync.timer /etc/systemd/system/
sudo chown root:root /etc/systemd/system/ichibanboshi-sync.{service,timer}
sudo chmod 644 /etc/systemd/system/ichibanboshi-sync.{service,timer}
```

`/etc/systemd/system/` への書き込みには sudo が要る（`/opt/ichibanboshi/` は
`ubuntu` 所有なので sudo 不要、という既存の話とは別）。

### 2. dry-run で 1 回手で回す（有効化の前に必ず）

**timer を enable する前に、`--apply` 無しで当月・前月を 1 回ずつ手で回す。**
`--apply` が無ければ書き込みは起きない（既定は書かない）。ここで見るのは
「月が正しく算出されているか」「上流に届くか」「未知の state で落ちないか」。

```bash
# 当月 — 出力 1 行目に month=YYYY-MM が出る。今日の JST の月と一致すること
sudo -u root /opt/ichibanboshi/ichibanboshi-sync.sh current
echo "exit=$?"    # 0 なら成功

# 前月
sudo -u root /opt/ichibanboshi/ichibanboshi-sync.sh previous
echo "exit=$?"
```

- 終了コードが非 0 なら**そこで止める**。上流に未知の state が来た等の想定外は
  非 0 で落ちる仕様なので、原因を潰すまで timer は有効化しない。
- ラッパーを直に叩いているので、systemd が実行するのと同じ月の算出・同じ
  バイナリ・同じ config を通る（違うのは `--apply` の有無だけ）。
- `--apply` 付きの単発実行を先に試したい場合は、上の 2 コマンドに `--apply` を
  足す。timer で流し始める前に一度は書き込みまで通しておくと、初回の無人実行で
  初めて書き込み系のエラーを踏む事故を避けられる。

月初（1 日〜数日）にこの手順を踏む場合は、`current` と `previous` が別の月に
なることを目視で確認しておくとラッパーの検証になる。

### 3. timer を有効化する

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ichibanboshi-sync.timer
```

`enable` するのは **`.timer` のほう**。`.service` には `[Install]` が無いので
enable できない（意図的にそうしてある。boot ごとに走らせないため）。

### 4. 動作確認

```bash
# 次回起動時刻。LEFT / NEXT が JST の翌 04:15 前後（+ 最大 5 分のばらつき）になること
systemctl list-timers ichibanboshi-sync.timer --all

# timer 自体の状態
systemctl status ichibanboshi-sync.timer

# 手で 1 回起動して本番と同じ経路（--apply 付き）を通す
sudo systemctl start ichibanboshi-sync.service

# ログ。ExecStart 2 本ぶんの "ichibanboshi sync: selector=... month=..." が出る
journalctl -u ichibanboshi-sync -n 100 --no-pager

# 直近の実行結果だけ見る
systemctl status ichibanboshi-sync.service
```

`RandomizedDelaySec=300` を入れてあるので、`list-timers` の NEXT は 04:15 ちょうど
ではなく 04:15〜04:20 の間に出る。これは正常。

翌朝、無人で走ったことを確認する:

```bash
journalctl -u ichibanboshi-sync --since yesterday --no-pager
systemctl show ichibanboshi-sync.timer -p LastTriggerUSec
```

### 5. 失敗したときの見かた

- `Type=oneshot` で `ExecStart=` が 2 本。**当月 → 前月の順**に実行し、当月が
  非 0 で落ちるとそこで打ち切って unit が `failed` になる（前月は走らない）。
  前月だけが落ちた場合、当月は済んでいる。
- `Restart=` は付けていない。リトライは翌日の timer 起動が兼ねる。同じ月を
  やり直すので、1 日ぶん遅れるだけで取りこぼしにはならない。
- `TimeoutStartSec=1800`（ExecStart 2 本の合計）。超えると SIGTERM で打ち切られ
  `failed` になる。上流が固まったまま次の起動を待たせないための上限。
- 失敗が続く場合の切り分けは、まず手で dry-run（手順 2）に戻る。

```bash
systemctl status ichibanboshi-sync.service     # Result= と終了コード
journalctl -u ichibanboshi-sync -p err --no-pager
```

## ロールバック

無人実行だけ止める（ファイルは残す）:

```bash
sudo systemctl disable --now ichibanboshi-sync.timer
systemctl list-timers ichibanboshi-sync.timer --all   # 消えていること
```

`--now` を付けると停止と無効化を同時に行う。**実行中のジョブがあれば別途止める**:

```bash
sudo systemctl stop ichibanboshi-sync.service
```

完全に撤去する:

```bash
sudo systemctl disable --now ichibanboshi-sync.timer
sudo rm /etc/systemd/system/ichibanboshi-sync.timer \
        /etc/systemd/system/ichibanboshi-sync.service
rm /opt/ichibanboshi/ichibanboshi-sync.sh
sudo systemctl daemon-reload
sudo systemctl reset-failed ichibanboshi-sync.service   # failed 状態が残っていれば
```

いずれも `ichibanboshi.service`（API 本体）には触らない。sync を止めても API は
動き続ける。

## 設計上の選択と、まだ確かめていないこと

### なぜラッパースクリプトを挟むか

systemd の `ExecStart=` はシェルを介さないので `$(date +%Y-%m)` を展開しない。
`/bin/sh -c` を挟む案もあるが、`deploy/ichibanboshi-sync.sh` を置く形を選んだ。

1. unit ファイル中の `%` は systemd の指示子なので `%%` にエスケープが要る。
   `date +%%Y-%%m` は読みにくく、書き間違えても systemd はエラーにせず別の
   文字列を渡すだけで、静かに壊れる。
2. 前月の算出が 1 行に収まらない。`date -d 'last month'` は月末に壊れる
   （**実測**: `2026-07-31` の `last month` は `2026-07`。`2026-06-31` が
   `2026-07-01` に正規化されるため）。月初を経由する必要があり、その式を
   ExecStart に押し込むと 1. のエスケープと掛け算になる。
3. dry-run のリハーサル（手順 2）と systemd が同じコードを通る。ExecStart に
   直書きすると、手で回すときだけ別の式になって月の算出そのものを検証できない。

`--apply` はラッパーではなく **unit の ExecStart 側**に書いてある。「書く／書かない」
がラッパーの中に隠れないようにするため。ラッパーは月を決めて引数を素通しするだけ。

### 04:15 を選んだ理由

- 前日分の打刻が出そろうのを待つ。運行が日をまたぐので 0〜3 時台では早い。
- 【推測】デジタコ（theearth）の scrape と R2 への取り込みは
  `dtako-scraper-relay` が夜間に回している。本 repo からはその実行時刻を読めない
  ので「深夜のどこか」までしか分からないが、少なくともその後ろに置きたい。
  **relay 側の時刻が判明したら、確実に後ろになるよう調整すること。**
- 04:00 / 03:00 のような丸い時刻は他の cron / timer（logrotate 等）が集まるので
  15 分ずらしている。
- 朝の業務開始までに終わっていること。上流を叩くので日中は避けたい。

`RandomizedDelaySec=300` は、上流（社内 MariaDB / CakePHP / alc API / Supabase）に
毎日同じ秒に当たりにいくのを避けるための保険。この timer 単体で thundering herd は
起きないが、「04:15 に終わっている」ことを期待する下流が無いので実質ノーコスト。

`Persistent=true` は、ホストが止まっていた分を起動後に 1 回だけ埋めるため。勤怠は
「毎日必ず 1 回は追い付く」ことが要件なので必須。

### タイムゾーンの扱い

- `.service` に `Environment=TZ=Asia/Tokyo` を明示した。「当月」は JST の当月で
  なければならず、UTC で算出すると毎月 1 日の 00:00〜09:00 JST が前月に化けて
  月初の当月分 sync が空振りする。ラッパー側にも既定値を持たせて二重化してある。
- `.timer` の `OnCalendar=` には TZ を書いていない。systemd 252 以降なら
  `OnCalendar=*-*-* 04:15:00 Asia/Tokyo` と直接書けるが、ohishi-data の systemd
  version が確認できておらず（Ubuntu 22.04 の systemd 249 は非対応で、書くと
  unit が読めなくなる）、代わりに手順 0 のホスト設定確認に倒した。
  **`systemctl --version` が 252 以上だと確認できたら、そちらに寄せてよい。**

### 未確認・推測

- 【推測】sync が `/opt/ichibanboshi/data.db`（SQLite）を直接触る実装になった場合、
  稼働中の `ichibanboshi.service` と書き込みが競合しうる。unit は `After=` だけで
  `Requires=` / `Conflicts=` は張っていない。04〜06 の実装が固まった時点で、
  WAL で足りるか排他が要るかを確認すること。
- `TimeoutStartSec=1800` は根拠のある実測値ではなく余裕をみた値。実際の所要時間が
  分かったら詰める。逆に 30 分で足りないなら、それ自体が上流の異常のサイン。
- 前月ぶんは毎日走らせる。差分が無ければ再計算は起きない想定なので、月の後半は
  実質 no-op のはず【推測】。無視できないコストになるなら、ラッパー側で
  「月の前半だけ前月を回す」等に絞る余地がある。
- 失敗を人に通知する仕組みは入れていない（journal に残るだけ）。継続的に落ちても
  誰も気付かない可能性がある。必要なら `OnFailure=` で通知 unit を足す。
