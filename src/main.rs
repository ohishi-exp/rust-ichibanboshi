use clap::{Parser, Subcommand};
use rust_ichibanboshi::config::{AppArgs, Config};

// CLI の最上位。
//
// **`AppArgs` はそのまま平たく取り込む** (`#[command(flatten)]`)。サブコマンドを
// `Option` にしてあるので、省略時は従来どおり HTTP サーバーとして起動する —
// `deploy/ichibanboshi.service` の `--console --config ...` も `Dockerfile` の
// `CMD ["ichibanboshi", "--console"]` も 1 バイトも変えずに動く。
//
// doc コメント (`///`) にしないのは、clap が `--help` の説明文として拾ってしまい、
// `#[command(about)]` の 1 行を押しのけるため。
#[derive(Parser, Debug, Clone)]
#[command(name = "ichibanboshi")]
#[command(about = "一番星 売上データ API — SQL Server bridge")]
struct Cli {
    #[command(flatten)]
    args: AppArgs,

    /// 一度きり実行して終わるバッチ。省略すると HTTP サーバーになる。
    #[command(subcommand)]
    command: Option<Command>,
}

/// 勤怠を Supabase へ畳んで持つためのバッチ (Refs #205 実装計画 04〜06)。
#[derive(Subcommand, Debug, Clone)]
enum Command {
    /// 打刻を kintai.kintai_events へ push する (04)
    Push(BatchArgs),
    /// kosoku.rs の出力を畳んで保存する (05)
    Recalc(BatchArgs),
    /// push と再計算を 1 回で回す (06)。systemd timer が呼ぶのはこれ
    Sync(BatchArgs),
}

#[derive(clap::Args, Debug, Clone)]
struct BatchArgs {
    /// 対象月 (`YYYY-MM`)
    #[arg(long)]
    month: String,

    /// 1 名だけに絞る (乗務員CD)。省略すると全乗務員
    #[arg(long)]
    driver: Option<u64>,

    /// **実際に書き込む。** 付けない限り 1 行も書かない (既定は dry-run)
    #[arg(long, default_value_t = false)]
    apply: bool,
}

/// 想定内に終わったが、入力に想定外があった。
///
/// 1 (一般の失敗) と区別する — systemd の journal で「落ちた」のか
/// 「走ったが上流に知らない値が来た」のかが読めるようにするため。
const EXIT_UNEXPECTED_INPUT: i32 = 3;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if let Some(command) = cli.command {
        return run_batch(command, cli.args);
    }

    let args = cli.args;
    if args.console {
        run_console(args)
    } else {
        #[cfg(windows)]
        {
            let _ = args;
            rust_ichibanboshi::service::run_service().map_err(|e| {
                eprintln!("Failed to start as service: {e}");
                eprintln!("Hint: Use --console flag to run in console mode");
                Box::new(e) as Box<dyn std::error::Error>
            })
        }
        #[cfg(not(windows))]
        {
            run_console(args)
        }
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ichibanboshi=info,rust_ichibanboshi=info".into()),
        )
        .init();
}

fn run_console(args: AppArgs) -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let config = Config::from_args_and_file(&args)?;

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let shutdown_trigger = shutdown.clone();

        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown_trigger.cancel();
        });

        rust_ichibanboshi::server::run(config, shutdown).await
    })?;

    Ok(())
}

/// バッチを 1 回走らせて終わる。
fn run_batch(command: Command, args: AppArgs) -> Result<(), Box<dyn std::error::Error>> {
    use rust_ichibanboshi::kintai_fold::{recalc_month, sync_month};
    use rust_ichibanboshi::kintai_push::{push_month, KintaiPgStore, PushOptions};
    use rust_ichibanboshi::server::{build_kintai_events_repo, build_kosoku_params};

    init_tracing();
    let config = Config::from_args_and_file(&args)?;
    // テナントが 2 つの値に割れたまま書き始めない (#205「決着済み」)
    config
        .kintai_push
        .validate(&config.kintai_events.tenant_id)?;
    // 受け口は X-Tenant-ID から取るので tenant_id は任意だが、CLI にヘッダは無い。
    // 既定のテナントへ落とさず、ここで止める
    if config.kintai_push.tenant_id.trim().is_empty() {
        return Err("[kintai_push] tenant_id は push/recalc/sync に必須です \
                    (受け口だけなら X-Tenant-ID から取るので省略できます)"
            .into());
    }

    let batch = match &command {
        Command::Push(b) | Command::Recalc(b) | Command::Sync(b) => b.clone(),
    };
    let opts = PushOptions {
        month: batch.month.clone(),
        driver: batch.driver,
        apply: batch.apply,
    };
    if !opts.apply {
        println!("[dry-run] --apply が無いので 1 行も書きません");
    }

    let params = build_kosoku_params(&config);

    // **書き先を先に繋いでから読み先を組む** (Refs #205 の G6)。MariaDB が無い形態
    // では打刻の読み返しがこの pool を共有するので、server.rs と同じ順序にする —
    // 順序が違うと「画面と CLI で読み先が違う」が生まれる
    let rt = tokio::runtime::Runtime::new()?;
    let store = std::sync::Arc::new(rt.block_on(KintaiPgStore::connect(&config.kintai_push))?);
    let (repo, backend) = build_kintai_events_repo(&config, Some(store.clone()))?;
    println!("kintai events backend: {backend}");

    let unexpected = rt.block_on(async move {
        let unexpected = match command {
            Command::Push(_) => {
                let r = push_month(&repo, &store, &opts).await?;
                print_push(&r);
                r.has_unexpected()
            }
            Command::Recalc(_) => {
                let r = recalc_month(&repo, &store, &params, &opts.month, opts.driver, opts.apply)
                    .await?;
                print_fold(&r);
                !r.skipped.is_empty()
            }
            Command::Sync(_) => {
                let r = sync_month(&repo, &store, &params, &opts).await?;
                print_push(&r.push);
                print_fold(&r.fold);
                r.has_unexpected()
            }
        };
        Ok::<bool, rust_ichibanboshi::kintai_push::KintaiPushError>(unexpected)
    })?;

    if unexpected {
        eprintln!("想定外の入力がありました (上のログを確認してください)");
        std::process::exit(EXIT_UNEXPECTED_INPUT);
    }
    Ok(())
}

fn print_push(r: &rust_ichibanboshi::kintai_push::PushReport) {
    println!("push: 乗務員 {} 名 / 生行 {} 件", r.drivers, r.rows_read);
    println!(
        "push: 書いた打刻 {} 件 (PK 重複で捨てた {} 件)",
        r.events_pushed, r.deduped
    );
    println!(
        "push: 差分 {} 日 / 削除 {} 日 / 変化なし {} 日",
        r.days_changed, r.days_deleted, r.days_unchanged
    );
    for (reason, n) in &r.rejected {
        println!("push: 読み飛ばし {reason:?} {n} 件");
    }
    if !r.unknown_states.is_empty() {
        println!("push: DDL の CHECK に無い state: {:?}", r.unknown_states);
    }
}

fn print_fold(r: &rust_ichibanboshi::kintai_fold::FoldReport) {
    println!(
        "recalc: 乗務員 {} 名 (書いた {} / 指紋一致で据え置き {})",
        r.drivers, r.drivers_written, r.drivers_unchanged
    );
    println!(
        "recalc: shifts {} / day_summaries {} / day_parts {}",
        r.shifts, r.day_summaries, r.day_parts
    );
    for s in &r.skipped {
        println!("recalc: 写せなかった行 {s:?}");
    }
}
