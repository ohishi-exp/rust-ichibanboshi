use clap::Parser;
use rust_ichibanboshi::config::{AppArgs, Config};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = AppArgs::parse();

    if args.console {
        run_console(args)
    } else {
        #[cfg(windows)]
        {
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

fn run_console(args: AppArgs) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ichibanboshi=info,rust_ichibanboshi=info".into()),
        )
        .init();

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
