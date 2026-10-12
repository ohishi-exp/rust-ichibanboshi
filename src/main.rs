use clap::Parser;
use rust_ichibanboshi::config::{AppArgs, Config};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = AppArgs::parse();

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
