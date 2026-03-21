use std::ffi::OsString;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::info;
use windows_service::service::{
    ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
use windows_service::{define_windows_service, service_dispatcher};

use crate::config::Config;
use crate::server;

const SERVICE_NAME: &str = "IchibanboshiSales";

define_windows_service!(ffi_service_main, service_main);

/// Register with Windows SCM and block until stopped
pub fn run_service() -> windows_service::Result<()> {
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)
}

fn service_main(_arguments: Vec<OsString>) {
    if let Err(e) = run_service_inner() {
        tracing::error!("Service failed: {}", e);
    }
}

fn run_service_inner() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load_default_locations()?;

    // File-based logging for service mode
    let log_dir = if config.log_dir.is_empty() {
        std::env::current_exe()?
            .parent()
            .map(|p| p.join("logs"))
            .unwrap_or_default()
    } else {
        std::path::PathBuf::from(&config.log_dir)
    };

    let file_appender = tracing_appender::rolling::daily(&log_dir, "ichibanboshi.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter("ichibanboshi=info,rust_ichibanboshi=info")
        .init();

    let shutdown = CancellationToken::new();
    let shutdown_trigger = shutdown.clone();

    let event_handler = move |control_event| -> ServiceControlHandlerResult {
        match control_event {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                shutdown_trigger.cancel();
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        }
    };

    let status_handle = service_control_handler::register(SERVICE_NAME, event_handler)?;

    // Report "Starting"
    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::StartPending,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::from_secs(10),
        process_id: None,
    })?;

    let rt = tokio::runtime::Runtime::new()?;

    // Report "Running"
    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Running,
        controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    })?;

    info!("一番星売上データAPI service started");

    let result = rt.block_on(server::run(config, shutdown));

    // Report "Stopped"
    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: match &result {
            Ok(_) => ServiceExitCode::Win32(0),
            Err(_) => ServiceExitCode::Win32(1),
        },
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    })?;

    if let Err(e) = result {
        tracing::error!("Server exited with error: {}", e);
    }

    info!("一番星売上データAPI service stopped");
    Ok(())
}
