//! `snodra-event-browser`: Snodra Server's event browser add-on (see the crate docs and the
//! README).
//!
//! - `snodra-event-browser` runs it (settings from the environment);
//! - `snodra-event-browser --health-check` probes the local browser's gRPC health service (the
//!   image's `HEALTHCHECK`) and exits 0 when it is `SERVING`, 1 otherwise;
//! - `snodra-event-browser --version` prints the version.

use std::process::ExitCode;
use std::time::Duration;

use snodra_event_browser::{SERVICE, Settings, health, telemetry};
use snodra_messaging::Config;

const USAGE: &str = "usage:\n  snodra-event-browser                 run the event browser\n  snodra-event-browser --health-check  probe the local browser's gRPC health service\n  snodra-event-browser --version       print the version";
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        [] => Mode::Run,
        ["--health-check"] => Mode::HealthCheck,
        ["--version"] => {
            println!("{SERVICE} {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    // The browser registers as `snodra-event-browser`; the library reads the service name (and
    // derives the OAuth2 client id from it) from SNODRA_SERVICE_NAME. Set before any thread
    // exists, so changing the environment is sound.
    if std::env::var_os("SNODRA_SERVICE_NAME").is_none() {
        // SAFETY: single-threaded here: no runtime or other thread has started.
        unsafe { std::env::set_var("SNODRA_SERVICE_NAME", SERVICE) };
    }
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async move {
        match mode {
            Mode::HealthCheck => health_check().await,
            Mode::Run => run().await,
        }
    })
}

enum Mode {
    Run,
    HealthCheck,
}

async fn run() -> ExitCode {
    telemetry::init_logs();
    let started = async {
        let config = Config::from_env().map_err(|e| e.to_string())?;
        let settings = Settings::from_env()?;
        Ok::<_, String>((config, settings))
    };
    let (config, settings) = match started.await {
        Ok(started) => started,
        Err(e) => {
            tracing::error!(error = %e, "invalid settings");
            return ExitCode::FAILURE;
        }
    };
    let _metrics = match telemetry::init_metrics(&config, settings.otlp_endpoint.as_deref()) {
        Ok(guard) => guard,
        Err(e) => {
            tracing::error!(error = %e, "cannot export metrics");
            return ExitCode::FAILURE;
        }
    };
    match snodra_event_browser::run(config, settings, snodra_event_browser::shutdown_signal()).await
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "event browser failed");
            ExitCode::FAILURE
        }
    }
}

async fn health_check() -> ExitCode {
    let listen = match Settings::from_env().map(|s| s.health_listen) {
        Ok(Some(listen)) => listen,
        Ok(None) => {
            println!("{SERVICE}: the health service is off (SNODRA_EVENT_BROWSER_HEALTH_LISTEN)");
            return ExitCode::FAILURE;
        }
        Err(e) => {
            println!("{SERVICE}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let addr = snodra_event_browser::probe_address(listen);
    match tokio::time::timeout(PROBE_TIMEOUT, health::probe(addr)).await {
        Ok(Ok(true)) => {
            println!("{SERVICE} at {addr}: SERVING");
            ExitCode::SUCCESS
        }
        Ok(Ok(false)) => {
            println!("{SERVICE} at {addr}: NOT_SERVING");
            ExitCode::FAILURE
        }
        Ok(Err(e)) => {
            println!("{SERVICE} at {addr}: health check failed: {e}");
            ExitCode::FAILURE
        }
        Err(_) => {
            println!("{SERVICE} at {addr}: health check timed out after {PROBE_TIMEOUT:?}");
            ExitCode::FAILURE
        }
    }
}
