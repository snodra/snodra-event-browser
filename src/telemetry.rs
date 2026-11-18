//! Logs and metrics, as the hub and the services do them: JSON lines on stdout filtered by
//! `RUST_LOG` (default `info`), and metrics pushed over OTLP gRPC to `SNODRA_OTLP_ENDPOINT`
//! (ADR 0026; every 60 s, `OTEL_METRIC_EXPORT_INTERVAL` changes it). The resource is the
//! identity Prometheus labels the series with: `service.name` `snodra-event-browser` and
//! `service.instance.id` the instance id the hub sees. Without an endpoint nothing is exported
//! (the library's metrics and the lag gauge are recorded on a no-op meter).

use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use snodra_messaging::{Config, MessagingError};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Owns the meter provider; dropping it exports what is pending.
pub struct TelemetryGuard(Option<SdkMeterProvider>);

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.0.take()
            && let Err(e) = provider.shutdown()
        {
            tracing::debug!(error = %e, "meter provider shutdown");
        }
    }
}

/// Installs the JSON log subscriber.
pub fn init_logs() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().json())
        .init();
}

/// The resource of `config`'s instance.
pub fn resource(config: &Config) -> Resource {
    Resource::builder_empty()
        .with_service_name(config.service_name.clone())
        .with_attribute(KeyValue::new(
            "service.instance.id",
            config.instance_id.clone(),
        ))
        .build()
}

/// Installs the global OTLP meter provider when `endpoint` is set; inside the runtime, before
/// the library's builder (which records on the global provider as it is then).
pub fn init_metrics(
    config: &Config,
    endpoint: Option<&str>,
) -> Result<TelemetryGuard, MessagingError> {
    let Some(endpoint) = endpoint else {
        return Ok(TelemetryGuard(None));
    };
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .build()
        .map_err(|e| MessagingError::Transport(format!("OTLP metric exporter: {e}")))?;
    let provider = SdkMeterProvider::builder()
        .with_resource(resource(config))
        .with_periodic_exporter(exporter)
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());
    tracing::info!(endpoint, "exporting metrics over OTLP");
    Ok(TelemetryGuard(Some(provider)))
}
