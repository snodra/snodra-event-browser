//! Health, as the hub does it: the gRPC health service (`grpc.health.v1.Health`, service `""`)
//! on `KRAKEN_EVENT_BROWSER_HEALTH_LISTEN`, and `kraken-event-browser --health-check` probing it
//! (the distroless image has no shell or curl). `SERVING` while at least one hub has the
//! browser registered (it can answer); `NOT_SERVING` before, while no hub is reachable, and
//! from the start of a shutdown.

use std::net::SocketAddr;

use kraken_messaging::MessagingError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;

/// The service name the status is reported under (the whole process).
const SERVICE: &str = "";

#[derive(Clone)]
pub struct Health {
    reporter: HealthReporter,
    server: std::sync::Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl Health {
    /// `SERVING` or `NOT_SERVING`.
    pub async fn set(&self, serving: bool) {
        let status = if serving {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        self.reporter.set_service_status(SERVICE, status).await;
    }

    /// Waits for the server to stop (it stops with the token given to [`serve`]).
    pub async fn stopped(self) {
        if let Some(task) = self.server.lock().await.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), task).await;
        }
    }
}

/// Serves the health service on `addr` until `stop`, `NOT_SERVING` to begin with.
pub async fn serve(addr: SocketAddr, stop: CancellationToken) -> Result<Health, MessagingError> {
    let (reporter, service) = tonic_health::server::health_reporter();
    reporter
        .set_service_status(SERVICE, ServingStatus::NotServing)
        .await;
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        MessagingError::Transport(format!(
            "cannot listen on {addr} (KRAKEN_EVENT_BROWSER_HEALTH_LISTEN): {e}"
        ))
    })?;
    tracing::info!(addr = %listener.local_addr().unwrap_or(addr), "health service listening");
    let incoming = tonic::transport::server::TcpIncoming::from(listener);
    let task = tokio::spawn(async move {
        let served = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, stop.cancelled_owned())
            .await;
        if let Err(e) = served {
            tracing::warn!(error = %e, "health service failed");
        }
    });
    Ok(Health {
        reporter,
        server: std::sync::Arc::new(tokio::sync::Mutex::new(Some(task))),
    })
}

/// `grpc.health.v1.Health/Check` of service `""` on `addr`: `Ok(true)` when `SERVING`.
pub async fn probe(addr: SocketAddr) -> Result<bool, String> {
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus as PbStatus;
    use tonic_health::pb::health_client::HealthClient;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| e.to_string())?
        .connect()
        .await
        .map_err(|e| e.to_string())?;
    let mut client = HealthClient::new(channel);
    let response = client
        .check(HealthCheckRequest {
            service: SERVICE.to_string(),
        })
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();
    Ok(response.status() == PbStatus::Serving)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_probe_reads_what_the_browser_reports() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let stop = CancellationToken::new();
        let health = serve(addr, stop.clone()).await.unwrap();
        assert_eq!(probe(addr).await, Ok(false), "not serving before a hub");
        health.set(true).await;
        assert_eq!(probe(addr).await, Ok(true));
        health.set(false).await;
        assert_eq!(probe(addr).await, Ok(false));
        stop.cancel();
        health.stopped().await;
        assert!(probe(addr).await.is_err(), "nothing listens after the stop");
    }
}
