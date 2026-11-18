//! A real server for the tests: a `snodra-server` container from the server's image
//! (`SNODRA_SERVER_IMAGE`, default `snodra-server:dev`), configured by its `SNODRA_SERVER_*`
//! variables, with its gRPC and HTTP ports mapped on the loopback. This repository depends on the
//! server's image, never on its source.
//! Needs Docker.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const GRPC: u16 = 7000;
const HTTP: u16 = 7001;

/// The server image the tests run: `SNODRA_SERVER_IMAGE`, default `snodra-server:dev`.
pub fn image() -> String {
    std::env::var("SNODRA_SERVER_IMAGE").unwrap_or_else(|_| "snodra-server:dev".into())
}

pub struct Hub {
    pub grpc: SocketAddr,
    pub http: SocketAddr,
    container: ContainerAsync<GenericImage>,
}

impl Hub {
    /// Stops the hub (SIGTERM, its graceful shutdown); the container is removed when dropped.
    pub async fn stop(&self) {
        self.container
            .stop_with_timeout(Some(10))
            .await
            .expect("the hub stops");
    }
}

/// A hub with its defaults (metrics export off).
pub async fn hub() -> Hub {
    hub_with(&[], &[]).await
}

/// A hub with the variables `env` and the files `files` (path in the container, content).
pub async fn hub_with(env: &[(&str, &str)], files: &[(&str, &str)]) -> Hub {
    let image = image();
    let (name, tag) = image.split_once(':').unwrap_or((&image, "latest"));
    let mut request = GenericImage::new(name, tag)
        .with_exposed_port(GRPC.tcp())
        .with_exposed_port(HTTP.tcp())
        .with_wait_for(WaitFor::message_on_stdout("http listening"))
        .with_wait_for(WaitFor::message_on_stdout("hub listening"))
        .with_env_var("SNODRA_SERVER_OTLP_ENDPOINT", "");
    for (key, value) in env {
        request = request.with_env_var(*key, *value);
    }
    for (path, content) in files {
        request = request.with_copy_to(*path, content.as_bytes().to_vec());
    }
    let container = request.start().await.expect("hub container");
    let grpc = container.get_host_port_ipv4(GRPC.tcp()).await.unwrap();
    let http = container.get_host_port_ipv4(HTTP.tcp()).await.unwrap();
    let hub = Hub {
        grpc: SocketAddr::from(([127, 0, 0, 1], grpc)),
        http: SocketAddr::from(([127, 0, 0, 1], http)),
        container,
    };
    until_answers(hub.http).await;
    hub
}

/// A mapped port may refuse for a moment after the hub logged that it listens: waits until the
/// HTTP API answers (15 s at most).
async fn until_answers(http: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(r) = reqwest::get(format!("http://{http}/api/v1/version")).await
            && r.status().is_success()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the hub's HTTP API never answered"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
