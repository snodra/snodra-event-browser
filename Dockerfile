# syntax=docker/dockerfile:1
# snodra-event-browser, from the repository root: docker buildx build -t snodra-event-browser .
FROM rust:1.99-bookworm AS builder
# cmake: librdkafka is built from source (the Kafka add-on's rdkafka `cmake-build`), with zlib
# and zstd built from source and linked in, so the distroless runtime needs no extra library.
# protoc: the snodra-messaging-rs build script generates the hub client.
RUN apt-get update && apt-get install -y --no-install-recommends cmake protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bin snodra-event-browser

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY --from=builder /app/target/release/snodra-event-browser /app/snodra-event-browser
ENV SNODRA_EVENT_BROWSER_HEALTH_LISTEN=0.0.0.0:7010
EXPOSE 7010
USER nonroot
# Distroless has no shell or curl: the binary probes its own gRPC health service.
HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD ["/app/snodra-event-browser", "--health-check"]
ENTRYPOINT ["/app/snodra-event-browser"]
