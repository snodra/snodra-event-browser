# syntax=docker/dockerfile:1
# kraken-event-browser (ADR 0027), from the repository root. The builder fetches kraken-messaging-rs,
# a git dependency named by its ssh URL (Cargo.toml), with the git CLI; the credentials reach only
# the RUN that fetches, never a layer. Either
#   docker buildx build --ssh default -t kraken-event-browser .
# with an ssh agent holding a key that can read the repository, or (CI) a git configuration as the
# secret `gitconfig` whose url.<https URL with a deploy token>.insteadOf rewrites the ssh URL:
#   docker buildx build --secret id=gitconfig,env=KRAKEN_GITCONFIG -t kraken-event-browser .
FROM rust:1.99-bookworm AS builder
# cmake: librdkafka is built from source (the Kafka add-on's rdkafka `cmake-build`), with zlib
# and zstd linked in, so the distroless runtime needs no extra library.
RUN apt-get update && apt-get install -y --no-install-recommends cmake protobuf-compiler \
    zlib1g-dev libzstd-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
# The build container has no known_hosts: the gitlab host key is accepted on first use.
ENV GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new"
RUN --mount=type=ssh \
    --mount=type=secret,id=gitconfig,target=/root/.gitconfig \
    cargo build --release --locked --bin kraken-event-browser

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY --from=builder /app/target/release/kraken-event-browser /app/kraken-event-browser
ENV KRAKEN_EVENT_BROWSER_HEALTH_LISTEN=0.0.0.0:7010
EXPOSE 7010
USER nonroot
# Distroless has no shell or curl: the binary probes its own gRPC health service.
HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD ["/app/kraken-event-browser", "--health-check"]
ENTRYPOINT ["/app/kraken-event-browser"]
