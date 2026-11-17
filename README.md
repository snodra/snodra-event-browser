<p align="center">
  <img alt="Snodra mascot" src="docs/assets/brand/svg/snodra.svg" width="160">
</p>

<h1 align="center">Snodra Event Browser</h1>

<p align="center">
  The add-on that gives Snodra Server's dashboard a read-only window into the event store: search, records, sources, lag and live tails.
</p>

<p align="center">
  <a href="https://github.com/snodra/snodra-event-browser/actions/workflows/ci.yml"><img alt="ci" src="https://github.com/snodra/snodra-event-browser/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/snodra/snodra-event-browser/releases"><img alt="release" src="https://img.shields.io/github/v/release/snodra/snodra-event-browser"></a>
  <a href="https://hub.docker.com/r/snodra/snodra-event-browser"><img alt="docker" src="https://img.shields.io/docker/v/snodra/snodra-event-browser?label=docker"></a>
  <img alt="Rust" src="https://img.shields.io/badge/Rust-1.99.1-000000?logo=rust&logoColor=white">
  <a href="https://snodra.damoreira.com/"><img alt="docs" src="https://img.shields.io/badge/docs-snodra.damoreira.com-0B5FFF"></a>
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
</p>

---

## What it is

Snodra Server routes messages and stores nothing, so it cannot answer questions about history. This
service does: it connects to the server like any other Snodra service, registers the reserved query
types `snodra.events.Search`, `Get`, `Sources`, `Lag` and `Tail`, and answers them from the event
store it reads.

The dashboard's Events page appears as soon as one browser is registered, and disappears when it is
gone. The browser only reads; it never writes to the event store.

- **Sources**: every topic or stream the event store holds, with lag.
- **Search**: newest first across every segment of a source, paged by cursor.
- **Record**: one event with its headers, and its payload when the caller's token allows it.
- **Tail**: a streaming query that follows new records live.
- **One store per browser**: Kafka, or RabbitMQ Streams. Several browsers can serve several servers.

## Run it

```sh
docker run --rm \
  -e SNODRA_SERVER_ADDRESS=static://snodra-server:7000 \
  -e SNODRA_KAFKA_BOOTSTRAP=kafka:9092 \
  snodra/snodra-event-browser:0.2.0
```

| Setting | What it is |
|---|---|
| `SNODRA_SERVER_ADDRESS` | the server's gRPC target (`static://host:port`, a list, or `dns:///name:port`) |
| `SNODRA_SERVER_TOKEN_FILE`, `SNODRA_SERVER_TLS*` | credentials and TLS for a secured server |
| `SNODRA_KAFKA_BOOTSTRAP` | the event store's Kafka bootstrap |
| `SNODRA_EVENT_BROWSER_*` | sources, page size, tail buffering, retention |
| `SNODRA_OTLP_ENDPOINT` | traces and metrics (`snodra_event_browser_*`) |

Without Kafka the browser still starts and serves the sources it knows; an Events route answers only
while a browser is registered on that server.

## Documentation

The routes, the bounds and the payload rules: **<https://snodra.damoreira.com/server/event-browser/>** and
<https://snodra.damoreira.com/tools/event-browser/>.

## Contributing

Branches off `main`, conventional commits, a pull request with a green workflow.

## License

[Apache-2.0](LICENSE) © 2026 Guilherme D'Amoreira.
