<p align="center">
  <img alt="Snodra mascot" src="docs/assets/brand/svg/snodra.svg" width="160">
</p>

<h1 align="center">snodra-event-browser</h1>

<p align="center">
  Snodra Server's event browser add-on: answers the dashboard's event searches, records, lag and tails from the event store.
</p>

<p align="center">
  <a href="https://gitlab.damoreira.ml/snodra/snodra-event-browser/-/pipelines"><img alt="pipeline" src="https://gitlab.damoreira.ml/snodra/snodra-event-browser/badges/main/pipeline.svg"></a>
  <a href="https://gitlab.damoreira.ml/snodra/snodra-event-browser/-/releases"><img alt="release" src="https://gitlab.damoreira.ml/snodra/snodra-event-browser/-/badges/release.svg"></a>
  <img alt="Rust" src="https://img.shields.io/badge/Rust-1.99.1-000000?logo=rust&logoColor=white">
  <img alt="image" src="https://img.shields.io/badge/image-distroless-blue?logo=docker&logoColor=white">
  <a href="https://snodra.damoreira.com/"><img alt="Snodra docs" src="https://img.shields.io/badge/docs-snodra.damoreira.com-0B5FFF"></a>
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
</p>

---

`snodra-event-browser` is the crate, binary and image of Snodra Server's event browser add-on (ADR 0027,
`docs/adr/0027-modular-event-store.md`; the routes, bounds and payload rules: ADR 0025). The server reads no event
store: it answers its `/api/v1/events*` routes and the dashboard's Events page by asking this add-on, which connects to
the server like any service, registers the reserved query types `snodra.events.Search`, `snodra.events.Get`,
`snodra.events.Sources`, `snodra.events.Lag` (point queries) and `snodra.events.Tail` (a streaming query), and answers
them from the event store.

**Formerly kraken.** Snodra Event Browser is the renamed `kraken-event-browser`: the same design, the same event
topics (`kraken.events`) and the same answers, with a new crate, binary, image and service name, new settings, new
metrics and new reserved query types. See [Moving from kraken](#moving-from-kraken) for the old → new mapping.

Design, wire protocol, testing and repository conventions: the [Snodra docs](https://snodra.damoreira.com/).

## Table of contents

- [How it relates to the server and the library](#how-it-relates-to-the-server-and-the-library)
- [Behaviour](#behaviour)
- [Settings](#settings)
- [Running it next to the server](#running-it-next-to-the-server)
- [Build and test](#build-and-test)
- [The library dependency](#the-library-dependency)
- [Image](#image)
- [Layout](#layout)
- [Moving from kraken](#moving-from-kraken)
- [CI/CD pipeline](#cicd-pipeline)
- [License](#license)

## How it relates to the server and the library

- **It depends on the server; the server does not depend on it.** It speaks to the server over the hub protocol
  through snodra-messaging-rs, as every service does, and its tests run the server's released image
  (`SNODRA_SERVER_IMAGE`). The server's repository holds no code of the add-on and links no event store client; it only
  answers its events routes through whichever browser is connected (404 `event browser not installed` without one).
  The server's side (the routes, the payload rules, the audit lines, its metrics) is documented in snodra-server's
  README, "Event browser".
- **It builds on snodra-messaging-rs** and the library's event store add-on, which give it the hub client, the
  settings of the server connection and the event store's browser (see [The library dependency](#the-library-dependency)).
- It was developed inside the kraken-hub repository (`event-browser/`) and extracted at kraken-hub commit `d869416`.

## Behaviour

It connects to every server of `SNODRA_SERVER_ADDRESS` exactly as a service built on snodra-messaging-rs does (the
library's hub client and settings), registers as service `snodra-event-browser` with the five `snodra.events.*` types,
and answers them from the event store's browser chosen by `SNODRA_EVENT_STORE`. It audits nothing (the server does) and
logs one JSON line per call with the request's `userId` and `correlationId` headers (cut to 128 characters;
informational, any sender allowed to send the types can set them).

- **Answers fit the server.** A point answer stays under the server's 2 MiB and a tail part under 64 KiB, whatever the
  settings: a record's payload is cut further (`payloadTruncated`) until its JSON-escaped or base64 text fits; a lag
  view drops the subscriptions with the least lag until it fits (`truncated`); a search page that would not fit is
  asked again for as many results as fit, so it comes with the event store's cursor to the rest; anything else that
  would still not fit is answered `unavailable`.
- **Tails** begin with `start`, carry the heartbeats the server asks for (`heartbeatMs`), end with `end`. When the
  browser stops (SIGTERM, Ctrl-C) every open tail ends with `shutdown` before the hub connections close (at most 5 s);
  a reader that leaves makes the server cancel the request, which frees the tail's slot. A search, record or listing
  whose caller leaves is forgotten by the server at once, but a point query has no `Cancel` in the protocol: the
  browser's read runs to its own end, so its read slot stays taken until then (with `kafka`, until the scan reaches
  its time budget, `SNODRA_EVENT_BROWSER_TIMEOUT_MS` or the server's lower `timeoutMs`).
- **Lag metric**: every `SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS` the lag of every subscription of every source, live
  instance or not, as `snodra_event_browser_subscription_lag{subscription, source}`, plus
  `snodra_event_browser_lag_refreshes_total{outcome=ok|failed}`. It reads through a browser instance of its own
  (one read at a time), so it never takes a slot of the dashboard's calls. It runs only when the metrics go somewhere
  (`SNODRA_OTLP_ENDPOINT`). At most 500 series per source (the event store's bound on a lag listing). A listing that
  comes back truncated (more than 500 subscriptions or out of time; one warning per source) updates the
  subscriptions it reached and keeps the last values of the others; a subscription leaves the gauge when a complete
  listing lacks it; a failed refresh keeps the last values.
- **Metrics** are pushed over OTLP gRPC to `SNODRA_OTLP_ENDPOINT` (ADR 0026, as the services do; with the library's
  own hub-client metrics), resource `service.name` `snodra-event-browser` and `service.instance.id` the instance id.
- **Health**: the `grpc.health.v1.Health` service (service `""`) on `SNODRA_EVENT_BROWSER_HEALTH_LISTEN`, `SERVING`
  while at least one server has the browser registered, `NOT_SERVING` before, without a server, and from the start of a
  shutdown. `snodra-event-browser --health-check` probes it on the loopback within 3 s (the image's `HEALTHCHECK`);
  `snodra-event-browser --version` prints the version. Logs: JSON on stdout, filtered by `RUST_LOG` (default `info`).

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `SNODRA_EVENT_STORE` | `kafka` | the event store read; the only one today is `kafka` (another value stops the startup, naming the known ones) |
| `SNODRA_EVENT_BROWSER_SOURCES` | `kraken.events` | comma-separated sources the browser reads; the first is the default of a call that names none (each at most 1 KiB) |
| `SNODRA_EVENT_BROWSER_MAX_SCANNED` | `50000` | records a search page reads at most (but one per segment at least) |
| `SNODRA_EVENT_BROWSER_MAX_RESULTS` | `100` | results a search page returns at most (1 to 1000) |
| `SNODRA_EVENT_BROWSER_TIMEOUT_MS` | `10000` | time a search, a record read, a sources or lag listing or a tail's setup takes at most (at most 5 min) |
| `SNODRA_EVENT_BROWSER_MAX_SEARCHES` | `4` | searches, record reads, sources and lag listings at once; one more is `busy` (429) (at most 1024) |
| `SNODRA_EVENT_BROWSER_MAX_TAILS` | `4` | tails open at once; one more is `busy` (429) (at most 1024) |
| `SNODRA_EVENT_BROWSER_TAIL_DURATION_MS` | `900000` | a tail's longest duration (1 s to 24 h; the server asks less: `SNODRA_SERVER_MAX_STREAM_DURATION` - 2 s) |
| `SNODRA_EVENT_BROWSER_TAIL_BUFFER` | `256` | events a tail holds for a slow reader; one more ends it `slowReader` (at most 65536) |
| `SNODRA_EVENT_BROWSER_MAX_PAYLOAD_BYTES` | `262144` | payload bytes a record carries at most (at most 1 MiB, so it fits the server's answers) |
| `SNODRA_EVENT_BROWSER_LAG_INTERVAL_MS` | `30000` | the lag metric's period, at least 1000; `0` turns it off. Without `SNODRA_OTLP_ENDPOINT` the metric is off too (one `info` line says so): no second event store connection, no periodic listing |
| `SNODRA_EVENT_BROWSER_HEALTH_LISTEN` | `0.0.0.0:7010` | the gRPC health service's address; `off` turns it off |
| `SNODRA_OTLP_ENDPOINT` | unset | OTLP gRPC endpoint for the metrics (`OTEL_METRIC_EXPORT_INTERVAL` in ms, default 60 s) |
| `RUST_LOG` | `info` | log filter |
| `SNODRA_KAFKA_BOOTSTRAP` | `kafka:19092` | (`kafka`) the brokers, the Kafka add-on's own setting |
| `SNODRA_KAFKA_PROPERTY_<NAME>` | none | (`kafka`) a librdkafka property for the browser's consumers, read by the Kafka add-on itself (`KafkaConfig::from_env`): the name in capitals with `_` for `.` and `__` for `_`: `SNODRA_KAFKA_PROPERTY_SECURITY_PROTOCOL=SASL_PLAINTEXT`, `SNODRA_KAFKA_PROPERTY_SASL_MECHANISMS=PLAIN`, `SNODRA_KAFKA_PROPERTY_LOG__LEVEL=6`, ... (the image's librdkafka is built without TLS: `SSL`/`SASL_SSL` need a build with the `ssl` feature). The browser opens the add-on's browser alone (`KafkaBrowser::open`): no producer |
| `SNODRA_SERVER_ADDRESS`, `SNODRA_INSTANCE_ID`, `SNODRA_SERVER_TOKEN`, `SNODRA_SERVER_TOKEN_FILE`, `SNODRA_SERVER_TLS*`, `SNODRA_SERVER_OAUTH2_*`, ... | the library's | the server connection, as for any service on snodra-messaging-rs (`Config::from_env`): every server of `SNODRA_SERVER_ADDRESS` (`static://a:7000,b:7000` or `dns:///`), a static token, JWT client credentials or a client certificate (`snodra://service/snodra-event-browser`) |
| `SNODRA_SERVICE_NAME` | `snodra-event-browser` | must be `snodra-event-browser` when set (the ACL entry names it) |

Another variable named `SNODRA_EVENT_BROWSER_*` (a typo, a removed setting) is not read: one startup warning names it
(never its value).

Under access control the server must let it register the types (the `event-browser` right) and nothing else:

```toml
[[service]]
name = "snodra-event-browser"
event-browser = true
```

or, for a static token: `[[token]] token_sha256 = "<digest>" service = "snodra-event-browser" event-browser = true`.
With the server's access control `off` or `permissive`, any connected client may register or send the `snodra.events.*`
types and read events without the server's payload rules: run the browser with access control enforced wherever those
rules matter.

## Running it next to the server

```yaml
services:
  snodra-server:
    image: snodra-server:dev
  snodra-event-browser:
    image: snodra-event-browser:dev
    environment:
      SNODRA_SERVER_ADDRESS: static://snodra-server:7000
      SNODRA_KAFKA_BOOTSTRAP: kafka:19092
      SNODRA_EVENT_BROWSER_SOURCES: kraken.events
      SNODRA_OTLP_ENDPOINT: http://otel-lgtm:4317
    # Stopping ends every open tail (up to 5 s), then lets calls in flight finish (up to 10 s)
    # before the hub connections close: more than Docker's default 10 s before SIGKILL.
    stop_grace_period: 30s
    depends_on:
      snodra-server:
        condition: service_healthy
```

Without it the server answers every `/api/v1/events*` route 404 `{"error":"event browser not installed"}`, its version
says `eventBrowser.installed: false`, and the dashboard hides the Events page and the Processors page's card of
subscriptions without a live instance.

## Build and test

Requires Rust 1.99.1 (`rust-toolchain.toml` pins it), `protoc` and `cmake` (the event store client library is built
from source), ssh access to snodra-messaging-rs (see [The library dependency](#the-library-dependency)) and, for the
tests behind a real server and the end-to-end test, Docker.

```sh
export CARGO_NET_GIT_FETCH_WITH_CLI=true
cargo build
cargo test --all-features -- --test-threads=1
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

- unit tests in `src/` and `tests/answers.rs` (no Docker): the settings, the answers fitted to the server's bounds,
  over an event store double (`tests/common/`);
- `tests/hub.rs` (Docker): the browser behind a real server on the event store double: the server's events routes
  answer through it, a tail's `start`, records, heartbeats and `shutdown`, a freed tail slot, 429 when busy, health
  following the server connection, a refused browser (its credential lacks the `event-browser` right) retrying slowly;
- `tests/e2e.rs` (Docker): an event store container, a server container and the browser as the binary runs it: search
  with its filters and paging, a record with and without its payload under the server's payload rules, the sources, the
  lag of a subscription without a live instance, a tail, 429, and 404 on every route once the browser stopped.

The server is always a container of the server's image (`tests/hub_container/`): `SNODRA_SERVER_IMAGE`, default
`snodra-server:dev`, configured by its `SNODRA_SERVER_*` variables. This repository never builds the server from source.

## The library dependency

It builds on snodra-messaging-rs and its Kafka add-on (`snodra-messaging-kafka`) as git dependencies pinned to the
library's release tag (`tag = "v0.6.0"`), named by the ssh URL
`ssh://git@local.gitlab.damoreira.ml/snodra/snodra-messaging-rs.git`. The repository is private: the ssh URL works
unchanged on a developer machine with ssh access, where an https URL would need a token on every machine; CI, which
has a deploy token and no ssh key, rewrites it to https (see [CI/CD pipeline](#cicd-pipeline)), and `Cargo.lock`
records the ssh URL either way. Cargo's built-in git client reads ssh keys only from an agent, so fetch with the git
CLI: `CARGO_NET_GIT_FETCH_WITH_CLI=true cargo test` (or `net.git-fetch-with-cli = true` in your cargo configuration).

## Image

```sh
# with an ssh agent holding a key that can read snodra-messaging-rs (`ssh-add`)
docker buildx build --ssh default -t snodra-event-browser:dev .
# or with a token: a git configuration that rewrites the library's ssh URL, given as the secret `gitconfig`
export SNODRA_GITCONFIG="$(printf '[url "https://%s:%s@gitlab.damoreira.ml/snodra/snodra-messaging-rs.git"]\n\tinsteadOf = ssh://git@local.gitlab.damoreira.ml/snodra/snodra-messaging-rs.git\n' "$GITLAB_USER" "$GITLAB_TOKEN")"
docker buildx build --secret id=gitconfig,env=SNODRA_GITCONFIG -t snodra-event-browser:dev .
```

The builder fetches the library with the git CLI; the ssh agent or the secret reaches only the `RUN` that fetches,
never a layer (the build container accepts the gitlab host key on first use). It builds the event store client
library from source (cmake) with zlib and zstd linked in; the runtime is distroless (no shell, no curl), listens for
health on 7010, and its `HEALTHCHECK` runs `--health-check`. CI publishes it with every release of this repository:
`harbor.damoreira.ml/snodra/snodra-event-browser:X.Y.Z` and `:latest`.

## Layout

```
src/lib.rs          crate root: the five snodra.events.* handlers, serve() and run()
src/main.rs         thin binary: --version, --health-check, settings from the environment, run
src/settings.rs     SNODRA_EVENT_BROWSER_* and SNODRA_EVENT_STORE parsing and bounds
src/store.rs        the event store chosen by SNODRA_EVENT_STORE, its browser opened from its add-on's settings
src/answers.rs      answers fitted to the server's bounds
src/tails.rs        the open tails, ended with `shutdown` when the browser stops
src/lag.rs          the lag metric
src/health.rs       the gRPC health service and its probe
src/telemetry.rs    JSON logs and OTLP metrics
tests/common/       the event store double
tests/hub_container/  the server container (SNODRA_SERVER_IMAGE) the tests run
tests/answers.rs, tests/hub.rs, tests/e2e.rs
Dockerfile          the image
```

## Moving from kraken

v0.5.1 was the last release under the old name (`kraken-event-browser`). This is a clean cut: there are no aliases, and
Snodra Server must move with it — the protocol package changed (`kraken.hub.v1` → `snodra.hub.v1`), so the browser's
reserved query types changed with it.

1. **Names.** The crate, binary, image and registered service name are `snodra-event-browser`; the library is
   `snodra_event_browser`. The repository is `gitlab.damoreira.ml/snodra/snodra-event-browser` and the image
   `harbor.damoreira.ml/snodra/snodra-event-browser`.

2. **Environment.** Every `KRAKEN_*` variable is `SNODRA_*`, the browser's own `KRAKEN_EVENT_BROWSER_*` are
   `SNODRA_EVENT_BROWSER_*`, the Kafka add-on's `KRAKEN_KAFKA_*` are `SNODRA_KAFKA_*`, and the hub client's
   `KRAKEN_HUB_*` are the server's own names, `SNODRA_SERVER_*` (there is no alias; the [settings table](#settings) is
   the current list):

   | v0.5.1 | Now |
   |---|---|
   | `KRAKEN_SERVICE_NAME`, `KRAKEN_INSTANCE_ID` | `SNODRA_SERVICE_NAME`, `SNODRA_INSTANCE_ID` |
   | `KRAKEN_HUB_ADDRESS` | `SNODRA_SERVER_ADDRESS` |
   | `KRAKEN_HUB_TOKEN`, `KRAKEN_HUB_TOKEN_FILE` | `SNODRA_SERVER_TOKEN`, `SNODRA_SERVER_TOKEN_FILE` |
   | `KRAKEN_HUB_TLS*` | `SNODRA_SERVER_TLS*` |
   | `KRAKEN_HUB_OAUTH2_*` | `SNODRA_SERVER_OAUTH2_*` |
   | `KRAKEN_HUB_IMAGE` (test server image) | `SNODRA_SERVER_IMAGE` (default `snodra-server:dev`) |
   | `KRAKEN_EVENT_STORE` | `SNODRA_EVENT_STORE` |
   | `KRAKEN_EVENT_BROWSER_*` | `SNODRA_EVENT_BROWSER_*` |
   | `KRAKEN_KAFKA_*` | `SNODRA_KAFKA_*` |
   | `KRAKEN_OTLP_ENDPOINT` | `SNODRA_OTLP_ENDPOINT` |
   | `KRAKEN_MESSAGING_RS_DEPLOY_USER`, `_TOKEN`, `KRAKEN_GITCONFIG` (CI) | `SNODRA_MESSAGING_RS_DEPLOY_USER`, `_TOKEN`, `SNODRA_GITCONFIG` |

3. **Wire.** The reserved query types are `snodra.events.Search`, `snodra.events.Get`, `snodra.events.Sources`,
   `snodra.events.Lag` and `snodra.events.Tail`; an ACL namespace is `snodra.events.`; the protocol package is
   `snodra.hub.v1`; the mTLS SAN scheme is `snodra://service/<name>`.

4. **Metrics.** The browser's own instruments are `snodra_event_browser_subscription_lag` and
   `snodra_event_browser_lag_refreshes_total`; the library's series moved to `snodra_*` too (see its README). Update
   dashboards, alerts and recording rules.

5. **Kept as they are:** the event source/topic `kraken.events`, its dead-letter topic `kraken.events.dlt`, the
   source-derived stream/segment names (`kraken.events-N`) and every stored record. The browser only reads them, so
   existing event-store data needs no migration.

## CI/CD pipeline

Defined in [`.gitlab-ci.yml`](.gitlab-ci.yml); a pipeline runs for every merge request and for every push to `main`,
nothing else. The runners use the shell executor (Rust, `protoc`, `cmake`, Docker and buildx installed on the host):
no job image, no services; the tests talk to the host's Docker daemon (`TESTCONTAINERS_RYUK_DISABLED=true`).

| Stage | Job | Runs | Produces |
|---|---|---|---|
| check | `fmt` | always | `cargo fmt --all -- --check` |
| check | `clippy` | always | `cargo clippy --all-targets --all-features -- -D warnings` |
| test | `test` | always | logs in to Harbor, pulls `SNODRA_SERVER_IMAGE` (the server's latest release), then `cargo test --all-features -- --test-threads=1` |
| release | `release` | pushes to `main` only, one at a time | the next version and its release (below) |

The cargo jobs fetch snodra-messaging-rs over https with a deploy token: they extend `.snodra-messaging-rs-fetch`,
which sets `CARGO_NET_GIT_FETCH_WITH_CLI=true` and puts git's
`url.https://<user>:<token>@$CI_SERVER_HOST/snodra/snodra-messaging-rs.git.insteadOf <ssh URL>` in the job's
environment only (`GIT_CONFIG_COUNT`, git 2.31 or later on the runner), so nothing is written to the shell runner's
git configuration and no command line shows the token.

`release` computes the semantic-version bump from the commits since the latest `v*` tag: `BREAKING CHANGE` or a
`type!:` subject is major, a `feat` subject minor, anything else patch (`SEMVER_BUMP` or `RELEASE_VERSION` override
it; nothing to do when `HEAD` is already tagged or no commit is new; `DRY_RUN=true` stops before publishing). Without
a tag it starts from `v0.0.0`, so the first release is `v0.1.0`. It first builds the image, with the deploy token
given to BuildKit as the secret `gitconfig` (a git configuration with that rewrite: never a layer, never a command
line); a failed build stops the release before anything is published. It then tags `vX.Y.Z` and pushes the tag,
pushes `<harbor>/<project>/snodra-event-browser:X.Y.Z` and `:latest`, and creates the GitLab release. A failed push
after the tag is not retried by a rerun (`HEAD` is then tagged): push the image by hand or release again with
`RELEASE_VERSION`.

CI/CD variables (masked where secret, visible to this project as group or project variables):

| Variable | Used by | Purpose |
|---|---|---|
| `SNODRA_MESSAGING_RS_DEPLOY_USER`, `SNODRA_MESSAGING_RS_DEPLOY_TOKEN` | cargo jobs, `release` | a deploy token with `read_repository` on snodra-messaging-rs |
| `SNODRA_SERVER_IMAGE` | `test` | the server image the tests run; the group variable the server's release sets |
| `HARBOR_HOST`, `HARBOR_PROJECT`, `HARBOR_USERNAME`, `HARBOR_PASSWORD` | `test`, `release` | the registry: pull the server image, push this image |

## License

Apache-2.0, see [LICENSE](LICENSE).
