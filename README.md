<p align="center">
  <img alt="kraken event browser mascot" src="https://gitlab.damoreira.ml/kraken-org/kraken/-/raw/main/docs/assets/brand/svg/kraken-event-browser.svg" width="160">
</p>

<h1 align="center">kraken-event-browser</h1>

<p align="center">
  The hub's event browser add-on: answers the dashboard's event searches, records, lag and tails from the event store.
</p>

<p align="center">
  <a href="https://gitlab.damoreira.ml/kraken-org/kraken-event-browser/-/pipelines"><img alt="pipeline" src="https://gitlab.damoreira.ml/kraken-org/kraken-event-browser/badges/main/pipeline.svg"></a>
  <a href="https://gitlab.damoreira.ml/kraken-org/kraken-event-browser/-/releases"><img alt="release" src="https://gitlab.damoreira.ml/kraken-org/kraken-event-browser/-/badges/release.svg"></a>
  <img alt="Rust" src="https://img.shields.io/badge/Rust-1.99.1-000000?logo=rust&logoColor=white">
  <img alt="image" src="https://img.shields.io/badge/image-distroless-blue?logo=docker&logoColor=white">
  <a href="https://gitlab.damoreira.ml/kraken-org/kraken/-/tree/main/docs"><img alt="kraken docs" src="https://img.shields.io/badge/kraken-umbrella%20docs-0B5FFF"></a>
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

---

`kraken-event-browser` is the crate, binary and image of the hub's event browser add-on (umbrella ADR 0027,
`docs/adr/0027-modular-event-store.md`; the routes, bounds and payload rules: ADR 0025). The hub reads no event store:
it answers its `/api/v1/events*` routes and the dashboard's Events page by asking this add-on, which connects to the hub
like any service, registers the reserved query types `kraken.events.Search`, `kraken.events.Get`,
`kraken.events.Sources`, `kraken.events.Lag` (point queries) and `kraken.events.Tail` (a streaming query), and answers
them from the event store.

## Table of contents

- [How it relates to the hub and the library](#how-it-relates-to-the-hub-and-the-library)
- [Behaviour](#behaviour)
- [Settings](#settings)
- [Running it next to the hub](#running-it-next-to-the-hub)
- [Build and test](#build-and-test)
- [The library dependency](#the-library-dependency)
- [Image](#image)
- [Layout](#layout)
- [CI/CD pipeline](#cicd-pipeline)
- [License](#license)

## How it relates to the hub and the library

- **It depends on the hub; the hub does not depend on it.** It speaks to the hub over the hub's protocol through
  kraken-messaging-rs, as every service does, and its tests run the hub's released image (`KRAKEN_HUB_IMAGE`). The
  hub's repository holds no code of the add-on and links no event store client; it only answers its events routes
  through whichever browser is connected (404 `event browser not installed` without one). The hub's side (the routes,
  the payload rules, the audit lines, its metrics) is documented in kraken-hub's README, "Event browser".
- **It builds on kraken-messaging-rs** and the library's event store add-on, which give it the hub client, the
  settings of the hub connection and the event store's browser (see [The library dependency](#the-library-dependency)).
- It was developed inside the kraken-hub repository (`event-browser/`) and extracted at kraken-hub commit `d869416`.

## Behaviour

It connects to every hub of `KRAKEN_HUB_ADDRESS` exactly as a service built on kraken-messaging-rs does (the library's
hub client and settings), registers as service `kraken-event-browser` with the five `kraken.events.*` types, and
answers them from the event store's browser chosen by `KRAKEN_EVENT_STORE`. It audits nothing (the hub does) and logs
one JSON line per call with the request's `userId` and `correlationId` headers (cut to 128 characters; informational,
any sender allowed to send the types can set them).

- **Answers fit the hub.** A point answer stays under the hub's 2 MiB and a tail part under 64 KiB, whatever the
  settings: a record's payload is cut further (`payloadTruncated`) until its JSON-escaped or base64 text fits; a lag
  view drops the subscriptions with the least lag until it fits (`truncated`); a search page that would not fit is
  asked again for as many results as fit, so it comes with the event store's cursor to the rest; anything else that
  would still not fit is answered `unavailable`.
- **Tails** begin with `start`, carry the heartbeats the hub asks for (`heartbeatMs`), end with `end`. When the
  browser stops (SIGTERM, Ctrl-C) every open tail ends with `shutdown` before the hub connections close (at most 5 s);
  a reader that leaves makes the hub cancel the request, which frees the tail's slot. A search, record or listing
  whose caller leaves is forgotten by the hub at once, but a point query has no `Cancel` in the protocol: the
  browser's read runs to its own end, so its read slot stays taken until then (with `kafka`, until the scan reaches
  its time budget, `KRAKEN_EVENT_BROWSER_TIMEOUT_MS` or the hub's lower `timeoutMs`).
- **Lag metric**: every `KRAKEN_EVENT_BROWSER_LAG_INTERVAL_MS` the lag of every subscription of every source, live
  instance or not, as `kraken_event_browser_subscription_lag{subscription, source}`, plus
  `kraken_event_browser_lag_refreshes_total{outcome=ok|failed}`. It reads through a browser instance of its own
  (one read at a time), so it never takes a slot of the dashboard's calls. It runs only when the metrics go somewhere
  (`KRAKEN_OTLP_ENDPOINT`). At most 500 series per source (the event store's bound on a lag listing). A listing that
  comes back truncated (more than 500 subscriptions or out of time; one warning per source) updates the
  subscriptions it reached and keeps the last values of the others; a subscription leaves the gauge when a complete
  listing lacks it; a failed refresh keeps the last values.
- **Metrics** are pushed over OTLP gRPC to `KRAKEN_OTLP_ENDPOINT` (ADR 0026, as the services do; with the library's
  own hub-client metrics), resource `service.name` `kraken-event-browser` and `service.instance.id` the instance id.
- **Health**: the `grpc.health.v1.Health` service (service `""`) on `KRAKEN_EVENT_BROWSER_HEALTH_LISTEN`, `SERVING`
  while at least one hub has the browser registered, `NOT_SERVING` before, without a hub, and from the start of a
  shutdown. `kraken-event-browser --health-check` probes it on the loopback within 3 s (the image's `HEALTHCHECK`);
  `kraken-event-browser --version` prints the version. Logs: JSON on stdout, filtered by `RUST_LOG` (default `info`).

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `KRAKEN_EVENT_STORE` | `kafka` | the event store read; the only one today is `kafka` (another value stops the startup, naming the known ones) |
| `KRAKEN_EVENT_BROWSER_SOURCES` | `kraken.events` | comma-separated sources the browser reads; the first is the default of a call that names none (each at most 1 KiB) |
| `KRAKEN_EVENT_BROWSER_MAX_SCANNED` | `50000` | records a search page reads at most (but one per segment at least) |
| `KRAKEN_EVENT_BROWSER_MAX_RESULTS` | `100` | results a search page returns at most (1 to 1000) |
| `KRAKEN_EVENT_BROWSER_TIMEOUT_MS` | `10000` | time a search, a record read, a sources or lag listing or a tail's setup takes at most (at most 5 min) |
| `KRAKEN_EVENT_BROWSER_MAX_SEARCHES` | `4` | searches, record reads, sources and lag listings at once; one more is `busy` (429) (at most 1024) |
| `KRAKEN_EVENT_BROWSER_MAX_TAILS` | `4` | tails open at once; one more is `busy` (429) (at most 1024) |
| `KRAKEN_EVENT_BROWSER_TAIL_DURATION_MS` | `900000` | a tail's longest duration (1 s to 24 h; the hub asks less: `KRAKEN_HUB_MAX_STREAM_DURATION` - 2 s) |
| `KRAKEN_EVENT_BROWSER_TAIL_BUFFER` | `256` | events a tail holds for a slow reader; one more ends it `slowReader` (at most 65536) |
| `KRAKEN_EVENT_BROWSER_MAX_PAYLOAD_BYTES` | `262144` | payload bytes a record carries at most (at most 1 MiB, so it fits the hub's answers) |
| `KRAKEN_EVENT_BROWSER_LAG_INTERVAL_MS` | `30000` | the lag metric's period, at least 1000; `0` turns it off. Without `KRAKEN_OTLP_ENDPOINT` the metric is off too (one `info` line says so): no second event store connection, no periodic listing |
| `KRAKEN_EVENT_BROWSER_HEALTH_LISTEN` | `0.0.0.0:7010` | the gRPC health service's address; `off` turns it off |
| `KRAKEN_OTLP_ENDPOINT` | unset | OTLP gRPC endpoint for the metrics (`OTEL_METRIC_EXPORT_INTERVAL` in ms, default 60 s) |
| `RUST_LOG` | `info` | log filter |
| `KRAKEN_KAFKA_BOOTSTRAP` | `kafka:19092` | (`kafka`) the brokers, the Kafka add-on's own setting |
| `KRAKEN_KAFKA_PROPERTY_<NAME>` | none | (`kafka`) a librdkafka property for the browser's consumers, read by the Kafka add-on itself (`KafkaConfig::from_env`): the name in capitals with `_` for `.` and `__` for `_`: `KRAKEN_KAFKA_PROPERTY_SECURITY_PROTOCOL=SASL_PLAINTEXT`, `KRAKEN_KAFKA_PROPERTY_SASL_MECHANISMS=PLAIN`, `KRAKEN_KAFKA_PROPERTY_LOG__LEVEL=6`, ... (the image's librdkafka is built without TLS: `SSL`/`SASL_SSL` need a build with the `ssl` feature). The browser opens the add-on's browser alone (`KafkaBrowser::open`): no producer |
| `KRAKEN_HUB_ADDRESS`, `KRAKEN_INSTANCE_ID`, `KRAKEN_HUB_TOKEN`, `KRAKEN_HUB_TOKEN_FILE`, `KRAKEN_HUB_TLS*`, `KRAKEN_HUB_OAUTH2_*`, ... | the library's | the hub connection, as for any service on kraken-messaging-rs (`Config::from_env`): every hub of `KRAKEN_HUB_ADDRESS` (`static://a:7000,b:7000` or `dns:///`), a static token, JWT client credentials or a client certificate (`kraken://service/kraken-event-browser`) |
| `KRAKEN_SERVICE_NAME` | `kraken-event-browser` | must be `kraken-event-browser` when set (the ACL entry names it) |

Another variable named `KRAKEN_EVENT_BROWSER_*` (a typo, a removed setting) is not read: one startup warning names it
(never its value).

Under access control the hub must let it register the types (the `event-browser` right) and nothing else:

```toml
[[service]]
name = "kraken-event-browser"
event-browser = true
```

or, for a static token: `[[token]] token_sha256 = "<digest>" service = "kraken-event-browser" event-browser = true`.
With the hub's access control `off` or `permissive`, any connected client may register or send the `kraken.events.*`
types and read events without the hub's payload rules: run the browser with access control enforced wherever those
rules matter.

## Running it next to the hub

```yaml
services:
  kraken-hub:
    image: kraken-hub:dev
  kraken-event-browser:
    image: kraken-event-browser:dev
    environment:
      KRAKEN_HUB_ADDRESS: static://kraken-hub:7000
      KRAKEN_KAFKA_BOOTSTRAP: kafka:19092
      KRAKEN_EVENT_BROWSER_SOURCES: kraken.events
      KRAKEN_OTLP_ENDPOINT: http://otel-lgtm:4317
    # Stopping ends every open tail (up to 5 s), then lets calls in flight finish (up to 10 s)
    # before the hub connections close: more than Docker's default 10 s before SIGKILL.
    stop_grace_period: 30s
    depends_on:
      kraken-hub:
        condition: service_healthy
```

Without it the hub answers every `/api/v1/events*` route 404 `{"error":"event browser not installed"}`, its version
says `eventBrowser.installed: false`, and the dashboard hides the Events page and the Processors page's card of
subscriptions without a live instance.

## Build and test

Requires Rust 1.99.1 (`rust-toolchain.toml` pins it), `protoc` and `cmake` (the event store client library is built
from source), ssh access to kraken-messaging-rs (see [The library dependency](#the-library-dependency)) and, for the
tests behind a real hub and the end-to-end test, Docker.

```sh
export CARGO_NET_GIT_FETCH_WITH_CLI=true
cargo build
cargo test --all-features -- --test-threads=1
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

- unit tests in `src/` and `tests/answers.rs` (no Docker): the settings, the answers fitted to the hub's bounds, over
  an event store double (`tests/common/`);
- `tests/hub.rs` (Docker): the browser behind a real hub on the event store double: the hub's events routes answer
  through it, a tail's `start`, records, heartbeats and `shutdown`, a freed tail slot, 429 when busy, health following
  the hub connection, a refused browser (its credential lacks the `event-browser` right) retrying slowly;
- `tests/e2e.rs` (Docker): an event store container, a hub container and the browser as the binary runs it: search
  with its filters and paging, a record with and without its payload under the hub's payload rules, the sources, the
  lag of a subscription without a live instance, a tail, 429, and 404 on every route once the browser stopped.

The hub is always a container of the hub's image (`tests/hub_container/`): `KRAKEN_HUB_IMAGE`, default
`kraken-hub:modular`, configured by its `KRAKEN_HUB_*` variables. This repository never builds the hub from source.

## The library dependency

It builds on kraken-messaging-rs and its Kafka add-on (`kraken-messaging-kafka`) as git dependencies pinned by `rev` to
the library's merged `feat/modular-stores` commit (`tag = "v0.5.0"` once the library is released), named by the ssh URL
`ssh://git@local.gitlab.damoreira.ml/kraken-org/kraken-messaging-rs.git`. The repository is private: the ssh URL works
unchanged on a developer machine with ssh access, where an https URL would need a token on every machine; CI, which
has a deploy token and no ssh key, rewrites it to https (see [CI/CD pipeline](#cicd-pipeline)), and `Cargo.lock`
records the ssh URL either way. Cargo's built-in git client reads ssh keys only from an agent, so fetch with the git
CLI: `CARGO_NET_GIT_FETCH_WITH_CLI=true cargo test` (or `net.git-fetch-with-cli = true` in your cargo configuration).

## Image

```sh
# with an ssh agent holding a key that can read kraken-messaging-rs (`ssh-add`)
docker buildx build --ssh default -t kraken-event-browser:dev .
# or with a token: a git configuration that rewrites the library's ssh URL, given as the secret `gitconfig`
export KRAKEN_GITCONFIG="$(printf '[url "https://%s:%s@gitlab.damoreira.ml/kraken-org/kraken-messaging-rs.git"]\n\tinsteadOf = ssh://git@local.gitlab.damoreira.ml/kraken-org/kraken-messaging-rs.git\n' "$GITLAB_USER" "$GITLAB_TOKEN")"
docker buildx build --secret id=gitconfig,env=KRAKEN_GITCONFIG -t kraken-event-browser:dev .
```

The builder fetches the library with the git CLI; the ssh agent or the secret reaches only the `RUN` that fetches,
never a layer (the build container accepts the gitlab host key on first use). It builds the event store client
library from source (cmake) with zlib and zstd linked in; the runtime is distroless (no shell, no curl), listens for
health on 7010, and its `HEALTHCHECK` runs `--health-check`. CI publishes it with every release of this repository:
`harbor.damoreira.ml/kraken-org/kraken-event-browser:X.Y.Z` and `:latest`.

## Layout

```
src/lib.rs          crate root: the five kraken.events.* handlers, serve() and run()
src/main.rs         thin binary: --version, --health-check, settings from the environment, run
src/settings.rs     KRAKEN_EVENT_BROWSER_* and KRAKEN_EVENT_STORE parsing and bounds
src/store.rs        the event store chosen by KRAKEN_EVENT_STORE, its browser opened from its add-on's settings
src/answers.rs      answers fitted to the hub's bounds
src/tails.rs        the open tails, ended with `shutdown` when the browser stops
src/lag.rs          the lag metric
src/health.rs       the gRPC health service and its probe
src/telemetry.rs    JSON logs and OTLP metrics
tests/common/       the event store double
tests/hub_container/  the hub container (KRAKEN_HUB_IMAGE) the tests run
tests/answers.rs, tests/hub.rs, tests/e2e.rs
Dockerfile          the image
```

## CI/CD pipeline

Defined in [`.gitlab-ci.yml`](.gitlab-ci.yml); a pipeline runs for every merge request and for every push to `main`,
nothing else. The runners use the shell executor (Rust, `protoc`, `cmake`, Docker and buildx installed on the host):
no job image, no services; the tests talk to the host's Docker daemon (`TESTCONTAINERS_RYUK_DISABLED=true`).

| Stage | Job | Runs | Produces |
|---|---|---|---|
| check | `fmt` | always | `cargo fmt --all -- --check` |
| check | `clippy` | always | `cargo clippy --all-targets --all-features -- -D warnings` |
| test | `test` | always | logs in to Harbor, pulls `KRAKEN_HUB_IMAGE` (the hub's latest release), then `cargo test --all-features -- --test-threads=1` |
| release | `release` | pushes to `main` only, one at a time | the next version and its release (below) |

The cargo jobs fetch kraken-messaging-rs over https with a deploy token: they extend `.kraken-messaging-rs-fetch`,
which sets `CARGO_NET_GIT_FETCH_WITH_CLI=true` and puts git's
`url.https://<user>:<token>@$CI_SERVER_HOST/kraken-org/kraken-messaging-rs.git.insteadOf <ssh URL>` in the job's
environment only (`GIT_CONFIG_COUNT`, git 2.31 or later on the runner), so nothing is written to the shell runner's
git configuration and no command line shows the token.

`release` computes the semantic-version bump from the commits since the latest `v*` tag: `BREAKING CHANGE` or a
`type!:` subject is major, a `feat` subject minor, anything else patch (`SEMVER_BUMP` or `RELEASE_VERSION` override
it; nothing to do when `HEAD` is already tagged or no commit is new; `DRY_RUN=true` stops before publishing). Without
a tag it starts from `v0.0.0`, so the first release is `v0.1.0`. It first builds the image, with the deploy token
given to BuildKit as the secret `gitconfig` (a git configuration with that rewrite: never a layer, never a command
line); a failed build stops the release before anything is published. It then tags `vX.Y.Z` and pushes the tag,
pushes `<harbor>/<project>/kraken-event-browser:X.Y.Z` and `:latest`, and creates the GitLab release. A failed push
after the tag is not retried by a rerun (`HEAD` is then tagged): push the image by hand or release again with
`RELEASE_VERSION`.

CI/CD variables (masked where secret, visible to this project as group or project variables):

| Variable | Used by | Purpose |
|---|---|---|
| `KRAKEN_MESSAGING_RS_DEPLOY_USER`, `KRAKEN_MESSAGING_RS_DEPLOY_TOKEN` | cargo jobs, `release` | a deploy token with `read_repository` on kraken-messaging-rs |
| `KRAKEN_HUB_IMAGE` | `test` | the hub image the tests run; the group variable the hub's release sets |
| `HARBOR_HOST`, `HARBOR_PROJECT`, `HARBOR_USERNAME`, `HARBOR_PASSWORD` | `test`, `release` | the registry: pull the hub image, push this image |

## License

MIT, see [LICENSE](LICENSE).
