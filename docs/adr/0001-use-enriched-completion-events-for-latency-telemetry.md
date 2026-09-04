# Use enriched completion events for latency telemetry

Latency diagnostics will extend the existing structured completion events with ordered process-local milestones instead of introducing distributed tracing or an OpenTelemetry backend. Each milestone records its monotonic elapsed offset from the operation's start, allowing adjacent durations to be calculated during analysis without requiring synchronized clocks. Existing correlation identifiers may connect related events for manual analysis, but they do not form a span hierarchy.

During development, every actor operation emits its timeline, including successful warm reads and writes. Sampling and latency thresholds are intentionally deferred until production volume demonstrates a need for them.

Milestones are flat fields on each completion event, named with an `_at_ms` suffix. Nested timing objects and milestone arrays are avoided so events remain simple to query in Cloud Logging.

Modal scheduler and container-boot internals will not be inferred through polling or parsed from client debug logs. Provider events record the supported API boundaries and retain their existing Modal sandbox and region identifiers; a separate actor-host startup event records initialization from process entry through readiness. Modal's dashboard timeline remains the source for provider-owned scheduled, started, and ready transitions.

The supported event families are `actor_client_invocation`, `actor_target_resolution`, `actor_host_provisioning`, `actor_host_startup`, `actor_host_invocation`, and `actor_state_write`. Together they cover target caching, control-plane resolution, provider and Modal activation, Rust and Node startup, warm execution, state reads, and state publication. They are correlated manually through existing request, host, and session identifiers.

Node actor execution remains one Rust-measured Unix-socket round trip. The internal Rust-to-Node protocol will not be extended with worker milestones for this work.

Successful and failed operations emit the same event schema. Failure events preserve the existing outcome or error fields and include every milestone reached before the operation stopped.

This work adds timing fields only. Existing identifiers and outcome fields remain, while new classification flags, counters, payload metadata, and protocol data are outside scope.
