# Durable-object gRPC protocol

`durable_object.proto` defines the public routing/invocation surface and the internal
host-to-control-plane command envelope:

```text
ActorControlPlaneService
|-- ResolveActorHost
`-- Execute

ActorHostService
`-- Invoke
```

`Execute` keeps JSON metadata separate from binary LTX/checkpoint payloads. SQLite and
storage implementation details do not appear in the application-facing invocation API.

This protocol is still under development. There is no legacy wire-compatibility
requirement yet; change the schema directly when the design calls for it and update
the checked Rust conversions and round-trip tests in the same change.
