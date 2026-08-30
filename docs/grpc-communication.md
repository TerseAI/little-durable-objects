# gRPC communication

There are three runtime communication edges.

| Edge | Method/protocol | Role |
| --- | --- | --- |
| Application -> control plane | `ResolveActorHost` gRPC | Resolve the authoritative host route on every invocation. |
| Application -> host | `Invoke` gRPC | Execute the actor directly; normal invocation data bypasses the control plane. |
| Host -> control plane | `Execute` gRPC | Maintain leases, ownership, durability, recovery, and telemetry. |

The Rust host and JavaScript executor additionally use private NDJSON over a Unix socket inside the host sandbox. That socket is not externally routable.

## Resolution and invocation

```text
application              control plane              provider              host
    | ResolveActorHost        |                        |                    |
    |------------------------>| read manifest + lease  |                    |
    |                         |-- ensure host -------->| (only if needed)   |
    |                         |<--------- handle ------|                    |
    |                         | verify active lease ----------------------->|
    |<---------------- route--|                        |                    |
    | Invoke(actor, method, args, request_id) ---------------------------->|
    |<------------------------------------------- completed/failed/reroute-|
```

The application preserves `request_id` across a routing retry. The host durably stores an invocation receipt with state, so a repeated request can replay the result without executing customer code again.

`InvokeActorReply` has three results:

- `completed`: the result and receipt crossed the durability boundary.
- `failed`: customer or bounded system failure with a stable code and message.
- `reroute`: ownership changed before execution; resolve again with the same request ID.

## Host control-plane commands

Commands are serialized by the Rust client. `publish` detaches each LTX payload into the gRPC binary list; `recovery` returns checkpoint bytes followed by LTX payloads in metadata order. All other commands reject unexpected binary payloads.

The control plane never trusts a provider response as routing authority. Only the lease store can prove that a returned route belongs to a currently active host/session.

## Fencing

A host may execute only while its exact `(host_id, session_id)` lease is active. Ownership lives in a versioned manifest. Lease expiry allows a replacement to claim a higher ownership epoch; compare-and-swap publication then prevents the old owner from advancing canonical state even if it is still running.

## Security boundary

- Workflow authority tokens can resolve but cannot claim, publish, recover, or manage leases.
- Workflow invocation tokens can call hosts but cannot call the control plane.
- Host authority tokens are bound to one namespace, host ID, session, role, region, and optional code revision.
- Sandboxes receive no PostgreSQL, GCS, or sandbox-provider credentials.
