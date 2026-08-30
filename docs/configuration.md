# Configuration

The Rust binary runs one role at a time. It has no built-in Terse endpoints or compatibility aliases.

## Process

`DURABLE_OBJECT_PROCESS_ROLE`
: `control-plane`, `maintenance`, or `host`. Defaults to `host`.

`DURABLE_OBJECT_PARENT_LIFETIME_STDIN`
: Shut down when parent stdin closes. Set to any value to enable it.

## Control plane

`DURABLE_OBJECT_CONTROL_PLANE_BIND`
: Cleartext HTTP/2 listen address. Defaults to `127.0.0.1:7100`. Terminate TLS at the hosting platform or reverse proxy.

`DURABLE_OBJECT_JWKS_URL`
: Required JWKS URL used to verify authority credentials.

`DURABLE_OBJECT_JWT_ISSUER`
: Expected JWT issuer. Defaults to `durable-object-control-plane`.

`DURABLE_OBJECT_AUTHORITY_JWT_AUDIENCE`
: Expected control-plane audience. Defaults to `durable-object-authority`.

`DURABLE_OBJECT_JWT_MAX_TTL_SECONDS`
: Maximum accepted JWT lifetime. Defaults to `1800`.

`DURABLE_OBJECT_SANDBOX_PROVIDER_URL`
: Optional provider endpoint used to start or reuse a host. With the bundled HTTP server, use `/hosts/ensure`.

`DURABLE_OBJECT_SANDBOX_PROVIDER_TOKEN`
: Bearer token for the provider endpoint. Set it together with the provider URL.

## Control-plane storage

`DURABLE_OBJECT_STORAGE`
: `local` or `rapid`. Defaults to `rapid`.

`DURABLE_OBJECT_LOCAL_STORE_ROOT`
: Filesystem root for local development. Defaults to `.local/durable-objects/control-plane`.

`DURABLE_OBJECT_POSTGRES_URL`
: PostgreSQL URL for distributed manifests and host leases.

`DURABLE_OBJECT_RAPID_BUCKETS`
: JSON map of canonical region to zonal GCS Rapid bucket.

`DURABLE_OBJECT_STANDARD_BUCKETS`
: JSON map of the same canonical regions to Standard multi-region archive/checkpoint buckets. Values may repeat.

Example:

```sh
DURABLE_OBJECT_RAPID_BUCKETS='{"north-america-east":"rapid-us-east4-a","europe-west":"rapid-europe-west1-b"}'
DURABLE_OBJECT_STANDARD_BUCKETS='{"north-america-east":"checkpoints-us","europe-west":"checkpoints-eu"}'
```

## Maintenance

`DURABLE_OBJECT_POSTGRES_URL`
: PostgreSQL URL used by the maintenance process.

`DURABLE_OBJECT_RAPID_BUCKETS`
: Same canonical-region map as the control plane.

`DURABLE_OBJECT_STANDARD_BUCKETS`
: Same archive/checkpoint map as the control plane.

`DURABLE_OBJECT_DURABILITY_POLL_MS`
: Delay between maintenance passes. Defaults to `5000`.

`DURABLE_OBJECT_CHECKPOINT_TAIL_TXIDS`
: Minimum tail length before creating a consolidated checkpoint. Defaults to `64`.

`DURABLE_OBJECT_DURABILITY_BATCH_SIZE`
: Maximum objects processed in one pass. Defaults to `100`.

`DURABLE_OBJECT_RAPID_GC_GRACE_MS`
: Minimum age of an archived copy before deleting the corresponding Rapid log. Defaults to `43200000`.

## Host

`DURABLE_OBJECT_CREDENTIALS_URL`
: Required endpoint that exchanges a host bootstrap credential for scoped JWT credentials.

`DURABLE_OBJECT_CREDENTIAL`
: Required bootstrap credential supplied by the sandbox provider.

`DURABLE_OBJECT_CONTROL_PLANE_URL`
: Required control-plane origin.

`DURABLE_OBJECT_HOST_ID` and `DURABLE_OBJECT_SESSION_ID`
: Optional preassigned host identity. Set both or neither.

`DURABLE_OBJECT_REGION`
: Canonical home region served by this host. Defaults to `default`.

`DURABLE_OBJECT_CODE_REVISION`
: Opaque immutable actor-code revision served by the host.

`DURABLE_OBJECT_LOCAL_ROOT`
: Required root for SQLite and LTX files. The Modal adapter mounts the regional cache volume here.

`DURABLE_OBJECT_EXECUTOR_SOCKET`
: Unix socket used by Rust and the JavaScript executor. Defaults to `<local root>/actor-session.sock`.

`DURABLE_OBJECT_ENTRYPOINT`
: Optional JavaScript actor entrypoint. Defaults to `src/durable-objects.ts`.

`DURABLE_OBJECT_HOST_ROUTE`
: Public host origin advertised in its lease.

`DURABLE_OBJECT_HOST_BIND`
: Host gRPC listen address. Defaults to `127.0.0.1:0`, or `0.0.0.0:7101` when a public route is configured.

`DURABLE_OBJECT_INVOKE_JWT_AUDIENCE`
: Expected invocation-token audience. Defaults to `durable-object-invoke`.

`DURABLE_OBJECT_LEASE_MS`
: Host lease lifetime. Defaults to `30000` and may not exceed `60000`.

`DURABLE_OBJECT_RENEW_MS`
: Lease-renew interval. Defaults to `10000` and must be shorter than the lease lifetime.

## Telemetry

Telemetry is disabled unless explicitly configured.

`DURABLE_OBJECT_TELEMETRY_EXPORTER`
: `none` or `posthog`. Defaults to `none`.

`DURABLE_OBJECT_POSTHOG_API_KEY`
: Required only for the PostHog exporter.

`DURABLE_OBJECT_POSTHOG_HOST`
: Defaults to `https://us.i.posthog.com`.

`DURABLE_OBJECT_ENVIRONMENT`
: Deployment environment attached to telemetry. Defaults to `development`.

## Canonical region catalog

The catalog is exported by `@terse/durable-objects/regions`. Adapters may replace it with deployment-specific configuration.

| Canonical region | Modal selector | GCS Rapid zone | Standard multi-region |
| --- | --- | --- | --- |
| `north-america-east` | `us-east` on GCP | `US-EAST4-A` | `US` |
| `north-america-central` | `us-central` on GCP | `US-CENTRAL1-A` | `US` |
| `north-america-south` | `us-south` on GCP | `US-SOUTH1-A` | `US` |
| `north-america-west` | `us-west` on GCP | `US-WEST4-A` | `US` |
| `europe-west` | `eu-west` on GCP | `EUROPE-WEST1-B` | `EU` |
| `asia-southeast` | `ap-southeast` on GCP | `ASIA-SOUTHEAST1-A` | `ASIA` |

Objects store only the canonical region. The Modal and GCS mappings can evolve independently as provider coverage changes.
