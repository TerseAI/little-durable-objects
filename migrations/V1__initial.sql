CREATE TABLE IF NOT EXISTS durable_object_host_leases (
    host_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    route TEXT NOT NULL,
    expires_at_ms BIGINT NOT NULL CHECK (expires_at_ms > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS durable_object_placements (
    object_id TEXT PRIMARY KEY,
    owner_host_id TEXT NOT NULL,
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    home_region TEXT NOT NULL,
    state_version BIGINT NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    state_object TEXT,
    last_request_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS durable_object_namespaces (
    namespace_id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS durable_object_project_specs (
    namespace_id TEXT PRIMARY KEY REFERENCES durable_object_namespaces(namespace_id),
    code_revision TEXT NOT NULL,
    image_ref TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    actor_entrypoint TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
