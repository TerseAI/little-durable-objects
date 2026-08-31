interface EnsureHostRequest {
    readonly namespaceId: string
    readonly codeRevision: string
    readonly canonicalRegion: string
    readonly hostId: string
    readonly sessionId: string
    readonly hostToken: string
    readonly jwtPublicKeys: string
    readonly controlPlaneUrl: string
    readonly jwtIssuer: string
    readonly invocationJwtAudience: string
    readonly imageRef: string
    readonly workingDirectory: string
    readonly actorEntrypoint?: string
    readonly actorIdleTimeoutMs: number
    readonly hostIdleTimeoutMs: number
}

interface ActorHostHandle {
    readonly hostId: string
    readonly route: string
    readonly canonicalRegion: string
    readonly cacheSource: "volume" | "durable_storage"
}

interface SandboxProvider {
    ensureHost(request: EnsureHostRequest): Promise<ActorHostHandle>
    status(request: EnsureHostRequest): Promise<"serving" | "warm" | "cold">
    deactivate(request: EnsureHostRequest): Promise<void>
    removeLocalCache(request: EnsureHostRequest): Promise<void>
}

type SandboxProviderCommand =
    | { readonly operation: "ensure_host"; readonly request: EnsureHostRequest }
    | { readonly operation: "status"; readonly request: EnsureHostRequest }
    | { readonly operation: "deactivate"; readonly request: EnsureHostRequest }
    | { readonly operation: "remove_local_cache"; readonly request: EnsureHostRequest }

export type { ActorHostHandle, EnsureHostRequest, SandboxProvider, SandboxProviderCommand }
