interface EnsureHostRequest {
    readonly namespaceId: string
    readonly principalId: string
    readonly credentialId: string
    readonly codeRevision: string
    readonly canonicalRegion: string
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

interface ActorHostArtifact {
    readonly imageId: string
    readonly workingDirectory: string
    readonly actorEntrypoint?: string
}

interface ActorHostBootstrap {
    readonly credential: string
}

interface SandboxProviderHooks {
    resolveArtifact(request: EnsureHostRequest): Promise<ActorHostArtifact>
    issueHostBootstrap(request: EnsureHostRequest): Promise<ActorHostBootstrap>
}

export type { ActorHostArtifact, ActorHostBootstrap, ActorHostHandle, EnsureHostRequest, SandboxProvider, SandboxProviderHooks }
