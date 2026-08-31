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
}

interface SandboxProvider {
    ensureHost(request: EnsureHostRequest): Promise<ActorHostHandle>
}

type SandboxProviderCommand = { readonly operation: "ensure_host"; readonly request: EnsureHostRequest }

export type { ActorHostHandle, EnsureHostRequest, SandboxProvider, SandboxProviderCommand }
