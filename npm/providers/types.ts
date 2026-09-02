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
    readonly provisioning?: ActorHostProvisioning
}

interface ActorHostProvisioning {
    readonly provider: string
    readonly resourceId: string
    readonly reused: boolean
    readonly resourceLookupMs: number
    readonly existingLookupMs: number
    readonly createMs: number
    readonly placementMs: number
    readonly tunnelMs: number
    readonly readyMs: number
    readonly metadataMs: number
    readonly totalMs: number
}

interface WarmImageRequest {
    readonly namespaceId: string
    readonly codeRevision: string
    readonly canonicalRegion: string
    readonly imageRef: string
}

interface ImageWarmup {
    readonly provider: string
    readonly resourceId: string
    readonly totalMs: number
}

interface SandboxProvider {
    ensureHost(request: EnsureHostRequest): Promise<ActorHostHandle>
    warmImage(request: WarmImageRequest): Promise<ImageWarmup>
}

type SandboxProviderCommand = { readonly operation: "ensure_host"; readonly request: EnsureHostRequest } | { readonly operation: "warm_image"; readonly request: WarmImageRequest }

export type { ActorHostHandle, ActorHostProvisioning, EnsureHostRequest, ImageWarmup, SandboxProvider, SandboxProviderCommand, WarmImageRequest }
