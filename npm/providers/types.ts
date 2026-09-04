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
    readonly startedAtMs: number
    readonly inputParsedAtMs?: number
    readonly sdkLoadedAtMs?: number
    readonly resourcesResolvedAtMs?: number
    readonly existingHostCheckedAtMs?: number
    readonly sandboxScheduledAtMs?: number
    readonly hostReadyObservedAtMs?: number
    readonly routeReadAtMs?: number
    readonly metadataWrittenAtMs?: number
    readonly completedAtMs: number
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

interface TerminateHostsRequest {
    readonly namespaceId: string
    readonly codeRevision: string
    readonly canonicalRegions: readonly string[]
}

interface HostTermination {
    readonly provider: string
    readonly resourceIds: readonly string[]
}

interface PublicHostRouteRequest {
    readonly namespaceId: string
    readonly codeRevision: string
    readonly canonicalRegion: string
}

interface PublicHostRoute {
    readonly route: string
}

interface SandboxProvider {
    ensureHost(request: EnsureHostRequest): Promise<ActorHostHandle>
    publicHostRoute(request: PublicHostRouteRequest): Promise<PublicHostRoute>
    warmImage(request: WarmImageRequest): Promise<ImageWarmup>
    terminateHosts(request: TerminateHostsRequest): Promise<HostTermination>
}

type SandboxProviderCommand =
    | { readonly operation: "ensure_host"; readonly request: EnsureHostRequest }
    | { readonly operation: "public_host_route"; readonly request: PublicHostRouteRequest }
    | { readonly operation: "warm_image"; readonly request: WarmImageRequest }
    | { readonly operation: "terminate_hosts"; readonly request: TerminateHostsRequest }

export type {
    ActorHostHandle,
    ActorHostProvisioning,
    EnsureHostRequest,
    HostTermination,
    ImageWarmup,
    PublicHostRoute,
    PublicHostRouteRequest,
    SandboxProvider,
    SandboxProviderCommand,
    TerminateHostsRequest,
    WarmImageRequest
}
