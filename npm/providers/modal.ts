import { AlreadyExistsError, ModalClient, NotFoundError, Probe } from "modal"
import type { App, Image, Sandbox } from "modal"
import { createHash } from "node:crypto"
import { performance } from "node:perf_hooks"

import { modalPlacement } from "../regions.js"
import type { CanonicalRegionCatalog } from "../regions.js"

import type {
    ActorHostHandle,
    ActorHostProvisioning,
    EnsureHostRequest,
    HostTermination,
    ImageWarmup,
    PublicHostRoute,
    PublicHostRouteRequest,
    SandboxProvider,
    TerminateHostsRequest,
    WarmImageRequest
} from "./types.js"

const hostPort = 7101
const hostRouteFile = "/tmp/durable-object-route"
const readyFile = "/tmp/durable-object-ready"
const hostStderrFile = "/tmp/durable-object-host.stderr"
const hostExitedFile = "/tmp/durable-object-host-exited"
const maximumSandboxLifetimeMs = 24 * 60 * 60 * 1000
const modalPrivateHostname = "i6pn.modal.local"

interface ModalSandboxProviderOptions {
    readonly appName?: string
    readonly binaryPath?: string
    readonly catalog?: CanonicalRegionCatalog
    readonly client?: ModalClient
    readonly now?: () => number
}

type SandboxAcquisition = { readonly sandbox: Sandbox; readonly reused: boolean }
type ProvisioningPhases = {
    -readonly [Key in keyof Omit<ActorHostProvisioning, "provider" | "resourceId" | "reused" | "completedAtMs">]: ActorHostProvisioning[Key]
}

class ModalSandboxProvider implements SandboxProvider {
    private readonly modal: ModalClient
    private readonly appName: string
    private readonly binaryPath: string
    private readonly now: () => number
    private readonly activations = new Map<string, Promise<ActorHostHandle>>()

    constructor(private readonly options: ModalSandboxProviderOptions = {}) {
        this.modal = options.client ?? new ModalClient()
        this.appName = options.appName ?? "durable-object-hosts"
        this.binaryPath = options.binaryPath ?? "/usr/local/bin/little-durable-objects"
        this.now = options.now ?? (() => performance.now())
    }

    async warmImage(request: WarmImageRequest): Promise<ImageWarmup> {
        const startedAt = this.now()
        validateWarmImageRequest(request)
        const placement = modalPlacement(request.canonicalRegion, this.options.catalog)
        const [app, image] = await Promise.all([this.modal.apps.fromName(this.appName, { createIfMissing: true }), this.modal.images.fromId(request.imageRef)])
        const sandbox = await this.modal.sandboxes.experimentalCreate(app, image, {
            command: ["true"],
            timeoutMs: 120_000,
            regions: [...placement.regions],
            cloud: placement.cloud
        })
        try {
            const exitCode = await sandbox.wait()
            if (exitCode !== 0) throw new Error(`Modal image warmup exited with status ${exitCode}`)
            return { provider: "modal", resourceId: sandbox.sandboxId, totalMs: elapsedMs(startedAt, this.now()) }
        } finally {
            await sandbox.terminate().catch(() => undefined)
        }
    }

    async terminateHosts(request: TerminateHostsRequest): Promise<HostTermination> {
        validateTerminateHostsRequest(request, this.options.catalog)
        let app: App
        try {
            app = await this.modal.apps.fromName(this.appName, { createIfMissing: false })
        } catch (error) {
            if (error instanceof NotFoundError) return { provider: "modal", resourceIds: [] }
            throw error
        }
        const resourceIds: string[] = []
        for (const region of request.canonicalRegions) {
            const name = resourceName("host", request.namespaceId, request.codeRevision, region)
            try {
                const sandbox = await this.modal.sandboxes.experimentalFromName(app.name ?? this.appName, name)
                await sandbox.terminate()
                resourceIds.push(sandbox.sandboxId)
            } catch (error) {
                if (!(error instanceof NotFoundError)) throw error
            }
        }
        return { provider: "modal", resourceIds }
    }

    async ensureHost(request: EnsureHostRequest): Promise<ActorHostHandle> {
        const key = resourceName("host", request.namespaceId, request.codeRevision, request.canonicalRegion)
        const current = this.activations.get(key)
        if (current) return current
        const activation = this.ensureHostOnce(request, key).finally(() => this.activations.delete(key))
        this.activations.set(key, activation)
        return activation
    }

    async publicHostRoute(request: PublicHostRouteRequest): Promise<PublicHostRoute> {
        return this.publicRoute(request, hostPort, "HTTP/2")
    }

    private async publicRoute(request: PublicHostRouteRequest, port: number, protocol: string): Promise<PublicHostRoute> {
        validatePublicHostRouteRequest(request, this.options.catalog)
        const app = await this.modal.apps.fromName(this.appName, { createIfMissing: false })
        const name = resourceName("host", request.namespaceId, request.codeRevision, request.canonicalRegion)
        const sandbox = await this.modal.sandboxes.experimentalFromName(app.name ?? this.appName, name)
        if ((await sandbox.poll()) !== null) throw new Error("Modal durable-object host is not running")
        const route = (await sandbox.tunnels())[port]?.url
        if (!route) throw new Error(`Modal did not create the durable-object ${protocol} tunnel`)
        return { route }
    }

    private async ensureHostOnce(request: EnsureHostRequest, name: string): Promise<ActorHostHandle> {
        const startedAt = this.now()
        const phases: ProvisioningPhases = { startedAtMs: 0 }
        validateEnsureRequest(request)
        const placement = modalPlacement(request.canonicalRegion, this.options.catalog)
        const [app, image] = await Promise.all([this.modal.apps.fromName(this.appName, { createIfMissing: true }), this.modal.images.fromId(request.imageRef)])
        this.mark(phases, "resourcesResolvedAtMs", startedAt)
        const existing = await this.existing(app, name)
        this.mark(phases, "existingHostCheckedAtMs", startedAt)
        if (existing) {
            const handle = await this.reuse(existing, request.canonicalRegion, startedAt, phases)
            if (handle) return handle
        }

        const acquired = await this.createSandbox(request, name, app, image, placement)
        this.mark(phases, "sandboxScheduledAtMs", startedAt)
        if (acquired.reused) {
            const handle = await this.reuse(acquired.sandbox, request.canonicalRegion, startedAt, phases)
            if (!handle) throw new Error("concurrent Modal V2 host could not be reused")
            return handle
        }
        return this.activate(acquired.sandbox, request, placement, startedAt, phases)
    }

    private async reuse(sandbox: Sandbox, canonicalRegion: string, startedAt: number, phases: ProvisioningPhases): Promise<ActorHostHandle | undefined> {
        try {
            const handle = await this.readHandle(sandbox, canonicalRegion)
            this.mark(phases, "hostReadyObservedAtMs", startedAt)
            this.mark(phases, "routeReadAtMs", startedAt)
            return this.withProvisioning(handle, sandbox, true, startedAt, phases)
        } catch {
            await sandbox.terminate()
            return undefined
        }
    }

    private async createSandbox(request: EnsureHostRequest, name: string, app: App, image: Image, placement: ReturnType<typeof modalPlacement>): Promise<SandboxAcquisition> {
        try {
            const sandbox = await this.modal.sandboxes.experimentalCreate(app, image, {
                name,
                timeoutMs: maximumSandboxLifetimeMs,
                idleTimeoutMs: request.hostIdleTimeoutMs,
                command: hostCommand(this.binaryPath, placement.privateNetwork === true),
                workdir: request.workingDirectory,
                env: hostEnvironment(request, placement.privateNetwork === true),
                h2Ports: [hostPort],
                i6pn: placement.privateNetwork,
                readinessProbe: Probe.withExec(["sh", "-c", `test -f ${readyFile}`], { intervalMs: 50 }),
                regions: [...placement.regions],
                cloud: placement.cloud
            })
            return { sandbox, reused: false }
        } catch (error) {
            if (!(error instanceof AlreadyExistsError)) throw error
            const raced = (await this.existing(app, name)) ?? (await this.modal.sandboxes.experimentalFromName(this.appName, name))
            return { sandbox: raced, reused: true }
        }
    }

    private async activate(sandbox: Sandbox, request: EnsureHostRequest, placement: ReturnType<typeof modalPlacement>, startedAt: number, phases: ProvisioningPhases): Promise<ActorHostHandle> {
        const started = placement.privateNetwork ? await this.startPrivate(sandbox, request, startedAt, phases) : await this.startPublic(sandbox, request, startedAt, phases)
        await writeMetadata(sandbox, started.handle)
        this.mark(phases, "metadataWrittenAtMs", startedAt)
        return this.withProvisioning(started.handle, sandbox, false, startedAt, phases)
    }

    private async existing(app: App, name: string): Promise<Sandbox | undefined> {
        try {
            const sandbox = await this.modal.sandboxes.experimentalFromName(app.name ?? this.appName, name)
            return (await sandbox.poll()) === null ? sandbox : undefined
        } catch (error) {
            if (error instanceof NotFoundError) return undefined
            throw error
        }
    }

    private async readHandle(sandbox: Sandbox, canonicalRegion: string): Promise<ActorHostHandle> {
        const process = await sandbox.exec(["sh", "-c", "for i in $(seq 1 1200); do test -s /tmp/durable-object-host.json && exec cat /tmp/durable-object-host.json; sleep 0.05; done; exit 1"], {
            stdout: "pipe",
            stderr: "pipe"
        })
        const [document, exitCode] = await Promise.all([process.stdout.readText(), process.wait()])
        if (exitCode !== 0) throw new Error("existing Modal host has no ready metadata")
        const handle = JSON.parse(document) as ActorHostHandle
        if (handle.canonicalRegion !== canonicalRegion) throw new Error("existing Modal host has the wrong canonical region")
        return handle
    }

    private async startPrivate(sandbox: Sandbox, request: EnsureHostRequest, startedAt: number, phases: ProvisioningPhases): Promise<StartedHost> {
        await this.waitForReady(sandbox)
        this.mark(phases, "hostReadyObservedAtMs", startedAt)
        const route = await sandbox.filesystem.readText(hostRouteFile)
        this.mark(phases, "routeReadAtMs", startedAt)
        if (!route) throw new Error("Modal durable-object host did not publish its private route")
        return { handle: { hostId: request.hostId, route: route.trim(), canonicalRegion: request.canonicalRegion } }
    }

    private async startPublic(sandbox: Sandbox, request: EnsureHostRequest, startedAt: number, phases: ProvisioningPhases): Promise<StartedHost> {
        const route = (await sandbox.tunnels())[hostPort]?.url
        if (!route) throw new Error("Modal did not create the durable-object HTTP/2 tunnel")
        await writeFile(sandbox, hostRouteFile, route)
        this.mark(phases, "routeReadAtMs", startedAt)
        await this.waitForReady(sandbox)
        this.mark(phases, "hostReadyObservedAtMs", startedAt)
        return { handle: { hostId: request.hostId, route, canonicalRegion: request.canonicalRegion } }
    }

    private async waitForReady(sandbox: Sandbox): Promise<void> {
        try {
            await sandbox.waitUntilReady(60_000)
        } catch (error) {
            const detail = await sandbox.filesystem.readText(hostStderrFile).catch(() => "")
            await sandbox.terminate().catch(() => undefined)
            const message = detail.trim() || (error instanceof Error ? error.message : String(error))
            throw new Error(`durable-object host did not become ready${message ? `: ${message}` : ""}`)
        }
    }

    private withProvisioning(handle: ActorHostHandle, sandbox: Sandbox, reused: boolean, startedAt: number, phases: ProvisioningPhases): ActorHostHandle {
        return {
            ...handle,
            provisioning: {
                provider: "modal",
                resourceId: sandbox.sandboxId,
                reused,
                ...phases,
                completedAtMs: elapsedMs(startedAt, this.now())
            }
        }
    }

    private mark(phases: ProvisioningPhases, name: keyof ProvisioningPhases, startedAt: number): void {
        phases[name] = elapsedMs(startedAt, this.now())
    }
}

function hostEnvironment(request: EnsureHostRequest, privateNetwork: boolean): Record<string, string> {
    return {
        DURABLE_OBJECT_PROCESS_ROLE: "host",
        DURABLE_OBJECT_HOST_TOKEN: request.hostToken,
        DURABLE_OBJECT_JWT_PUBLIC_KEYS: request.jwtPublicKeys,
        DURABLE_OBJECT_NAMESPACE_ID: request.namespaceId,
        DURABLE_OBJECT_CONTROL_PLANE_URL: request.controlPlaneUrl,
        DURABLE_OBJECT_JWT_ISSUER: request.jwtIssuer,
        DURABLE_OBJECT_INVOKE_JWT_AUDIENCE: request.invocationJwtAudience,
        DURABLE_OBJECT_HOST_ID: request.hostId,
        DURABLE_OBJECT_SESSION_ID: request.sessionId,
        DURABLE_OBJECT_REGION: request.canonicalRegion,
        DURABLE_OBJECT_CODE_REVISION: request.codeRevision,
        DURABLE_OBJECT_EXECUTOR_SOCKET: "/tmp/durable-object-executor.sock",
        DURABLE_OBJECT_HOST_READY_FILE: readyFile,
        DURABLE_OBJECT_HOST_BIND: `${privateNetwork ? "[::]" : "0.0.0.0"}:${hostPort}`,
        DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS: String(request.actorIdleTimeoutMs),
        DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS: String(request.hostIdleTimeoutMs),
        ...(request.actorEntrypoint ? { DURABLE_OBJECT_ENTRYPOINT: request.actorEntrypoint } : {}),
        ...(privateNetwork
            ? {
                  DURABLE_OBJECT_HOST_PRIVATE_HOSTNAME: modalPrivateHostname,
                  DURABLE_OBJECT_HOST_ROUTE_FILE: hostRouteFile
              }
            : {})
    }
}

function hostCommand(binaryPath: string, privateNetwork: boolean): string[] {
    const bootstrap = privateNetwork
        ? '"$1" 2>"$2"; status=$?; printf \'%s\n\' "$status" >"$3"; if ! test -f "$4"; then sleep 60; fi; exit "$status"'
        : 'until test -s "$1"; do sleep 0.05; done; export DURABLE_OBJECT_HOST_ROUTE="$(cat "$1")"; "$2" 2>"$3"; status=$?; printf \'%s\n\' "$status" >"$4"; if ! test -f "$5"; then sleep 60; fi; exit "$status"'
    const commandArguments = privateNetwork ? [binaryPath, hostStderrFile, hostExitedFile, readyFile] : [hostRouteFile, binaryPath, hostStderrFile, hostExitedFile, readyFile]
    return ["sh", "-c", bootstrap, "durable-object-host-bootstrap", ...commandArguments]
}

function validateEnsureRequest(request: EnsureHostRequest): void {
    if (!request.hostId.startsWith(`host.v1.${request.namespaceId}.`)) throw new Error("host ID does not belong to its namespace")
    if (!Number.isInteger(request.actorIdleTimeoutMs) || request.actorIdleTimeoutMs <= 0 || request.actorIdleTimeoutMs > maximumSandboxLifetimeMs) {
        throw new Error("actor idle timeout is invalid")
    }
    if (!Number.isInteger(request.hostIdleTimeoutMs) || request.hostIdleTimeoutMs <= 0 || request.hostIdleTimeoutMs > maximumSandboxLifetimeMs) {
        throw new Error("host idle timeout is invalid")
    }
}

function validateWarmImageRequest(request: WarmImageRequest): void {
    if (!request.namespaceId || !request.codeRevision || !request.imageRef) throw new Error("image warmup request is invalid")
}

function validatePublicHostRouteRequest(request: PublicHostRouteRequest, catalog?: CanonicalRegionCatalog): void {
    if (!request.namespaceId || !request.codeRevision) throw new Error("public host route request is invalid")
    modalPlacement(request.canonicalRegion, catalog)
}

function validateTerminateHostsRequest(request: TerminateHostsRequest, catalog?: CanonicalRegionCatalog): void {
    if (!request.namespaceId || !request.codeRevision || request.canonicalRegions.length === 0) throw new Error("host termination request is invalid")
    for (const region of request.canonicalRegions) modalPlacement(region, catalog)
}

function resourceName(kind: string, namespaceId: string, codeRevision: string, canonicalRegion: string): string {
    const digest = createHash("sha256").update(namespaceId).update("\0").update(codeRevision).update("\0").update(canonicalRegion).digest("hex").slice(0, 32)
    return `do-${kind}-${digest}`
}

async function writeMetadata(sandbox: Sandbox, handle: ActorHostHandle): Promise<void> {
    await writeFile(sandbox, "/tmp/durable-object-host.json", JSON.stringify(handle))
}

async function writeFile(sandbox: Sandbox, path: string, contents: string): Promise<void> {
    await sandbox.filesystem.writeText(contents, path)
}

function elapsedMs(startedAt: number, finishedAt: number): number {
    return Math.max(0, Math.round(finishedAt - startedAt))
}

interface StartedHost {
    readonly handle: ActorHostHandle
}

export { ModalSandboxProvider }
export type { ModalSandboxProviderOptions }
