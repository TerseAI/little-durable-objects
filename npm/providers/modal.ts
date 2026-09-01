import { AlreadyExistsError, ModalClient, NotFoundError } from "modal"
import type { App, Image, Sandbox } from "modal"
import { createHash } from "node:crypto"

import { canonicalRegionForModal, modalPlacement } from "../regions.js"
import type { CanonicalRegionCatalog } from "../regions.js"

import type { ActorHostHandle, EnsureHostRequest, SandboxProvider } from "./types.js"

const hostPort = 7101
const hostRouteFile = "/tmp/durable-object-route"
const readyFile = "/tmp/durable-object-ready"
const maximumSandboxLifetimeMs = 24 * 60 * 60 * 1000

interface ModalSandboxProviderOptions {
    readonly appName?: string
    readonly binaryPath?: string
    readonly catalog?: CanonicalRegionCatalog
    readonly client?: ModalClient
}

type SandboxAcquisition = { readonly sandbox: Sandbox } | { readonly handle: ActorHostHandle }

class ModalSandboxProvider implements SandboxProvider {
    private readonly modal: ModalClient
    private readonly appName: string
    private readonly binaryPath: string
    private readonly activations = new Map<string, Promise<ActorHostHandle>>()

    constructor(private readonly options: ModalSandboxProviderOptions = {}) {
        this.modal = options.client ?? new ModalClient()
        this.appName = options.appName ?? "durable-object-hosts"
        this.binaryPath = options.binaryPath ?? "/usr/local/bin/durable-object-runtime"
    }

    async ensureHost(request: EnsureHostRequest): Promise<ActorHostHandle> {
        const key = resourceName("host", request)
        const current = this.activations.get(key)
        if (current) return current
        const activation = this.ensureHostOnce(request, key).finally(() => this.activations.delete(key))
        this.activations.set(key, activation)
        return activation
    }

    private async ensureHostOnce(request: EnsureHostRequest, name: string): Promise<ActorHostHandle> {
        validateEnsureRequest(request)
        const placement = modalPlacement(request.canonicalRegion, this.options.catalog)
        const [app, image] = await Promise.all([this.modal.apps.fromName(this.appName, { createIfMissing: true }), this.modal.images.fromId(request.imageRef)])
        const existing = await this.reuseExisting(app, name, request.canonicalRegion)
        if (existing) return existing
        const acquired = await this.createSandbox(request, name, app, image, placement)
        if ("handle" in acquired) return acquired.handle
        return this.activate(acquired.sandbox, request)
    }

    private async reuseExisting(app: App, name: string, canonicalRegion: string): Promise<ActorHostHandle | undefined> {
        const existing = await this.existing(app, name)
        if (!existing) return undefined
        try {
            return await this.readHandle(existing, canonicalRegion)
        } catch {
            await existing.terminate()
            return undefined
        }
    }

    private async createSandbox(request: EnsureHostRequest, name: string, app: App, image: Image, placement: ReturnType<typeof modalPlacement>): Promise<SandboxAcquisition> {
        try {
            const sandbox = await this.modal.sandboxes.create(app, image, {
                name,
                timeoutMs: maximumSandboxLifetimeMs,
                idleTimeoutMs: request.hostIdleTimeoutMs,
                command: [
                    "sh",
                    "-c",
                    'until test -s "$1"; do sleep 0.05; done; export DURABLE_OBJECT_HOST_ROUTE="$(cat "$1")"; exec "$2"',
                    "durable-object-host-bootstrap",
                    hostRouteFile,
                    this.binaryPath
                ],
                workdir: request.workingDirectory,
                env: hostEnvironment(request),
                h2Ports: [hostPort],
                regions: [...placement.regions],
                cloud: placement.cloud
            })
            return { sandbox }
        } catch (error) {
            if (!(error instanceof AlreadyExistsError)) throw error
            const raced = (await this.existing(app, name)) ?? (await this.modal.sandboxes.fromName(this.appName, name))
            return { handle: await this.readHandle(raced, request.canonicalRegion) }
        }
    }

    private async activate(sandbox: Sandbox, request: EnsureHostRequest): Promise<ActorHostHandle> {
        await this.validatePlacement(sandbox, request.canonicalRegion)
        const handle = await this.start(sandbox, request)
        await writeMetadata(sandbox, handle)
        return handle
    }

    private async validatePlacement(sandbox: Sandbox, expectedRegion: string): Promise<void> {
        const observed = await runtimePlacement(sandbox)
        const observedCanonical = canonicalRegionForModal(observed.cloud, observed.region, this.options.catalog)
        if (observedCanonical !== expectedRegion) {
            await sandbox.terminate()
            throw new Error(`Modal placed host in ${observedCanonical ?? "an unknown region"}; expected ${expectedRegion}`)
        }
    }

    private async existing(app: App, name: string): Promise<Sandbox | undefined> {
        try {
            const sandbox = await this.modal.sandboxes.fromName(app.name ?? this.appName, name)
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

    private async start(sandbox: Sandbox, request: EnsureHostRequest): Promise<ActorHostHandle> {
        const route = (await sandbox.tunnels())[hostPort]?.url
        if (!route) throw new Error("Modal did not create the durable-object HTTP/2 tunnel")
        await writeFile(sandbox, hostRouteFile, route)
        const ready = await sandbox.exec(["sh", "-c", `for i in $(seq 1 1200); do test -f ${readyFile} && exit 0; sleep 0.05; done; exit 1`], { stdout: "pipe", stderr: "pipe" })
        if ((await ready.wait()) !== 0) {
            await sandbox.terminate()
            throw new Error("durable-object host did not become ready")
        }
        return { hostId: request.hostId, route, canonicalRegion: request.canonicalRegion }
    }
}

function hostEnvironment(request: EnsureHostRequest): Record<string, string> {
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
        DURABLE_OBJECT_HOST_BIND: `0.0.0.0:${hostPort}`,
        DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS: String(request.actorIdleTimeoutMs),
        DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS: String(request.hostIdleTimeoutMs),
        ...(request.actorEntrypoint ? { DURABLE_OBJECT_ENTRYPOINT: request.actorEntrypoint } : {})
    }
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

function resourceName(kind: string, request: EnsureHostRequest): string {
    const digest = createHash("sha256").update(request.namespaceId).update("\0").update(request.codeRevision).update("\0").update(request.canonicalRegion).digest("hex").slice(0, 32)
    return `do-${kind}-${digest}`
}

async function runtimePlacement(sandbox: Sandbox): Promise<{ cloud?: string; region?: string }> {
    const process = await sandbox.exec(["sh", "-c", 'printf \'%s\\n%s\\n\' "${MODAL_CLOUD_PROVIDER:-}" "${MODAL_REGION:-}"'], { stdout: "pipe", stderr: "pipe" })
    const [stdout, exitCode] = await Promise.all([process.stdout.readText(), process.wait()])
    if (exitCode !== 0) throw new Error("could not inspect Modal host placement")
    const [cloud, region] = stdout.split("\n").map(value => value.trim())
    return { ...(cloud ? { cloud } : {}), ...(region ? { region } : {}) }
}

async function writeMetadata(sandbox: Sandbox, handle: ActorHostHandle): Promise<void> {
    await writeFile(sandbox, "/tmp/durable-object-host.json", JSON.stringify(handle))
}

async function writeFile(sandbox: Sandbox, path: string, contents: string): Promise<void> {
    const file = await sandbox.open(path, "w")
    await file.write(new TextEncoder().encode(contents))
    await file.flush()
    await file.close()
}

export { ModalSandboxProvider }
export type { ModalSandboxProviderOptions }
