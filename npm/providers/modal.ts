import { AlreadyExistsError, ModalClient, NotFoundError } from "modal"
import type { App, Sandbox } from "modal"
import { createHash, randomUUID } from "node:crypto"

import { canonicalRegionForModal, modalPlacement } from "../regions.js"
import type { CanonicalRegionCatalog } from "../regions.js"

import type { ActorHostHandle, EnsureHostRequest, SandboxProvider, SandboxProviderHooks } from "./types.js"

const hostPort = 7101
const cacheMount = "/var/cache/durable-objects"

interface ModalSandboxProviderOptions {
    readonly tokenId: string
    readonly tokenSecret: string
    readonly controlPlaneUrl: string
    readonly credentialsUrl: string
    readonly jwtIssuer?: string
    readonly invokeAudience?: string
    readonly appName?: string
    readonly binaryPath?: string
    readonly catalog?: CanonicalRegionCatalog
    readonly hooks: SandboxProviderHooks
}

class ModalSandboxProvider implements SandboxProvider {
    private readonly modal: ModalClient
    private readonly appName: string
    private readonly binaryPath: string
    private readonly activations = new Map<string, Promise<ActorHostHandle>>()

    constructor(private readonly options: ModalSandboxProviderOptions) {
        this.modal = new ModalClient({ tokenId: options.tokenId, tokenSecret: options.tokenSecret })
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
        const placement = modalPlacement(request.canonicalRegion, this.options.catalog)
        const [app, artifact, bootstrap, volume] = await Promise.all([
            this.modal.apps.fromName(this.appName, { createIfMissing: true }),
            this.options.hooks.resolveArtifact(request),
            this.options.hooks.issueHostBootstrap(request),
            this.modal.volumes.fromName(resourceName("cache", request), { createIfMissing: true })
        ])
        const existing = await this.existing(app, name)
        if (existing) {
            try {
                return await this.readHandle(existing, request.canonicalRegion)
            } catch {
                await existing.terminate()
            }
        }

        const image = await this.modal.images.fromId(artifact.imageId)
        let sandbox: Sandbox
        try {
            sandbox = await this.modal.sandboxes.create(app, image, {
                name,
                timeoutMs: 24 * 60 * 60 * 1000,
                idleTimeoutMs: 5 * 60 * 1000,
                h2Ports: [hostPort],
                regions: [...placement.regions],
                cloud: placement.cloud,
                volumes: { [cacheMount]: volume }
            })
        } catch (error) {
            if (!(error instanceof AlreadyExistsError)) throw error
            const raced = (await this.existing(app, name)) ?? (await this.modal.sandboxes.fromName(this.appName, name))
            return this.readHandle(raced, request.canonicalRegion)
        }

        const observed = await runtimePlacement(sandbox)
        const observedCanonical = canonicalRegionForModal(observed.cloud, observed.region, this.options.catalog)
        if (observedCanonical !== request.canonicalRegion) {
            await sandbox.terminate()
            throw new Error(`Modal placed host in ${observedCanonical ?? "an unknown region"}; expected ${request.canonicalRegion}`)
        }

        const handle = await this.start(sandbox, request, artifact.workingDirectory, artifact.actorEntrypoint, bootstrap.credential)
        await writeMetadata(sandbox, handle)
        return handle
    }

    async status(request: EnsureHostRequest): Promise<"serving" | "warm" | "cold"> {
        const app = await this.modal.apps.fromName(this.appName, { createIfMissing: true })
        if (await this.existing(app, resourceName("host", request))) return "serving"
        try {
            await this.modal.volumes.fromName(resourceName("cache", request))
            return "warm"
        } catch (error) {
            if (error instanceof NotFoundError) return "cold"
            throw error
        }
    }

    async deactivate(request: EnsureHostRequest): Promise<void> {
        const app = await this.modal.apps.fromName(this.appName, { createIfMissing: true })
        await (await this.existing(app, resourceName("host", request)))?.terminate()
    }

    async removeLocalCache(request: EnsureHostRequest): Promise<void> {
        await this.deactivate(request)
        await this.modal.volumes.delete(resourceName("cache", request), { allowMissing: true })
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

    private async start(sandbox: Sandbox, request: EnsureHostRequest, workingDirectory: string, actorEntrypoint: string | undefined, credential: string): Promise<ActorHostHandle> {
        const route = (await sandbox.tunnels())[hostPort]?.url
        if (!route) throw new Error("Modal did not create the durable-object HTTP/2 tunnel")
        const hostId = `host.v1.${request.namespaceId}.${randomUUID()}`
        const sessionId = randomUUID()
        const executorSocket = "/tmp/durable-object-executor.sock"
        const readyFile = "/tmp/durable-object-ready"
        const environment = {
            DURABLE_OBJECT_PROCESS_ROLE: "host",
            DURABLE_OBJECT_CREDENTIAL: credential,
            DURABLE_OBJECT_CREDENTIALS_URL: this.options.credentialsUrl,
            DURABLE_OBJECT_CONTROL_PLANE_URL: this.options.controlPlaneUrl,
            DURABLE_OBJECT_JWT_ISSUER: this.options.jwtIssuer ?? "durable-object-control-plane",
            DURABLE_OBJECT_INVOKE_JWT_AUDIENCE: this.options.invokeAudience ?? "durable-object-invoke",
            DURABLE_OBJECT_HOST_ID: hostId,
            DURABLE_OBJECT_SESSION_ID: sessionId,
            DURABLE_OBJECT_REGION: request.canonicalRegion,
            DURABLE_OBJECT_CODE_REVISION: request.codeRevision,
            DURABLE_OBJECT_LOCAL_ROOT: `${cacheMount}/runtime`,
            DURABLE_OBJECT_EXECUTOR_SOCKET: executorSocket,
            DURABLE_OBJECT_HOST_READY_FILE: readyFile,
            DURABLE_OBJECT_HOST_BIND: `0.0.0.0:${hostPort}`,
            DURABLE_OBJECT_HOST_ROUTE: route,
            ...(actorEntrypoint ? { DURABLE_OBJECT_ENTRYPOINT: actorEntrypoint } : {})
        }
        const process = await sandbox.exec([this.binaryPath], { workdir: workingDirectory, env: environment, stdout: "pipe", stderr: "pipe" })
        void process.wait().catch(() => undefined)
        const ready = await sandbox.exec(["sh", "-c", `for i in $(seq 1 1200); do test -f ${readyFile} && exit 0; sleep 0.05; done; exit 1`], { stdout: "pipe", stderr: "pipe" })
        if ((await ready.wait()) !== 0) {
            await sandbox.terminate()
            throw new Error("durable-object host did not become ready")
        }
        return { hostId, route, canonicalRegion: request.canonicalRegion, cacheSource: "volume" }
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
    const file = await sandbox.open("/tmp/durable-object-host.json", "w")
    await file.write(new TextEncoder().encode(JSON.stringify(handle)))
    await file.flush()
    await file.close()
}

export { ModalSandboxProvider }
export type { ModalSandboxProviderOptions }
