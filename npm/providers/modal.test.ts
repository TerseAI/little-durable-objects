import { NotFoundError } from "modal"
import type { ModalClient, SandboxCreateParams } from "modal"
import assert from "node:assert/strict"
import { test } from "node:test"

import { ModalSandboxProvider } from "./modal.js"
import type { EnsureHostRequest, PublicHostRouteRequest, TerminateHostsRequest, WarmImageRequest } from "./types.js"

test("terminates every cached host for a replaced deployment revision", async () => {
    const lookedUp: string[] = []
    const terminated: string[] = []
    const client = {
        apps: {
            async fromName() {
                return { name: "durable-object-hosts" }
            }
        },
        sandboxes: {
            async experimentalFromName(_appName: string, name: string) {
                lookedUp.push(name)
                if (lookedUp.length === 2) throw new NotFoundError("not found")
                return {
                    sandboxId: `sb-${lookedUp.length}`,
                    async terminate() {
                        terminated.push(name)
                    }
                }
            }
        }
    }
    const provider = new ModalSandboxProvider({ client: client as unknown as ModalClient })

    const result = await provider.terminateHosts(terminateHostsRequest())

    assert.equal(lookedUp.length, 3)
    assert.equal(new Set(lookedUp).size, 3)
    assert.deepEqual(terminated, [lookedUp[0], lookedUp[2]])
    assert.deepEqual(result, { provider: "modal", resourceIds: ["sb-1", "sb-3"] })
})

test("warms an image in a disposable regional V2 sandbox", async () => {
    let createOptions: SandboxCreateParams | undefined
    let waited = false
    let terminated = false
    const sandbox = {
        sandboxId: "sb-v2-warmup",
        async wait() {
            waited = true
            return 0
        },
        async terminate() {
            terminated = true
        }
    }
    const client = {
        apps: {
            async fromName() {
                return { name: "durable-object-hosts" }
            }
        },
        images: {
            async fromId() {
                return { imageId: "im-actor" }
            }
        },
        sandboxes: {
            async experimentalCreate(_app: unknown, _image: unknown, options: SandboxCreateParams) {
                createOptions = options
                return sandbox
            }
        }
    }
    const provider = new ModalSandboxProvider({ client: client as unknown as ModalClient, now: () => 0 })

    const result = await provider.warmImage(warmImageRequest())

    assert.deepEqual(result, { provider: "modal", resourceId: "sb-v2-warmup", totalMs: 0 })
    assert.deepEqual(createOptions?.command, ["true"])
    assert.deepEqual(createOptions?.regions, ["us-east4"])
    assert.equal(createOptions?.cloud, "gcp")
    assert.equal(createOptions?.name, undefined)
    assert.equal(waited, true)
    assert.equal(terminated, true)
})

test("creates a V2 Modal sandbox with the host as its main process and reports provisioning timings", async () => {
    let createOptions: SandboxCreateParams | undefined
    let usedLegacyCreate = false
    let tunnelLookups = 0
    const files = new Map<string, string>()
    let waitedForReadiness = false
    const sandbox = {
        sandboxId: "sb-v2-actor",
        async waitUntilReady() {
            waitedForReadiness = true
        },
        async tunnels() {
            tunnelLookups += 1
            throw new Error("same-region activation must not wait for a public tunnel")
        },
        filesystem: {
            async readText(path: string) {
                assert.equal(path, "/tmp/durable-object-route")
                return "http://[fd00:cafe::1234]:7101\n"
            },
            async writeText(contents: string, path: string) {
                files.set(path, contents)
            }
        },
        async terminate() {}
    }
    const client = {
        apps: {
            async fromName() {
                return { name: "durable-object-hosts" }
            }
        },
        images: {
            async fromId() {
                return { imageId: "im-actor" }
            }
        },
        volumes: {
            async fromName(_name: string, options?: { createIfMissing?: boolean }) {
                if (!options?.createIfMissing) throw new NotFoundError("not found")
                return { volumeId: "vo-cache" }
            },
            async delete() {}
        },
        sandboxes: {
            async experimentalFromName() {
                throw new NotFoundError("not found")
            },
            async experimentalCreate(_app: unknown, _image: unknown, options: SandboxCreateParams) {
                createOptions = options
                return sandbox
            },
            async create() {
                usedLegacyCreate = true
                throw new Error("legacy Sandbox creation must not be used")
            }
        }
    }
    const provider = new ModalSandboxProvider({ client: client as unknown as ModalClient, now: () => 0 })

    const handle = await provider.ensureHost(request())

    assert.deepEqual(handle, {
        hostId: "host.v1.project-1.00000000-0000-4000-8000-000000000001",
        route: "http://[fd00:cafe::1234]:7101",
        canonicalRegion: "north-america-east",
        provisioning: {
            provider: "modal",
            resourceId: "sb-v2-actor",
            reused: false,
            resourceLookupMs: 0,
            existingLookupMs: 0,
            createMs: 0,
            placementMs: 0,
            tunnelMs: 0,
            readyMs: 0,
            metadataMs: 0,
            totalMs: 0
        }
    })
    assert.equal(usedLegacyCreate, false)
    assert.equal(createOptions?.timeoutMs, 86_400_000)
    assert.equal(createOptions?.idleTimeoutMs, 300_000)
    assert.deepEqual(createOptions?.regions, ["us-east4"])
    assert.equal(createOptions?.i6pn, true)
    assert.deepEqual(createOptions?.h2Ports, [7101])
    assert.ok(createOptions?.readinessProbe)
    assert.equal(createOptions?.command?.[4], "/usr/local/bin/little-durable-objects")
    assert.equal(createOptions?.command?.[7], "/tmp/durable-object-ready")
    assert.match(createOptions?.command?.[2] ?? "", /if ! test -f "\$4"; then sleep 60; fi/u)
    assert.equal(createOptions?.env?.DURABLE_OBJECT_HOST_TOKEN, "host-jwt")
    assert.equal(createOptions?.env?.DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS, "60000")
    assert.equal(createOptions?.env?.DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS, "300000")
    assert.equal(createOptions?.env?.DURABLE_OBJECT_HOST_BIND, "[::]:7101")
    assert.equal(createOptions?.env?.DURABLE_OBJECT_HOST_PRIVATE_HOSTNAME, "i6pn.modal.local")
    assert.equal(createOptions?.env?.DURABLE_OBJECT_HOST_ROUTE_FILE, "/tmp/durable-object-route")
    assert.equal(createOptions?.env?.MODAL_TOKEN_ID, undefined)
    assert.equal(files.get("/tmp/durable-object-route"), undefined)
    assert.match(files.get("/tmp/durable-object-host.json") ?? "", /host\.v1\.project-1/u)
    assert.equal(waitedForReadiness, true)
    assert.equal(tunnelLookups, 0)
})

test("retrieves the public HTTP/2 route only when requested", async () => {
    let tunnelLookups = 0
    const sandbox = {
        sandboxId: "sb-v2-actor",
        async poll() {
            return null
        },
        async tunnels() {
            tunnelLookups += 1
            return { 7101: { url: "https://host.example.com" } }
        }
    }
    const client = {
        apps: {
            async fromName() {
                return { name: "durable-object-hosts" }
            }
        },
        sandboxes: {
            async experimentalFromName() {
                return sandbox
            }
        }
    }
    const provider = new ModalSandboxProvider({ client: client as unknown as ModalClient })

    const route = await provider.publicHostRoute(publicHostRouteRequest())

    assert.deepEqual(route, { route: "https://host.example.com" })
    assert.equal(tunnelLookups, 1)
})

test("surfaces host stderr when the main process exits before readiness", async () => {
    let terminated = false
    const sandbox = {
        sandboxId: "sb-v2-failed",
        async waitUntilReady() {
            throw new Error("sandbox stopped")
        },
        async tunnels() {
            return { 7101: { url: "https://host.example.com" } }
        },
        filesystem: {
            async readText(path: string) {
                assert.equal(path, "/tmp/durable-object-host.stderr")
                return "host could not register its lease\n"
            },
            async writeText() {}
        },
        async terminate() {
            terminated = true
        }
    }
    const client = {
        apps: {
            async fromName() {
                return { name: "durable-object-hosts" }
            }
        },
        images: {
            async fromId() {
                return { imageId: "im-actor" }
            }
        },
        sandboxes: {
            async experimentalFromName() {
                throw new NotFoundError("not found")
            },
            async experimentalCreate() {
                return sandbox
            }
        }
    }
    const provider = new ModalSandboxProvider({ client: client as unknown as ModalClient })

    await assert.rejects(provider.ensureHost(request()), /durable-object host did not become ready: host could not register its lease/u)
    assert.equal(terminated, true)
})

function request(): EnsureHostRequest {
    return {
        namespaceId: "project-1",
        codeRevision: "revision-1",
        canonicalRegion: "north-america-east",
        hostId: "host.v1.project-1.00000000-0000-4000-8000-000000000001",
        sessionId: "00000000-0000-4000-8000-000000000002",
        hostToken: "host-jwt",
        jwtPublicKeys: '{"primary":"public-key"}',
        controlPlaneUrl: "https://objects.example.com",
        jwtIssuer: "durable-object-control-plane",
        invocationJwtAudience: "durable-object-invoke",
        imageRef: "im-actor",
        workingDirectory: "/workspace",
        actorEntrypoint: "src/durable-objects.ts",
        actorIdleTimeoutMs: 60_000,
        hostIdleTimeoutMs: 300_000
    }
}

function warmImageRequest(): WarmImageRequest {
    return {
        namespaceId: "project-1",
        codeRevision: "revision-1",
        canonicalRegion: "north-america-east",
        imageRef: "im-actor"
    }
}

function terminateHostsRequest(): TerminateHostsRequest {
    return {
        namespaceId: "project-1",
        codeRevision: "revision-1",
        canonicalRegions: ["north-america-east", "north-america-central", "north-america-west"]
    }
}

function publicHostRouteRequest(): PublicHostRouteRequest {
    return {
        namespaceId: "project-1",
        codeRevision: "revision-1",
        canonicalRegion: "north-america-east"
    }
}
